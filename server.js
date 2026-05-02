// ══════════════════════════════════════════════════════════════════════════════
//  AlloCiné VOD Scraper — server.js
//  ─────────────────────────────────────────────────────────────────────────────
//  Serveur Express déployé sur Railway (port 3009).
//  Scrape AlloCiné avec Axios + Cheerio, persiste via Upstash Redis.
//
//  ┌─ DOMAINE FILMS ─────────────────────────────────────────────────────────┐
//  │  Pages scraping  : allocine.fr/vod/films/decennie-2020/annee-XXXX/     │
//  │  Pages détail    : allocine.fr/film/fichefilm_gen_cfilm=XXXXX.html      │
//  │  Flux            : /api/scrape (SSE) → Redis['films']                  │
//  │  Consultation    : /api/films + /api/details                            │
//  └─────────────────────────────────────────────────────────────────────────┘
//  ┌─ DOMAINE SÉRIES ────────────────────────────────────────────────────────┐
//  │  Pages scraping  : Top AlloCiné + presse par année (glissant 4 ans)    │
//  │  Pages détail    : allocine.fr/series/ficheserie_gen_cserie=XXXXX.html  │
//  │  Flux            : /api/series/scrape (SSE) → Redis['series']          │
//  │  Consultation    : /api/series + /api/series/details                   │
//  └─────────────────────────────────────────────────────────────────────────┘
//
//  Clés Redis :
//    films                  → liste complète films (JSON)
//    details                → cache fiches films   { "id:XXXXX": { pays, annee, providers } }
//    series                 → liste complète séries (JSON)
//    series_details         → cache fiches séries  { "sid:XXXXX": { statut, pays, ... } }
//    users                  → profils              { userId: { id, name, createdAt } }
//    userdata               → notes par profil     { userId: { allocineId: { vu, vouloir, ... } } }
//    prefs                  → préférences UI        { userId: { showDocumentaires, ... } }
//    lastScrape             → ISO horodatage dernier scraping films
//    lastDetailsScrape      → ISO horodatage dernier scraping plateformes films
//    lastSeriesScrape       → ISO horodatage dernier scraping séries
//    lastSeriesDetailsScrape→ ISO horodatage dernier scraping fiches séries
// ══════════════════════════════════════════════════════════════════════════════

'use strict';

const express = require('express');
const axios   = require('axios');
const path    = require('path');
const fs      = require('fs');
const cheerio = require('cheerio');
const { Redis } = require('@upstash/redis');

// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 1 — CONFIGURATION & CONSTANTES
// ══════════════════════════════════════════════════════════════════════════════

const app       = express();
const PORT      = Number(process.env.PORT || 3009);
const TOTAL_PAGES      = Number(process.env.TOTAL_PAGES || 25); // pages de films VOD par année
const DETAILS_TTL_MS   = 1000 * 60 * 60 * 24;       // 24h  — durée de validité des fiches films
const AUTO_SCRAPE_DAYS = 2;                          // re-scraper si cache > 2 jours (films + séries)
const DATA_DIR         = process.env.DATA_DIR || __dirname; // répertoire pour le fallback JSON local

// Version du build (incrémentée par le hook pre-commit)
const BUILD   = (() => { try { return require('./version.json').build; } catch(e) { return 0; } })();
const VERSION = `v9.2.${BUILD}`;
const SERVER_START = new Date().toISOString();

// ── Sécurité — Secret partagé ─────────────────────────────────────────────
// Définir APP_SECRET dans les variables d'environnement Railway pour activer la protection.
// Si non défini (développement local), tous les endpoints restent accessibles.
const APP_SECRET = process.env.APP_SECRET || null;

/** Middleware : rejette les requêtes sans le bon header x-app-secret (si APP_SECRET est défini). */
function requireSecret(req, res, next) {
  if (!APP_SECRET) return next();
  if (req.headers['x-app-secret'] !== APP_SECRET)
    return res.status(403).json({ error: 'Accès non autorisé' });
  next();
}

// ── Sécurité — Rate limiter simple (sans dépendance externe) ──────────────
const _rlBuckets = new Map();
/**
 * Retourne un middleware Express limitant les appels à `max` requêtes par `windowMs` ms par IP.
 * Usage : app.get('/api/scrape', requireRateLimit(3, 10 * 60 * 1000), async (req, res) => …)
 */
function requireRateLimit(max, windowMs) {
  return (req, res, next) => {
    const ip  = req.ip || req.socket?.remoteAddress || 'unknown';
    const key = `${req.path}::${ip}`;
    const now = Date.now();
    const b   = _rlBuckets.get(key) || { count: 0, reset: now + windowMs };
    if (now > b.reset) { b.count = 0; b.reset = now + windowMs; }
    b.count++;
    _rlBuckets.set(key, b);
    if (b.count > max)
      return res.status(429).json({ error: 'Trop de requêtes, réessayez dans quelques minutes.' });
    next();
  };
}

// ── État global Films ──────────────────────────────────────────────────────
let cachedFilms      = [];   // dernière liste de films scrapés (en mémoire + Redis)
let lastScrape            = null; // ISO — dernier scraping liste films
let lastDetailsScrape     = null; // ISO — dernier scraping plateformes films
let isScraping            = false;
let scrapeProgress        = { current: 0, total: 0, annee: '' };
let filmsDetailsProgress  = { current: 0, total: 0 };
let lastScrapeErrors      = [];
// Phase du scraping nocturne en cours
let scrapingPhase         = null; // null | 'films-list' | 'films-details' | 'series-list' | 'series-details' | 'bestever-list' | 'bestever-details'

// ── Sources Séries (fenêtre glissante 4 ans) ───────────────────────────────
// Génère dynamiquement : Top AlloCiné + presse de l'année courante et les 3 ans précédents.
const _currentYear   = new Date().getFullYear();
const _historyYears  = Array.from({ length: 4 }, (_, i) => _currentYear - i); // ex: [2026,2025,2024,2023]
const SERIES_SOURCES = [
  { label: 'Top AlloCiné', baseUrl: 'https://www.allocine.fr/series/top/', pages: 10 },
  ..._historyYears.map(y => ({
    label:   `Presse ${y}`,
    baseUrl: `https://www.allocine.fr/series-tv/presse/decennie-${Math.floor(y / 10) * 10}/annee-${y}/`,
    pages:   5,
  })),
];
const SERIES_PAGES          = SERIES_SOURCES.reduce((sum, s) => sum + s.pages, 0);
const SERIES_DETAILS_TTL_MS = 1000 * 60 * 60 * 24 * 7; // 7 jours — durée de validité des fiches séries

// ── État global Séries ─────────────────────────────────────────────────────
let cachedSeries            = [];   // dernière liste de séries scrapées
let lastSeriesScrape        = null; // ISO — dernier scraping liste séries
let lastSeriesDetailsScrape = null; // ISO — dernier scraping fiches séries
let isScrapingSeries         = false;
let seriesProgress           = { current: 0, total: 0 };
let seriesDetailsProgress    = { current: 0, total: 0 };
let lastSeriesScrapeErrors   = [];
const seriesDetailsCache    = new Map(); // clé: "sid:XXXXX" → { value, cachedAt }

// ── État global Bestever ────────────────────────────────────────────────────
const BESTEVER_DECADES          = [1940, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020];
const BESTEVER_PAGES_PER_DECADE = Number(process.env.BESTEVER_PAGES_PER_DECADE || 9);
let cachedBestever              = [];   // meilleurs films all-time (par décennie)
let lastBesteverScrape          = null; // ISO — dernier scraping liste bestever
let lastBesteverDetailsScrape   = null; // ISO — dernier scraping plateformes bestever
let isBesteverScraping          = false;
let besteverProgress            = { current: 0, total: 0 };
let besteverDetailsProgress     = { current: 0, total: 0 };

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 2 — BASE DE DONNÉES UTILISATEURS (Redis + fallback fichier local)
//
//  Trois tables en mémoire (synchronisées avec Redis) :
//    users    : profils        { userId → { id, name, createdAt } }
//    userdata : notes par film { userId → { allocineId → { vu, vouloir, nonInteresse, asuivre } } }
//    prefsDB  : préférences UI { userId → { showDocumentaires, showAnimations, hideVus, hideNonInteresse } }
// ══════════════════════════════════════════════════════════════════════════════

const redis = process.env.UPSTASH_REDIS_REST_URL
  ? new Redis({ url: process.env.UPSTASH_REDIS_REST_URL, token: process.env.UPSTASH_REDIS_REST_TOKEN })
  : null; // null si pas de Redis configuré → mode fichier local uniquement

let users    = {};  // { userId: { id, name, createdAt } }
let userdata = {};  // { userId: { allocineId: { vu, vouloir, nonInteresse, asuivre } } }
let prefsDB  = {};  // { userId: { showDocumentaires, showAnimations, hideVus, hideNonInteresse } }

/**
 * Détecte l'ancien format plat mono-utilisateur :
 * { allocineId: { vu: true, vouloir: false, ... } }  ← avant multi-profils
 */
function isOldUserdataFormat(data) {
  if (!data || typeof data !== 'object') return false;
  return Object.values(data).some(v => v && typeof v === 'object' && typeof v.vu === 'boolean');
}

/**
 * Migre l'ancien format plat vers le nouveau format multi-profils
 * en créant un profil par défaut "Mon profil" (renommé "JC" par seedDefaultProfiles).
 */
function migrateUserdata(old) {
  const defaultId = 'user_default';
  users[defaultId] = users[defaultId] || { id: defaultId, name: 'Mon profil', createdAt: new Date().toISOString() };
  userdata = { [defaultId]: old };
  console.log(`🔄 Migration userdata → format multi-utilisateurs (${Object.keys(old).length} entrées)`);
}

/**
 * Charge toutes les données persistées au démarrage du serveur.
 * Ordre de priorité : Redis → fichier local userdata.json (backup).
 * Si des données sont trouvées en fichier local mais pas en Redis, elles y sont resynchronisées.
 */
async function loadUserdata() {
  if (redis) {
    try {
      // Profils utilisateurs
      const usersRaw = await redis.get('users');
      if (usersRaw) {
        users = typeof usersRaw === 'string' ? JSON.parse(usersRaw) : usersRaw;
        console.log(`👤 Profils Redis chargés (${Object.keys(users).length} profils)`);
      }
      // Notes utilisateurs (avec migration si ancien format)
      const data = await redis.get('userdata');
      if (data) {
        const parsed = typeof data === 'string' ? JSON.parse(data) : data;
        if (isOldUserdataFormat(parsed)) {
          migrateUserdata(parsed);
          await redis.set('users',    JSON.stringify(users));
          await redis.set('userdata', JSON.stringify(userdata));
        } else {
          userdata = parsed;
          console.log(`📂 Userdata Redis chargé (${Object.keys(userdata).length} profils)`);
        }
      }
      // Préférences UI
      const prefsRaw = await redis.get('prefs');
      if (prefsRaw) {
        prefsDB = typeof prefsRaw === 'string' ? JSON.parse(prefsRaw) : prefsRaw;
        console.log(`🔖 Prefs Redis chargées (${Object.keys(prefsDB).length} profils)`);
      }
      // Horodatages scraping films
      const ts  = await redis.get('lastScrape');       if (ts)  lastScrape        = ts;
      const tsD = await redis.get('lastDetailsScrape');if (tsD) lastDetailsScrape = tsD;
      // Liste de films
      const films = await redis.get('films');
      if (films) {
        cachedFilms = typeof films === 'string' ? JSON.parse(films) : films;
        console.log(`🎬 Films Redis chargés (${cachedFilms.length} films)`);
      }
      // Cache détails films
      const details = await redis.get('details');
      if (details) {
        const obj = typeof details === 'string' ? JSON.parse(details) : details;
        Object.entries(obj).forEach(([k, v]) => detailsCache.set(k, v));
        console.log(`📋 détailsCache Redis chargé (${detailsCache.size} entrées)`);
      }
      // Liste de séries
      const seriesRaw = await redis.get('series');
      if (seriesRaw) {
        cachedSeries = typeof seriesRaw === 'string' ? JSON.parse(seriesRaw) : seriesRaw;
        console.log(`📺 Séries Redis chargées (${cachedSeries.length} séries)`);
      }
      // Horodatages scraping séries
      const seriesTsRaw = await redis.get('lastSeriesScrape');
      if (seriesTsRaw) lastSeriesScrape = seriesTsRaw;
      const seriesDetailsTsRaw = await redis.get('lastSeriesDetailsScrape');
      if (seriesDetailsTsRaw) lastSeriesDetailsScrape = seriesDetailsTsRaw;
      // Cache détails séries
      const seriesDetailsRaw = await redis.get('series_details');
      if (seriesDetailsRaw) {
        const sdObj = typeof seriesDetailsRaw === 'string' ? JSON.parse(seriesDetailsRaw) : seriesDetailsRaw;
        Object.entries(sdObj).forEach(([k, v]) => seriesDetailsCache.set(k, v));
        console.log(`📋 seriesDetailsCache Redis chargé (${seriesDetailsCache.size} entrées)`);
      }
      // Liste bestever
      const besteverRaw = await redis.get('bestever');
      if (besteverRaw) {
        cachedBestever = typeof besteverRaw === 'string' ? JSON.parse(besteverRaw) : besteverRaw;
        console.log(`🏆 Bestever Redis chargé (${cachedBestever.length} films)`);
      }
      // Horodatages bestever
      const besteverTsRaw = await redis.get('lastBesteverScrape');
      if (besteverTsRaw) lastBesteverScrape = besteverTsRaw;
      const besteverDetailsTsRaw = await redis.get('lastBesteverDetailsScrape');
      if (besteverDetailsTsRaw) lastBesteverDetailsScrape = besteverDetailsTsRaw;
    } catch(e) { console.warn('Erreur chargement Redis:', e.message); }
  }

  // Fallback fichier local si Redis vide ou absent
  if (Object.keys(userdata).length === 0) {
    const file = path.join(DATA_DIR, 'userdata.json');
    try {
      if (fs.existsSync(file)) {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (isOldUserdataFormat(parsed)) migrateUserdata(parsed);
        else userdata = parsed;
        console.log(`📂 Fallback userdata.json chargé`);
        if (redis) {
          redis.set('users',    JSON.stringify(users)).catch(() => {});
          redis.set('userdata', JSON.stringify(userdata)).catch(() => {});
          console.log('🔄 Données resynchronisées vers Redis');
        }
      }
    } catch(e) { console.warn('Impossible de charger userdata.json:', e.message); }
  }
}

// Sauvegarde des horodatages de scraping
async function saveLastScrape() {
  lastScrape = new Date().toISOString();
  if (redis) {
    try { await redis.set('lastScrape', lastScrape); }
    catch(e) { console.warn('Erreur sauvegarde lastScrape:', e.message); }
  }
}

// Sauvegarde des profils dans Redis
async function saveUsers() {
  if (redis) {
    try { await redis.set('users', JSON.stringify(users)); }
    catch(e) { console.warn('Erreur sauvegarde users Redis:', e.message); }
  }
}

// Sauvegarde des préférences UI dans Redis
async function savePrefsData() {
  if (redis) {
    try { await redis.set('prefs', JSON.stringify(prefsDB)); }
    catch(e) { console.warn('Erreur sauvegarde prefs Redis:', e.message); }
  }
}

/**
 * Sauvegarde les notes utilisateurs :
 *   1. Toujours dans le fichier local userdata.json (backup de secours)
 *   2. Dans Redis si disponible
 */
async function saveUserdataFile() {
  try {
    fs.writeFileSync(path.join(DATA_DIR, 'userdata.json'), JSON.stringify(userdata, null, 2), 'utf8');
  } catch(e) { console.warn('Erreur sauvegarde fichier local:', e.message); }
  if (redis) {
    try { await redis.set('userdata', JSON.stringify(userdata)); }
    catch(e) { console.warn('Erreur sauvegarde Redis:', e.message); }
  }
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 3 — HTTP & CACHE
//
//  • BROWSER_HEADERS : headers qui imitent un vrai navigateur (évite le blocage)
//  • rateLimitedFetch : file d'attente sérialisée (max 1 req / ALLO_DELAY ms)
//                       + pause longue tous les ALLO_BURST_EVERY requêtes
//  • pageCache : cache en mémoire des pages de liste (TTL 20 min, évite de
//                re-télécharger si l'utilisateur relance un scraping)
//  • detailsCache : cache en mémoire des fiches films (TTL 24h, sauvegardé
//                   dans Redis avec un debounce de 8s)
// ══════════════════════════════════════════════════════════════════════════════

const detailsCache = new Map(); // clé: "id:XXXXX" ou "q:titre" → { value, cachedAt }

/** Headers imitant Chrome 124 sur Mac — réduit le risque de soft-block AlloCiné */
const BROWSER_HEADERS = {
  'User-Agent':       'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Accept':           'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language':  'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
  'Accept-Encoding':  'gzip, deflate, br',
  'Connection':       'keep-alive',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Fetch-Dest':   'document',
  'Sec-Fetch-Mode':   'navigate',
  'Sec-Fetch-Site':   'none',
};

/** Instance Axios commune avec timeout 15s et acceptance des 4xx (pour les gérer manuellement) */
const http = axios.create({
  headers: BROWSER_HEADERS,
  timeout: 15000,
  validateStatus: (status) => status >= 200 && status < 500,
});

// ── Cache des pages de liste (20 min) ────────────────────────────────────────
const pageCache   = new Map();
const PAGE_TTL_MS = 1000 * 60 * 20;

function getCachedPage(key) {
  const c = pageCache.get(key);
  if (!c) return null;
  if (Date.now() - c.cachedAt > PAGE_TTL_MS) { pageCache.delete(key); return null; }
  return c.html;
}
function setCachedPage(key, html) {
  pageCache.set(key, { html, cachedAt: Date.now() });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Effectue une requête GET avec retry automatique.
 * Sur 429 (rate limit AlloCiné), attend 60s avant de réessayer.
 * Sur erreur réseau, backoff exponentiel (3s, 6s, 9s…).
 * @param {string} url
 * @param {number} retries  nombre de tentatives supplémentaires après la première (défaut 4)
 */
async function fetchWithRetry(url, retries = 4) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await http.get(url, {
        headers: { ...BROWSER_HEADERS, Referer: 'https://www.allocine.fr/' },
      });
      if (resp.status === 429) {
        const wait = 60000; // AlloCiné lève le ban après ~1 min
        console.warn(`429 — attente ${wait / 1000}s (tentative ${attempt + 1}/${retries + 1})…`);
        if (attempt === retries) throw Object.assign(new Error('HTTP 429 après toutes les tentatives'), { response: resp });
        await sleep(wait);
        continue;
      }
      if (resp.status >= 400) throw Object.assign(new Error(`HTTP ${resp.status}`), { response: resp });
      return resp;
    } catch (err) {
      if (attempt === retries) throw err;
      const wait = 3000 * (attempt + 1);
      console.warn(`Erreur réseau, retry dans ${wait / 1000}s…`);
      await sleep(wait);
    }
  }
  throw new Error('fetchWithRetry: toutes les tentatives épuisées');
}

// ── File d'attente sérialisée pour AlloCiné ───────────────────────────────────
// Toutes les requêtes vers AlloCiné passent par rateLimitedFetch, qui :
//   • les sérialise (1 seule requête active à la fois)
//   • impose un délai minimal de ALLO_DELAY ms entre chaque requête
//   • fait une pause longue (ALLO_BURST_PAUSE) toutes les ALLO_BURST_EVERY requêtes
const ALLO_DELAY       = 900;   // ms entre chaque requête
const ALLO_BURST_EVERY = 50;    // pause longue toutes les N requêtes
const ALLO_BURST_PAUSE = 12000; // 12s de cooldown anti-ban

let _alloQueue    = Promise.resolve();
let _alloReqCount = 0;

/**
 * Enchaîne la requête dans la file d'attente sérialisée.
 * La queue avance même si un ticket échoue (catch sur _alloQueue).
 */
function rateLimitedFetch(url) {
  const ticket = _alloQueue.then(async () => {
    _alloReqCount++;
    if (_alloReqCount % ALLO_BURST_EVERY === 0) {
      console.log(`⏸  Cooldown anti-429 (${_alloReqCount} requêtes) — pause ${ALLO_BURST_PAUSE / 1000}s…`);
      await sleep(ALLO_BURST_PAUSE);
    }
    const resp = await fetchWithRetry(url);
    await sleep(ALLO_DELAY);
    return resp;
  });
  _alloQueue = ticket.catch(() => sleep(ALLO_DELAY));
  return ticket;
}

// ── Cache des fiches films (24h, sauvegardé dans Redis en différé) ────────────
function getCachedDetails(key) {
  const cached = detailsCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.cachedAt > DETAILS_TTL_MS) { detailsCache.delete(key); return null; }
  return cached.value;
}

function setCachedDetails(key, value) {
  detailsCache.set(key, { value, cachedAt: Date.now() });
  if (value && value.providers && value.providers.length > 0) {
    lastDetailsScrape = new Date().toISOString();
  }
  scheduleDetailsBackup();
}

/** Debounce 8s — évite de saturer Redis en cas d'appels en rafale à /api/details */
let _detailsBackupTimer = null;
function scheduleDetailsBackup() {
  clearTimeout(_detailsBackupTimer);
  _detailsBackupTimer = setTimeout(saveDetailsCache, 8000);
}

async function saveDetailsCache() {
  if (!redis) return;
  try {
    const obj = {};
    detailsCache.forEach((v, k) => { obj[k] = v; });
    await redis.set('details', JSON.stringify(obj));
    if (lastDetailsScrape) await redis.set('lastDetailsScrape', lastDetailsScrape);
    console.log(`💾 détailsCache sauvegardé (${detailsCache.size} entrées)`);
  } catch(e) { console.warn('Erreur sauvegarde détailsCache:', e.message); }
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 4 — UTILITAIRES DE PARSING TEXTE
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Convertit un bloc HTML en tableau de lignes de texte nettoyées.
 * Utilisé pour parser à la fois les pages de liste et les fiches détail.
 *
 * Étapes :
 *   1. Supprime <script> et <style>
 *   2. Convertit <br> en saut de ligne
 *   3. Ajoute des sauts de ligne autour des balises bloc (div, p, li…)
 *   4. Supprime toutes les balises HTML restantes
 *   5. Décode les entités HTML (&amp; &nbsp; &#xxx;…)
 *   6. Découpe en lignes, trim, filtre les lignes vides
 *   7. Cas spécial : fusionne les lignes "X VOD" (ex: "Titre\n VOD" → "Titre VOD")
 *
 * @param  {string} html
 * @returns {string[]} lignes de texte non vides
 */
function htmlToLines(html) {
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/?(div|p|li|ul|ol|span|a|h[1-6]|header|footer|nav|section|article|td|th|tr|button|label)[^>]*>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g,    '&')
    .replace(/&lt;/g,     '<')
    .replace(/&gt;/g,     '>')
    .replace(/&nbsp;/g,   ' ')
    .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(parseInt(c, 10)))
    .replace(/&[a-z]+;/gi, '');

  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  // Fusionne "VOD" orphelin avec la ligne précédente (artefact des pages de liste films)
  for (let i = lines.length - 1; i > 0; i--) {
    if (lines[i] === 'VOD') {
      lines[i - 1] = `${lines[i - 1]} VOD`;
      lines.splice(i, 1);
    }
  }

  return lines;
}

/**
 * Normalise un titre pour comparaison insensible à la casse, aux accents
 * et à la ponctuation. Utilisé pour faire correspondre titres AlloCiné
 * avec les titres extraits des pages de liste.
 * @param  {string} value
 * @returns {string} titre normalisé (ex: "L'Été meurtrier" → "l ete meurtrier")
 */
function normalizeTitle(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/&/g, ' et ')
    .replace(/['']/g, ' ')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .replace(/\s+/g, ' ');
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 5 — SCRAPING FILMS : EXTRACTION & PARSING
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Extrait un mapping titre → allocineId depuis une PAGE DE LISTE VOD.
 * Les liens ont la forme : /film/fichefilm-XXXXX/telecharger-vod/
 * Le texte du lien est "Titre du film VOD" → on retire " VOD".
 * Utilisé pour associer les films parsés (lignes de texte) à leur ID AlloCiné.
 * @param  {string} html
 * @returns {Map<string, string>} normalizeTitle(titre) → allocineId
 */
function extractIdsFromListingPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();

  $('a[href*="fichefilm-"][href*="telecharger-vod"]').each((_, el) => {
    const href    = $(el).attr('href') || '';
    const idMatch = href.match(/fichefilm-(\d+)/);
    if (!idMatch) return;

    const rawText = $(el).text().trim().replace(/\s+VOD$/i, '').trim();
    if (!rawText) return;

    const key = normalizeTitle(rawText);
    if (!key || titleToId.has(key)) return;
    titleToId.set(key, idMatch[1]);
  });

  return titleToId;
}

/**
 * Extrait un mapping allocineId → URL du poster depuis une PAGE DE LISTE VOD.
 * Cherche l'image dans l'article/li parent du lien VOD.
 * @param  {string} html
 * @returns {Map<string, string>} allocineId → URL poster
 */
function extractPostersFromListingPage(html) {
  const $ = cheerio.load(html);
  const idToPoster = new Map();

  $('a[href*="fichefilm-"][href*="telecharger-vod"]').each((_, el) => {
    const href    = $(el).attr('href') || '';
    const idMatch = href.match(/fichefilm-(\d+)/);
    if (!idMatch) return;
    const id   = idMatch[1];
    const $card = $(el).closest('article, li, [class*="card"], [class*="item"]');
    const $img  = ($card.length ? $card : $(el).parent()).find('img').first();
    const rawSrc = $img.attr('data-src') || $img.attr('data-lazy-src') || $img.attr('data-original') || $img.attr('src') || '';
    if (rawSrc && !/blank|placeholder|gif$/i.test(rawSrc)) {
      const poster = rawSrc.startsWith('/') ? 'https://www.allocine.fr' + rawSrc : rawSrc;
      idToPoster.set(id, poster);
    }
  });

  return idToPoster;
}

/**
 * Extrait un mapping titre → allocineId depuis la PAGE DE RECHERCHE AlloCiné.
 * Les liens ont la forme : /film/fichefilm_gen_cfilm=XXXXX.html
 * Utilisé en fallback quand un film n'a pas d'ID depuis la page de liste.
 * @param  {string} html
 * @returns {Map<string, string>} normalizeTitle(titre) → allocineId
 */
function extractIdsFromSearchPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();

  $('a[href*="fichefilm_gen_cfilm="]').each((_, el) => {
    const href    = $(el).attr('href') || '';
    const idMatch = href.match(/fichefilm_gen_cfilm=(\d+)/);
    if (!idMatch) return;

    // Essaie plusieurs sources de texte pour trouver le titre
    const texts = [
      $(el).text(),
      $(el).attr('title'),
      $(el).find('img').attr('alt'),
      $(el).closest('article, li, div').find('h2, h3, .meta-title, .meta-title-link').first().text(),
    ];
    const title = texts.map((v) => String(v || '').trim()).find((v) => v && !/^image:/i.test(v));
    if (!title) return;

    const key = normalizeTitle(title);
    if (!key || titleToId.has(key)) return;
    titleToId.set(key, idMatch[1]);
  });

  return titleToId;
}

/** Plateformes à exclure de l'affichage (boutiques achats unitaires, services niches) */
const PROVIDERS_BLACKLIST = new Set([
  'universciné', 'universcine', 'rakuten tv', 'filmo', 'viva',
  'google play', 'microsoft', 'xbox', 'crunchyroll', 'molotov',
  'sooner', 'pathé home', 'pathe home', 'premieremax', 'première max',
  'tenk', 'tënk', 'cinemutins', 'cinémutins',
  'capuseen', 'capuseën', 'orange', 'arte boutique',
  'ciné+', 'cine+', 'ocs', 'ciné+ ocs', 'cine+ ocs',
  'en dvd blu-ray', 'en dvd/blu-ray', 'dvd blu-ray', 'dvd/blu-ray',
  'blu-ray', 'dvd',
]);

/**
 * Extrait les plateformes VOD/streaming depuis la page fiche d'un film ou d'une série.
 * Classe chaque plateforme en : svod | location | achat | vod (générique).
 * La classification prioritaire utilise le texte parent de la tuile ;
 * la classification secondaire (fallback) est par nom de plateforme.
 * @param  {string} html
 * @returns {{ name: string, type: 'svod'|'location'|'achat'|'vod' }[]}
 */
function extractProviders(html) {
  const $ = cheerio.load(html);
  const providers = [];
  const seen = new Set();

  $('.provider-tile-primary').each((_, el) => {
    const name = $(el).text().trim();
    if (!name || seen.has(name)) return;
    if (PROVIDERS_BLACKLIST.has(name.toLowerCase())) return;
    seen.add(name);

    const tileText = $(el).parent().text().toLowerCase();
    let type = 'vod';
    if (/inclus|abonnement|svod/i.test(tileText))     type = 'svod';
    else if (/location/i.test(tileText))               type = 'location';
    else if (/achat/i.test(tileText))                  type = 'achat';

    // Fallback par nom : plateformes clairement SVOD
    if (type === 'vod' && /netflix|prime video|disney\+|canal\+|ocs|paramount\+|crunchyroll|apple tv\+|molotov|arte|france\.tv/i.test(name))
      type = 'svod';

    providers.push({ name, type });
  });

  return providers;
}

/**
 * Parse une page de liste de films VOD AlloCiné.
 * Stratégie :
 *   1. Extrait le mapping titre→ID et ID→poster via les liens /fichefilm-
 *   2. Parcourt les lignes de texte en cherchant le motif "Presse" + note
 *   3. Remonte vers le titre (ligne se terminant par " VOD")
 *   4. Extrait genre, réalisateur, acteurs, titre original dans le segment entre titre et note
 *   5. Cherche un synopsis après la note
 *   6. Associe l'ID AlloCiné et le poster par correspondance de titre normalisé
 *
 * @param  {string} html
 * @returns {Array<{ titre, titreOriginal, genre, realisateur, acteurs, notePresse, noteSpect, synopsis, allocineId, poster }>}
 */
function parseFilms(html) {
  const titleToId  = extractIdsFromListingPage(html);
  const idToPoster = extractPostersFromListingPage(html);
  const lines      = htmlToLines(html);
  const films      = [];

  for (let i = 0; i < lines.length; i++) {
    // Ancre : "Presse" suivi d'une note de la forme "X,X"
    if (!(lines[i] === 'Presse' && lines[i + 1] && /^\d[,.]\d$/.test(lines[i + 1]))) continue;

    const notePresse = parseFloat(lines[i + 1].replace(',', '.'));
    const noteSpect  = lines[i + 2] === 'Spectateurs' && lines[i + 3] && /^\d[,.]\d$/.test(lines[i + 3])
      ? parseFloat(lines[i + 3].replace(',', '.')) : null;

    let titre = '', genre = '', realisateur = '', acteurs = '', titreOriginal = '';
    let titleIdx = -1;

    // Remonte pour trouver le titre (ligne se terminant par " VOD")
    for (let j = i - 1; j >= Math.max(0, i - 20); j--) {
      if (lines[j].endsWith(' VOD')) { titleIdx = j; break; }
    }

    if (titleIdx >= 0) {
      titre = lines[titleIdx].slice(0, -4).trim(); // retire " VOD"
      const seg    = lines.slice(titleIdx + 1, i);
      const deIdx  = seg.indexOf('De');
      const avecIdx= seg.indexOf('Avec');
      const origIdx= seg.indexOf('Titre original');
      const pipeIdx= seg.findIndex((line) => line === '|');

      // Genre : entre le premier "|" et "De" (s'arrête avant "Titre original" s'il est intercalé)
      if (pipeIdx >= 0 && deIdx > pipeIdx) {
        const genreEnd = (origIdx >= 0 && origIdx > pipeIdx && origIdx < deIdx) ? origIdx : deIdx;
        genre = seg
          .slice(pipeIdx + 1, genreEnd)
          .filter((line) => !/^\d+h/.test(line) && line !== '|' && line !== 'Titre original')
          .map((line) => line.replace(/,$/, ''))
          .join(', ');
      }

      if (deIdx >= 0 && seg[deIdx + 1])    realisateur = seg[deIdx + 1];

      if (avecIdx >= 0) {
        const end = origIdx > avecIdx ? origIdx : seg.length;
        acteurs = seg
          .slice(avecIdx + 1, end)
          .map((line) => line.replace(/,$/, ''))
          .filter((line) => !line.startsWith('Dès '))
          .join(', ');
      }

      if (origIdx >= 0 && seg[origIdx + 1]) titreOriginal = seg[origIdx + 1];
    }

    // Synopsis : première ligne longue (> 80 chars) après la note
    let synopsis = '';
    const synStart = lines[i + 2] === 'Spectateurs' ? i + 4 : i + 2;
    for (let k = synStart; k < Math.min(lines.length, synStart + 12); k++) {
      if (lines[k].endsWith(' VOD')) break;
      if (lines[k].length > 80 && !/^\d/.test(lines[k]) && !lines[k].startsWith('Dès ')) {
        synopsis = lines[k]; break;
      }
    }

    if (titre) {
      const titleKey    = normalizeTitle(titre);
      const originalKey = normalizeTitle(titreOriginal);
      const allocineId  = titleToId.get(titleKey) || titleToId.get(originalKey) || null;
      const poster      = allocineId ? (idToPoster.get(allocineId) || null) : null;
      films.push({ titre, titreOriginal, genre, realisateur, acteurs, notePresse, noteSpect, synopsis, allocineId, poster });
    }
  }

  return films;
}

/**
 * Déduplique et trie les films par note presse décroissante.
 * La déduplication utilise la clé "titre|notePresse".
 * @param  {Array}  films
 * @param  {number} noteMin  note presse minimale (les films en dessous sont filtrés)
 */
function dedupeAndSortFilms(films, noteMin) {
  const seen = new Set();
  return films
    .filter((film) => {
      const key = `${film.titre.toLowerCase()}|${film.notePresse}`;
      if (seen.has(key)) return false;
      seen.add(key); return true;
    })
    .filter((film) => film.notePresse >= noteMin)
    .sort((a, b) => b.notePresse - a.notePresse || a.titre.localeCompare(b.titre, 'fr'));
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 6 — API FILMS
// ══════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/films
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne la liste complète des films en cache (issue du dernier scraping),
 *        accompagnée des détails en cache (plateformes, pays, année) pour chaque film.
 *        Permet au client d'afficher les données sans effectuer de requêtes individuelles.
 *
 * Réponse : {
 *   films:     Film[]      — liste de films triés par note presse
 *   lastScrape: string|null — ISO date du dernier scraping
 *   count:     number       — nombre de films
 *   details:   (Detail|null)[] — détails indexés par position (même ordre que films[])
 * }
 */
/**
 * GET /api/config
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne la configuration publique nécessaire au client au démarrage.
 *        Expose le secret applicatif pour que le client puisse s'authentifier
 *        sur les endpoints protégés.
 *
 * Réponse: { secret: string }
 */
app.get('/api/config', (_req, res) => {
  res.json({ secret: APP_SECRET || '' });
});

app.get('/api/films', (_req, res) => {
  const details = cachedFilms.map(film => {
    const key = film.allocineId ? `id:${film.allocineId}` : `q:${film.titre}`;
    return getCachedDetails(key) || null;
  });
  res.json({ films: cachedFilms, lastScrape, count: cachedFilms.length, details });
});

/**
 * GET /api/users
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne la liste de tous les profils utilisateurs.
 *
 * Réponse : { id, name, createdAt }[]
 */
app.get('/api/users', (_req, res) => {
  res.json(Object.values(users));
});

/**
 * POST /api/users
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Crée un nouveau profil utilisateur.
 *
 * Body   : { name: string }
 * Réponse: { id, name, createdAt }
 * Erreur : 400 si name manquant ou vide
 */
app.post('/api/users', requireSecret, async (req, res) => {
  const { name } = req.body;
  if (!name || typeof name !== 'string' || !name.trim() || name.trim().length > 50)
    return res.status(400).json({ error: 'name requis (1–50 caractères)' });

  const id   = Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  const user = { id, name: name.trim(), createdAt: new Date().toISOString() };
  users[id]    = user;
  userdata[id] = userdata[id] || {};
  await saveUsers();
  await saveUserdataFile();
  console.log(`👤 Nouveau profil : "${user.name}" (${id})`);
  res.json(user);
});

/**
 * DELETE /api/users/:id
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Supprime un profil utilisateur et toutes ses données (notes, prefs).
 *        Réservé à l'administrateur (user_default). Refusé pour user_default lui-même.
 *
 * Params : id (route)
 * Réponse: { ok, deleted }
 * Erreur : 403 si tentative de suppression de user_default | 404 si introuvable
 */
app.delete('/api/users/:id', requireSecret, async (req, res) => {
  const { id } = req.params;
  if (id === 'user_default')
    return res.status(403).json({ error: 'Impossible de supprimer le profil administrateur' });
  if (!users[id])
    return res.status(404).json({ error: 'Profil introuvable' });
  const name = users[id].name;
  delete users[id];
  delete userdata[id];
  delete prefsDB[id];
  await saveUsers();
  await saveUserdataFile();
  console.log(`🗑️  Profil supprimé : "${name}" (${id})`);
  res.json({ ok: true, deleted: id });
});

/**
 * GET /api/prefs?userId=…
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne les préférences d'affichage d'un profil utilisateur.
 *        Le serveur est la source de vérité (synchronisé avec le localStorage client).
 *
 * Params : userId (query)
 * Réponse: { showDocumentaires, showAnimations, hideVus, hideNonInteresse }
 * Erreur : 400 si userId manquant
 */
app.get('/api/prefs', (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId requis' });
  res.json(prefsDB[userId] || {});
});

/**
 * POST /api/prefs
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Sauvegarde les préférences d'affichage d'un profil utilisateur.
 *
 * Body   : { userId: string, ...prefs }
 * Réponse: { ok: true }
 * Erreur : 400 si userId manquant
 */
app.post('/api/prefs', requireSecret, async (req, res) => {
  const { userId, ...userPrefs } = req.body;
  if (!userId || !users[userId]) return res.status(400).json({ error: 'userId invalide' });
  prefsDB[userId] = userPrefs;
  await savePrefsData();
  res.json({ ok: true });
});

/**
 * GET /api/userdata?userId=…
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne toutes les notes d'un profil utilisateur
 *        (vu, à voir, non intéressé, à suivre) pour tous les films/séries annotés.
 *
 * Params : userId (query)
 * Réponse: { allocineId: { vu, vouloir, nonInteresse, asuivre } }
 * Erreur : 400 si userId manquant
 */
app.get('/api/userdata', (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId requis' });
  res.json(userdata[userId] || {});
});

/**
 * POST /api/userdata
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Met à jour la note d'un utilisateur pour un film ou une série.
 *        Si toutes les valeurs booléennes sont false, l'entrée est supprimée
 *        (pas de stockage inutile d'entrées "vides").
 *
 * Body   : { userId, id, vu, vouloir, nonInteresse, asuivre, noteAC }
 * Réponse: { ok: true }
 * Erreur : 400 si userId ou id manquant
 */
app.post('/api/userdata', requireSecret, async (req, res) => {
  const { userId, id, vu, vouloir, nonInteresse, asuivre, noteAC } = req.body;
  if (!userId || !id || !users[userId]) return res.status(400).json({ error: 'userId invalide ou id manquant' });
  if (!userdata[userId]) userdata[userId] = {};
  const existing = userdata[userId][id] || {};
  const entry = { vu: !!vu, vouloir: !!vouloir, nonInteresse: !!nonInteresse, asuivre: !!asuivre };
  // Conserver noteAC existant si non fourni, écraser si fourni (null = suppression)
  const resolvedNote = noteAC !== undefined ? noteAC : existing.noteAC;
  if (resolvedNote !== undefined && resolvedNote !== null) entry.noteAC = resolvedNote;
  const isEmpty = !entry.vu && !entry.vouloir && !entry.nonInteresse && !entry.asuivre && !entry.noteAC;
  if (isEmpty) {
    delete userdata[userId][id]; // purge les entrées entièrement vides
  } else {
    userdata[userId][id] = entry;
  }
  await saveUserdataFile();
  res.json({ ok: true });
});

/**
 * GET /api/userdata/stats
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne les compteurs films/séries par profil.
 *        Films  → clés sans préfixe  (ex: "29")
 *        Séries → clés avec préfixe  (ex: "s:22881")
 *
 * Réponse : { [userId]: { name, films: { vu, vouloir, nonInteresse }, series: { vu, vouloir, asuivre, nonInteresse } } }
 */
app.get('/api/userdata/stats', (_req, res) => {
  const result = {};
  for (const [userId, ud] of Object.entries(userdata)) {
    const u = users[userId];
    if (!u) continue;
    const stats = {
      name: u.name,
      films:  { vu: 0, vouloir: 0, nonInteresse: 0 },
      series: { vu: 0, vouloir: 0, asuivre: 0, nonInteresse: 0 },
    };
    for (const [key, entry] of Object.entries(ud)) {
      const isSerie = key.startsWith('s:');
      if (isSerie) {
        if (entry.vu)           stats.series.vu++;
        if (entry.vouloir)      stats.series.vouloir++;
        if (entry.asuivre)      stats.series.asuivre++;
        if (entry.nonInteresse) stats.series.nonInteresse++;
      } else {
        if (entry.vu)           stats.films.vu++;
        if (entry.vouloir)      stats.films.vouloir++;
        if (entry.nonInteresse) stats.films.nonInteresse++;
      }
    }
    result[userId] = stats;
  }
  res.json(result);
});

/**
 * POST /api/userdata/import-ac-notes
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Import en masse des notes AlloCiné pour un profil.
 *        Pour les films (isSeries absent ou false) : vu est forcé à true,
 *        car la page "Mes films vus" garantit que le film a été regardé.
 *        Pour les séries (isSeries: true) : vu est préservé, car une note
 *        peut être donnée après quelques saisons sans avoir tout vu.
 *        Les champs vouloir/nonInteresse/asuivre existants sont toujours préservés.
 *
 * Body   : { userId, films: [{ allocineId, noteAC }], isSeries? }
 * Réponse: { ok: true, imported: N }
 */
app.options('/api/userdata/import-ac-notes', (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-app-secret');
  res.sendStatus(200);
});
app.post('/api/userdata/import-ac-notes', requireSecret, async (req, res) => {
  res.header('Access-Control-Allow-Origin', '*');
  const { userId, films, isSeries } = req.body;
  if (!userId || !users[userId] || !Array.isArray(films)) {
    return res.status(400).json({ error: 'userId invalide ou films manquant' });
  }
  if (!userdata[userId]) userdata[userId] = {};
  let count = 0;
  for (const { allocineId, noteAC } of films) {
    if (!allocineId) continue;
    // Les séries utilisent le préfixe "s:" pour distinguer des films (ex: s:22881)
    const key = isSeries ? `s:${allocineId}` : String(allocineId);
    const existing = userdata[userId][key] || {};
    // Pour les séries : on ne force pas vu, on préserve la valeur existante
    // (une note ≠ série entièrement visionnée — plusieurs saisons possibles)
    const resolvedVu = isSeries ? (existing.vu || false) : true;
    userdata[userId][key] = {
      vu:            resolvedVu,
      vouloir:       existing.vouloir       || false,
      nonInteresse:  existing.nonInteresse  || false,
      asuivre:       existing.asuivre       || false,
      noteAC:        typeof noteAC === 'number' ? noteAC : existing.noteAC,
    };
    count++;
  }
  await saveUserdataFile();
  const label = isSeries ? 'séries' : 'films';
  console.log(`📥 Import AC notes : ${count} ${label} pour userId=${userId}`);
  res.json({ ok: true, imported: count });
});

/**
 * GET /api/health
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne les informations de santé du serveur films.
 *        Utilisé par le client pour savoir si le serveur répond et activer
 *        les boutons de scraping dans l'UI.
 *
 * Réponse: { ok, port, totalPages, cachedDetails, cachedFilms, lastScrape,
 *            lastDetailsScrape, version, serverStart, lastScrapeErrors }
 */
app.get('/api/health', (_req, res) => {
  res.json({
    ok: true, isScraping, totalPages: TOTAL_PAGES,
    cachedDetails: detailsCache.size, cachedFilms: cachedFilms.length,
    lastScrape, lastDetailsScrape,
    version: VERSION, serverStart: SERVER_START,
    lastScrapeErrors,
  });
});

/**
 * GET /api/scrape-status
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne l'état d'avancement du scraping films en cours.
 *        Utilisé par le client pour afficher une barre de progression.
 *
 * Réponse: { isScraping, pct (0-100), annee }
 */
app.get('/api/scrape-status', (_req, res) => {
  const pct = scrapeProgress.total
    ? Math.round(scrapeProgress.current / scrapeProgress.total * 100) : 0;
  res.json({ isScraping, pct, annee: scrapeProgress.annee });
});

/**
 * GET /api/scraping-status
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne l'état d'avancement des 4 scrapings nocturnes.
 *        Utilisé par le menu Info pour afficher la progression en temps réel.
 *
 * Réponse: { phase, filmsList, filmsDetails, seriesList, seriesDetails }
 *   phase = null | 'films-list' | 'films-details' | 'series-list' | 'series-details'
 *   chaque section : { active: bool, pct: 0-100, label?: string }
 */
app.get('/api/scraping-status', (_req, res) => {
  const pctFilmsList      = scrapeProgress.total
    ? Math.round(scrapeProgress.current / scrapeProgress.total * 100) : 0;
  const pctFilmsDetails   = filmsDetailsProgress.total
    ? Math.round(filmsDetailsProgress.current / filmsDetailsProgress.total * 100) : 0;
  const pctSeriesList     = seriesProgress.total
    ? Math.round(seriesProgress.current / seriesProgress.total * 100) : 0;
  const pctSeriesDetails  = seriesDetailsProgress.total
    ? Math.round(seriesDetailsProgress.current / seriesDetailsProgress.total * 100) : 0;
  const pctBesteverList   = besteverProgress.total
    ? Math.round(besteverProgress.current / besteverProgress.total * 100) : 0;
  const pctBesteverDetails= besteverDetailsProgress.total
    ? Math.round(besteverDetailsProgress.current / besteverDetailsProgress.total * 100) : 0;
  res.json({
    phase: scrapingPhase,
    filmsList:       { active: scrapingPhase === 'films-list',      pct: pctFilmsList,       annee: scrapeProgress.annee },
    filmsDetails:    { active: scrapingPhase === 'films-details',   pct: pctFilmsDetails,    total: filmsDetailsProgress.total },
    seriesList:      { active: scrapingPhase === 'series-list',     pct: pctSeriesList       },
    seriesDetails:   { active: scrapingPhase === 'series-details',  pct: pctSeriesDetails,   total: seriesDetailsProgress.total },
    besteverList:    { active: scrapingPhase === 'bestever-list',   pct: pctBesteverList     },
    besteverDetails: { active: scrapingPhase === 'bestever-details',pct: pctBesteverDetails, total: besteverDetailsProgress.total },
  });
});

/**
 * GET /api/scrape?annees=2026,2025&noteMin=3.5
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Lance le scraping des pages VOD AlloCiné pour les années demandées.
 *        Répond en Server-Sent Events (SSE) pour envoyer les films au fur
 *        et à mesure de leur extraction, sans attendre la fin.
 *        À la fin, persiste la liste dédupliquée dans Redis.
 *
 * Params (query) :
 *   annees   : ex "2026,2025" (défaut "2025"). Aussi accepte l'ancien param ?annee=
 *   noteMin  : note presse minimale 0-5 (défaut 3.5)
 *
 * Événements SSE :
 *   { type: 'progress', page, total, annee }
 *   { type: 'films',    films[], annee, page, total }
 *   { type: 'error',    page, message }
 *   { type: 'done',     totalFilms, lastScrape }
 *
 * Erreur : 429 si scraping déjà en cours
 */
app.get('/api/scrape', requireSecret, requireRateLimit(5, 10 * 60 * 1000), async (req, res) => {
  if (isScraping) return res.status(429).json({ error: 'Scraping déjà en cours' });
  isScraping = true;

  // Support des deux formats : annees=2023,2024 ou l'ancien annee=2025
  const anneesParam = String(req.query.annees || req.query.annee || '2025').trim();
  const annees = anneesParam.split(',').map(s => s.trim()).filter(s => /^\d{4}$/.test(s));
  if (annees.length === 0) return res.status(400).json({ error: 'annees invalide' });

  const noteMin = parseFloat(String(req.query.noteMin || '3.5'));
  if (Number.isNaN(noteMin) || noteMin < 0 || noteMin > 5)
    return res.status(400).json({ error: 'noteMin invalide' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send        = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  const allFilms    = [];
  const totalPages  = annees.length * TOTAL_PAGES;
  let   globalPage  = 0;
  lastScrapeErrors  = [];

  for (const annee of annees) {
    const base = `https://www.allocine.fr/vod/films/decennie-2020/annee-${annee}/?page=`;
    for (let page = 1; page <= TOTAL_PAGES; page++) {
      globalPage++;
      send({ type: 'progress', page: globalPage, total: totalPages, annee });
      const url = base + page;
      try {
        let html = getCachedPage(url);
        if (html) {
          console.log(`[${annee}] Page ${page}/${TOTAL_PAGES} → cache`);
        } else {
          const response = await fetchWithRetry(url);
          html = response.data;
          setCachedPage(url, html);
        }
        const raw   = parseFilms(html).map(f => ({ ...f, anneeSortie: annee }));
        const films = raw.filter(f => f.notePresse >= noteMin);
        allFilms.push(...raw);
        console.log(`[${annee}] Page ${page}/${TOTAL_PAGES} → ${films.length} films`);
        send({ type: 'films', films, annee, page: globalPage, total: totalPages });
      } catch (error) {
        const message = error.response ? `HTTP ${error.response.status}` : error.code || error.message;
        console.error(`[${annee}] Page ${page} erreur: ${message}`);
        lastScrapeErrors.push({ page: globalPage, annee, message });
        send({ type: 'error', page: globalPage, message });
      }
      await sleep(1500 + Math.random() * 500);
    }
  }

  const result = dedupeAndSortFilms(allFilms, noteMin);
  cachedFilms  = result;
  await saveLastScrape();
  if (redis) {
    try { await redis.set('films', JSON.stringify(result)); }
    catch(e) { console.warn('Erreur sauvegarde films Redis:', e.message); }
  }
  send({ type: 'done', totalFilms: result.length, lastScrape });
  res.end();
  isScraping = false;
  console.log(`✅ ${result.length} films (note >= ${noteMin}) — années: ${annees.join(', ')}`);
});

/**
 * GET /api/details?allocineId=XXXXX  ou  ?q=Titre%20du%20film
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Récupère les détails d'un film depuis sa fiche AlloCiné :
 *        pays de production, année, URL et plateformes disponibles (VOD/SVOD/location).
 *        Le résultat est mis en cache 24h en mémoire et dans Redis.
 *
 *        Stratégie de résolution de l'ID :
 *          1. allocineId fourni → requête directe sur la fiche
 *          2. Sinon → recherche AlloCiné (?q=...) pour trouver l'ID
 *
 *        Détection soft-block : si la page retournée ne contient pas les
 *        marqueurs attendus, la réponse n'est pas mise en cache.
 *
 * Params (query) :
 *   allocineId : ID numérique AlloCiné (prioritaire)
 *   q          : titre du film (fallback si pas d'ID)
 *
 * Réponse: { pays, annee, allocineId, allocineUrl, providers[], error? }
 */
app.get('/api/details', async (req, res) => {
  const allocineId = String(req.query.allocineId || '').trim();
  const query      = String(req.query.q || '').trim();
  const cacheKey   = allocineId ? `id:${allocineId}` : `q:${query}`;

  if (!allocineId && !query)
    return res.json({ pays: null, annee: null, allocineId: null, allocineUrl: null, providers: [] });

  const cached = getCachedDetails(cacheKey);
  if (cached) {
    console.log(`Cache détails: ${cacheKey} → ${cached.providers?.length || 0} plateformes`);
    return res.json(cached);
  }

  try {
    let resolvedId = allocineId;

    if (!resolvedId) {
      // Recherche par titre → récupère l'ID depuis la page de résultats
      const searchUrl  = `https://www.allocine.fr/rechercher/?q=${encodeURIComponent(query)}`;
      const searchResp = await rateLimitedFetch(searchUrl);
      const titleToId  = extractIdsFromSearchPage(searchResp.data);
      resolvedId = titleToId.get(normalizeTitle(query)) || null;
      // Dernier recours : premier ID trouvé dans le HTML brut
      if (!resolvedId) {
        const idMatch = searchResp.data.match(/fichefilm_gen_cfilm=(\d+)/);
        resolvedId = idMatch ? idMatch[1] : null;
      }
    }

    if (!resolvedId) {
      const empty = { pays: null, annee: null, allocineId: null, allocineUrl: null, providers: [] };
      setCachedDetails(cacheKey, empty);
      return res.json(empty);
    }

    const filmUrl  = `https://www.allocine.fr/film/fichefilm_gen_cfilm=${resolvedId}.html`;
    const filmResp = await rateLimitedFetch(filmUrl);

    // Vérifie que la page est bien une fiche film (pas un CAPTCHA ou une page vide)
    const isValidPage = /fichefilm_gen_cfilm|provider-tile|Titre original|Année de production/i.test(filmResp.data);
    if (!isValidPage) {
      console.warn(`Fiche ${resolvedId} → page suspecte (soft-block ?), non mise en cache`);
      return res.json({ pays: null, annee: null, allocineId: resolvedId, allocineUrl: filmUrl, providers: [], error: 'soft_block' });
    }

    // Pays, année de production et durée (via scan de lignes)
    const lines = htmlToLines(filmResp.data);
    let pays = null, annee = null, duree = null;
    for (let i = 0; i < lines.length - 1; i++) {
      if (lines[i] === 'Nationalité' || lines[i] === 'Nationalités') pays = lines[i + 1];
      if (lines[i] === 'Année de production') annee = lines[i + 1];
      if (lines[i] === 'Durée') duree = lines[i + 1];
      if (pays && annee && duree) break;
    }

    const providers = extractProviders(filmResp.data);
    const data = { pays, annee, duree, allocineId: resolvedId, allocineUrl: filmUrl, providers };

    // Ne pas mettre en cache une page vide (fiche introuvable)
    if (pays || annee || providers.length > 0) {
      setCachedDetails(cacheKey, data);
      if (query && !allocineId) setCachedDetails(`id:${resolvedId}`, data);
    }

    const pNames = providers.map(p => `${p.name}(${p.type})`).join(', ') || '—';
    console.log(`Détails "${query || resolvedId}" → ${pays || '?'} (${annee || '?'}) | ${pNames}`);
    return res.json(data);

  } catch (error) {
    const status  = error.response?.status;
    const message = status ? `HTTP ${status}` : error.code || error.message;
    console.error(`Détails "${query || allocineId}" erreur: ${message}`);
    return res.status(200).json({
      pays: null, annee: null,
      allocineId: allocineId || null, allocineUrl: null, providers: [],
      error: status === 429 ? 'rate_limited' : message,
    });
  }
});

/**
 * GET /api/ping-allocine
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vérifie que AlloCiné répond correctement (sans passer par la file
 *        d'attente, pour un test rapide). Utilisé pour le diagnostic.
 *
 * Réponse: { ok: bool, status: number|null, message?: string }
 */
app.get('/api/ping-allocine', requireSecret, async (_req, res) => {
  try {
    const r = await fetchWithRetry('https://www.allocine.fr/', 0);
    res.json({ ok: true, status: r.status });
  } catch (err) {
    const status = err.response?.status || null;
    res.json({ ok: false, status, message: err.message });
  }
});

/**
 * POST /api/clear-details-cache
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vide le cache mémoire des fiches films (pays, année, plateformes).
 *        Utile quand des résultats vides ont été mis en cache à la suite d'un
 *        soft-block AlloCiné, ou pour forcer un re-fetch des plateformes.
 *
 * Réponse: { ok: true, cleared: number }
 */
app.post('/api/clear-details-cache', requireSecret, (_req, res) => {
  const count = detailsCache.size;
  detailsCache.clear();
  console.log(`🗑️  Cache plateformes films vidé (${count} entrées supprimées)`);
  res.json({ ok: true, cleared: count });
});

/**
 * POST /api/clear-films
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vide le cache mémoire + Redis de la liste des films.
 *        Force un nouveau scraping complet de la liste au prochain appel client.
 *
 * Réponse: { ok: true, cleared: number }
 */
app.post('/api/clear-films', requireSecret, async (_req, res) => {
  const count  = cachedFilms.length;
  cachedFilms  = [];
  lastScrape   = null;
  if (redis) {
    try { await redis.del('films'); await redis.del('lastScrape'); }
    catch(e) { console.warn('Redis del films:', e.message); }
  }
  console.log(`🗑️  Cache liste films vidé (${count} entrées)`);
  res.json({ ok: true, cleared: count });
});


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 7 — AUTO-SCRAPE NOCTURNE (4 phases indépendantes)
//
//  Chaque phase vérifie si son cache est périmé (> AUTO_SCRAPE_DAYS jours).
//  Le scheduler planifie les 4 phases à 3h00, 3h15, 3h30, 3h45 heure Paris.
//  Au démarrage du serveur, les 4 phases sont aussi vérifiées en séquence
//  pour rattraper un éventuel downtime.
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Calcule les millisecondes jusqu'à la prochaine occurrence de hh:mm heure Paris.
 * Fonctionne correctement en CET (UTC+1) et CEST (UTC+2).
 */
function msTillParis(h, m) {
  const now      = new Date();
  const parisNow = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Paris' }));
  const target   = new Date(parisNow);
  target.setHours(h, m, 0, 0);
  if (target <= parisNow) target.setDate(target.getDate() + 1);
  // Reconvertit target (exprimé en "faux UTC Paris") vers le vrai UTC
  return target.getTime() + (now.getTime() - parisNow.getTime()) - now.getTime();
}

/**
 * Planifie les 4 scraping nocturnes à 3h00, 3h15, 3h30, 3h45 heure Paris.
 * Chaque scraping ne s'exécute que si le cache dépasse AUTO_SCRAPE_DAYS jours.
 * Se reprogramme automatiquement chaque nuit.
 */
function scheduleNightlyScraping() {
  function run() {
    const label = new Date().toLocaleString('fr-FR', { timeZone: 'Europe/Paris', dateStyle: 'short', timeStyle: 'short' });
    console.log(`\n🌙 Scraping nocturne — démarrage à ${label} (heure Paris)`);
    autoScrapeFilmsListIfStale();                                                         // 3h00
    setTimeout(() => autoScrapeFilmsDetailsIfStale(),     15 * 60 * 1000);               // 3h15
    setTimeout(() => autoScrapeSeriesListIfStale(),       30 * 60 * 1000);               // 3h30
    setTimeout(() => autoScrapeSeriesDetailsIfStale(),    45 * 60 * 1000);               // 3h45
    setTimeout(() => autoScrapeBesteverListIfStale(),     60 * 60 * 1000);               // 4h00
    setTimeout(() => autoScrapeBesteverDetailsIfStale(),  75 * 60 * 1000);               // 4h15
    // Reprogramme pour demain 3h00
    setTimeout(run, msTillParis(3, 0));
  }
  const delay = msTillParis(3, 0);
  console.log(`🕒 Prochain scraping nocturne dans ${(delay / 3600000).toFixed(1)}h (3h00 heure Paris)`);
  setTimeout(run, delay);
}

// ── Phase 1 : liste des films ──────────────────────────────────────────────
/**
 * Scrape la liste des films si lastScrape > AUTO_SCRAPE_DAYS jours.
 * Déclenché au démarrage et chaque nuit à 3h00 par scheduleNightlyScraping().
 */
async function autoScrapeFilmsListIfStale() {
  if (isScraping) return;
  const ageDays = lastScrape ? (Date.now() - new Date(lastScrape).getTime()) / 86400000 : Infinity;
  if (ageDays < AUTO_SCRAPE_DAYS) {
    console.log(`⏭️  Films liste OK (${ageDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isScraping    = true;
  scrapingPhase = 'films-list';
  try {
    const label      = lastScrape ? `${ageDays.toFixed(1)} jour(s)` : 'jamais';
    console.log(`\n🔄 Auto-scrape films (liste) — dernier il y a ${label}`);
    const annees     = ['2026', '2025', '2024', '2023'];
    const allFilms   = [];
    const totalPages = annees.length * TOTAL_PAGES;
    let   globalPage = 0;
    lastScrapeErrors = [];
    scrapeProgress   = { current: 0, total: totalPages, annee: '' };
    for (const annee of annees) {
      const base = `https://www.allocine.fr/vod/films/decennie-2020/annee-${annee}/?page=`;
      for (let page = 1; page <= TOTAL_PAGES; page++) {
        globalPage++;
        scrapeProgress = { current: globalPage, total: totalPages, annee };
        const url = base + page;
        try {
          let html = getCachedPage(url);
          if (!html) { const r = await fetchWithRetry(url); html = r.data; setCachedPage(url, html); }
          const raw = parseFilms(html).map(f => ({ ...f, anneeSortie: annee }));
          allFilms.push(...raw);
          console.log(`[auto][${annee}] Page ${page}/${TOTAL_PAGES} → ${raw.length} films`);
        } catch(e) {
          console.warn(`[auto][${annee}] Page ${page} erreur: ${e.message}`);
          lastScrapeErrors.push({ page, annee, message: e.message });
        }
        await sleep(1500 + Math.random() * 500);
      }
    }
    const result = dedupeAndSortFilms(allFilms, 3.5);
    cachedFilms  = result;
    await saveLastScrape();
    if (redis) { try { await redis.set('films', JSON.stringify(result)); } catch(e) { console.warn('[auto] Redis films:', e.message); } }
    console.log(`✅ Films liste terminée — ${result.length} films\n`);
  } finally {
    isScraping     = false;
    scrapingPhase  = null;
    scrapeProgress = { current: 0, total: 0, annee: '' };
  }
}

// ── Phase 2 : plateformes des films ───────────────────────────────────────
/**
 * Scrape les plateformes/détails films si lastDetailsScrape > AUTO_SCRAPE_DAYS jours,
 * ou si la liste a été rescrapée plus récemment que les détails.
 * Déclenché au démarrage et chaque nuit à 3h15 par scheduleNightlyScraping().
 */
async function autoScrapeFilmsDetailsIfStale() {
  if (isScraping) return;
  const ageDetDays  = lastDetailsScrape ? (Date.now() - new Date(lastDetailsScrape).getTime()) / 86400000 : Infinity;
  const ageListDays = lastScrape        ? (Date.now() - new Date(lastScrape).getTime())        / 86400000 : Infinity;
  // Scrape si détails périmés OU si la liste est plus fraîche que les détails
  const detStale       = ageDetDays >= AUTO_SCRAPE_DAYS;
  const listMoreRecent = ageListDays < ageDetDays;
  if (!detStale && !listMoreRecent) {
    console.log(`⏭️  Films plateformes OK (${ageDetDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isScraping    = true;
  scrapingPhase = 'films-details';
  try {
    const toFetch = cachedFilms.filter(f => f.allocineId);
    if (toFetch.length === 0) { console.log('⏭️  Films plateformes : aucun film avec allocineId'); return; }
    const label = lastDetailsScrape ? `${ageDetDays.toFixed(1)}j` : 'jamais';
    console.log(`\n🔄 Auto-scrape films (plateformes) — ${toFetch.length} films — dernier il y a ${label}`);
    filmsDetailsProgress = { current: 0, total: toFetch.length };
    let done = 0;
    for (const film of toFetch) {
      const cacheKey = `id:${film.allocineId}`;
      detailsCache.delete(cacheKey); // force le rafraîchissement
      try {
        const filmUrl  = `https://www.allocine.fr/film/fichefilm_gen_cfilm=${film.allocineId}.html`;
        const filmResp = await rateLimitedFetch(filmUrl);
        const html     = filmResp.data;
        const isValid  = /fichefilm_gen_cfilm|provider-tile|Titre original|Année de production/i.test(html);
        if (!isValid) {
          console.warn(`[auto] Film ${film.allocineId} → page suspecte, ignorée`);
        } else {
          const lines = htmlToLines(html);
          let pays = null, annee = null;
          for (let i = 0; i < lines.length - 1; i++) {
            if (lines[i] === 'Nationalité' || lines[i] === 'Nationalités') pays = lines[i + 1];
            if (lines[i] === 'Année de production') annee = lines[i + 1];
            if (pays && annee) break;
          }
          const providers = extractProviders(html);
          const data = { pays, annee, allocineId: film.allocineId, allocineUrl: filmUrl, providers };
          if (pays || annee || providers.length > 0) setCachedDetails(cacheKey, data);
        }
      } catch(e) { console.warn(`[auto] Film ${film.allocineId}: ${e.message}`); }
      done++;
      filmsDetailsProgress.current = done;
      if (done % 50 === 0) console.log(`[auto] Plateformes films: ${done}/${toFetch.length}`);
    }
    lastDetailsScrape = new Date().toISOString();
    if (redis) {
      try { await saveDetailsCache(); await redis.set('lastDetailsScrape', lastDetailsScrape); }
      catch(e) { console.warn('[auto] Redis plateformes:', e.message); }
    }
    console.log(`✅ Films plateformes terminées — ${done} fiches\n`);
  } finally {
    isScraping           = false;
    scrapingPhase        = null;
    scrapeProgress       = { current: 0, total: 0, annee: '' };
    filmsDetailsProgress = { current: 0, total: 0 };
  }
}

// ── Phase 3 : liste des séries ─────────────────────────────────────────────
/**
 * Scrape la liste des séries si lastSeriesScrape > AUTO_SCRAPE_DAYS jours.
 * Déclenché au démarrage et chaque nuit à 3h30 par scheduleNightlyScraping().
 */
async function autoScrapeSeriesListIfStale() {
  if (isScrapingSeries) return;
  const ageDays = lastSeriesScrape ? (Date.now() - new Date(lastSeriesScrape).getTime()) / 86400000 : Infinity;
  if (ageDays < AUTO_SCRAPE_DAYS) {
    console.log(`⏭️  Séries liste OK (${ageDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isScrapingSeries       = true;
  scrapingPhase          = 'series-list';
  lastSeriesScrapeErrors = [];
  seriesProgress         = { current: 0, total: SERIES_PAGES };
  try {
    const label = lastSeriesScrape ? `${ageDays.toFixed(1)} jour(s)` : 'jamais';
    console.log(`\n🔄 Auto-scrape séries (liste) — dernier il y a ${label}`);
    const allSeries = [];
    let   globalPage = 0;
    for (const source of SERIES_SOURCES) {
      for (let page = 1; page <= source.pages; page++) {
        globalPage++;
        seriesProgress.current = globalPage;
        const url = `${source.baseUrl}?page=${page}`;
        try {
          let html = getCachedPage(url);
          if (!html) { const r = await fetchWithRetry(url); html = r.data; setCachedPage(url, html); }
          const raw = parseSeries(html);
          allSeries.push(...raw);
          if (raw.length === 0) break;
        } catch(e) { lastSeriesScrapeErrors.push({ source: source.label, page, message: e.message }); }
        await sleep(1500 + Math.random() * 500);
      }
    }
    const result     = dedupeAndSortSeries(allSeries);
    cachedSeries     = result;
    lastSeriesScrape = new Date().toISOString();
    if (redis) {
      try { await redis.set('series', JSON.stringify(result)); await redis.set('lastSeriesScrape', lastSeriesScrape); }
      catch(e) { console.warn('[auto] Redis séries liste:', e.message); }
    }
    console.log(`✅ Séries liste terminée — ${result.length} séries\n`);
  } finally {
    isScrapingSeries = false;
    scrapingPhase    = null;
    seriesProgress   = { current: 0, total: SERIES_PAGES };
  }
}

// ── Phase 4 : détails des séries ───────────────────────────────────────────
/**
 * Scrape les détails séries si lastSeriesDetailsScrape > AUTO_SCRAPE_DAYS jours,
 * ou si la liste a été rescrapée plus récemment que les détails.
 * Déclenché au démarrage et chaque nuit à 3h45 par scheduleNightlyScraping().
 */
async function autoScrapeSeriesDetailsIfStale() {
  if (isScrapingSeries) return;
  const ageDetDays  = lastSeriesDetailsScrape ? (Date.now() - new Date(lastSeriesDetailsScrape).getTime()) / 86400000 : Infinity;
  const ageListDays = lastSeriesScrape        ? (Date.now() - new Date(lastSeriesScrape).getTime())        / 86400000 : Infinity;
  const detStale       = ageDetDays >= AUTO_SCRAPE_DAYS;
  const listMoreRecent = ageListDays < ageDetDays;
  if (!detStale && !listMoreRecent) {
    console.log(`⏭️  Séries détails OK (${ageDetDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isScrapingSeries    = true;
  scrapingPhase       = 'series-details';
  try {
    const toFetch = cachedSeries.filter(s => s.allocineId);
    if (toFetch.length === 0) { console.log('⏭️  Séries détails : aucune série avec allocineId'); return; }
    const label = lastSeriesDetailsScrape ? `${ageDetDays.toFixed(1)}j` : 'jamais';
    console.log(`\n🔄 Auto-scrape séries (détails) — ${toFetch.length} séries — dernier il y a ${label}`);
    seriesDetailsProgress = { current: 0, total: toFetch.length };
    let done = 0;
    for (const serie of toFetch) {
      const cacheKey = `sid:${serie.allocineId}`;
      seriesDetailsCache.delete(cacheKey);
      try {
        const url  = `https://www.allocine.fr/series/ficheserie_gen_cserie=${serie.allocineId}.html`;
        const resp = await rateLimitedFetch(url);
        const html = resp.data;
        const lines = htmlToLines(html);
        let nbSaisons = null, statut = null, derniereAnnee = null, pays = null;
        for (let i = 0; i < lines.length - 1; i++) {
          const l = lines[i], n = lines[i + 1];
          if (/^Nationalités?$/i.test(l))              pays = n;
          if (/^Nationalité\s*:(.+)/i.test(l) && !pays) pays = l.replace(/^Nationalité\s*:\s*/i, '').trim();
          if (l === 'Saisons' && /^\d+$/.test(n))       nbSaisons = parseInt(n);
          if (l === 'Statut')                            statut = /en cours/i.test(n) ? 'En cours' : /termin/i.test(n) ? 'Terminée' : n;
          if (/^\d{4}\s*[-–—−]\s*(\d{4}|en cours|\.\.\.)$/i.test(l)) {
            const parts = l.split(/\s*[-–—−]\s*/);
            const yB = parts[1]?.match(/\d{4}/)?.[0];
            const yA = parts[0]?.match(/\d{4}/)?.[0];
            if (yB) derniereAnnee = yB;
            else if (yA && !derniereAnnee) derniereAnnee = yA;
          }
          if (!derniereAnnee) { const m = l.match(/^[Dd]epuis\s+(\d{4})$/); if (m) derniereAnnee = m[1]; }
          if (!derniereAnnee) { const m = l.match(/(\d{4})\s*(?:à|au|[-–—−])\s*(\d{4})/); if (m) { const y = parseInt(m[2]); if (y >= 1950 && y <= 2030) derniereAnnee = m[2]; } }
          if (!derniereAnnee && /^\d{4}$/.test(l)) { const y = parseInt(l); if (y >= 1950 && y <= 2030) derniereAnnee = l; }
        }
        const providers = extractProviders(html);
        const data = { nbSaisons, statut, derniereAnnee, pays, providers, allocineId: serie.allocineId, allocineUrl: url };
        if (nbSaisons || providers.length > 0 || pays || statut) setCachedSeriesDetails(cacheKey, data);
      } catch(e) { console.warn(`[auto] Série ${serie.allocineId}: ${e.message}`); }
      done++;
      seriesDetailsProgress.current = done;
      if (done % 20 === 0) console.log(`[auto] Séries détails: ${done}/${toFetch.length}`);
    }
    lastSeriesDetailsScrape = new Date().toISOString();
    if (redis) {
      try {
        await redis.set('series_details', JSON.stringify(Object.fromEntries(seriesDetailsCache)));
        await redis.set('lastSeriesDetailsScrape', lastSeriesDetailsScrape);
      } catch(e) { console.warn('[auto] Redis séries détails:', e.message); }
    }
    console.log(`✅ Séries détails terminés — ${done} fiches\n`);
  } finally {
    isScrapingSeries      = false;
    scrapingPhase         = null;
    seriesProgress        = { current: 0, total: SERIES_PAGES };
    seriesDetailsProgress = { current: 0, total: 0 };
  }
}


/**
 * Crée les profils famille par défaut s'ils n'existent pas encore.
 * Idempotent : peut être appelé à chaque démarrage sans risque de duplication.
 * Renomme aussi le profil migré "Mon profil" en "JC".
 */
async function seedDefaultProfiles() {
  let changed = false;

  // Renomme le profil migré depuis l'ancien format mono-utilisateur
  if (users['user_default'] && users['user_default'].name === 'Mon profil') {
    users['user_default'].name = 'JC';
    console.log('👤 Profil renommé : "Mon profil" → "JC"');
    changed = true;
  }

  const DEFAULTS = [
    { id: 'user_audrey',    name: 'Audrey'    },
    { id: 'user_josephine', name: 'Joséphine' },
    { id: 'user_augustin',  name: 'Augustin'  },
    { id: 'user_leonard',   name: 'Léonard'   },
  ];
  for (const { id, name } of DEFAULTS) {
    if (!users[id]) {
      users[id]    = { id, name, createdAt: new Date().toISOString() };
      userdata[id] = userdata[id] || {};
      console.log(`👤 Profil créé : "${name}"`);
      changed = true;
    }
  }
  if (changed) {
    await saveUsers();
    await saveUserdataFile();
  }
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 8 — SCRAPING SÉRIES : CACHE, EXTRACTION & PARSING
// ══════════════════════════════════════════════════════════════════════════════

// ── Cache des fiches séries (7 jours, sauvegardé en Redis en différé) ─────────
function getCachedSeriesDetails(key) {
  const c = seriesDetailsCache.get(key);
  if (!c) return null;
  if (Date.now() - c.cachedAt > SERIES_DETAILS_TTL_MS) { seriesDetailsCache.delete(key); return null; }
  return c.value;
}

function setCachedSeriesDetails(key, value) {
  seriesDetailsCache.set(key, { value, cachedAt: Date.now() });
  lastSeriesDetailsScrape = new Date().toISOString();
  scheduleSeriesDetailsBackup();
}

/** Debounce 8s — évite de saturer Redis en cas d'appels en rafale à /api/series/details */
let _seriesDetailsBackupTimer = null;
function scheduleSeriesDetailsBackup() {
  clearTimeout(_seriesDetailsBackupTimer);
  _seriesDetailsBackupTimer = setTimeout(saveSeriesDetailsCache, 8000);
}

async function saveSeriesDetailsCache() {
  if (!redis) return;
  try {
    const obj = {};
    seriesDetailsCache.forEach((v, k) => { obj[k] = v; });
    await redis.set('series_details', JSON.stringify(obj));
    if (lastSeriesDetailsScrape) await redis.set('lastSeriesDetailsScrape', lastSeriesDetailsScrape);
    console.log(`💾 seriesDetailsCache sauvegardé (${seriesDetailsCache.size} entrées)`);
  } catch(e) { console.warn('Erreur sauvegarde seriesDetailsCache:', e.message); }
}

/**
 * Extrait le mapping titre normalisé → allocineId depuis une page de liste de séries.
 * Les liens peuvent avoir deux formes :
 *   /series/ficheserie_gen_cserie=XXXXX.html  (ancienne URL)
 *   /series/ficheserie-XXXXX/                 (nouvelle URL)
 * @param  {string} html
 * @returns {Map<string, string>} normalizeTitle(titre) → allocineId
 */
function extractSeriesIdsFromTopPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();
  $('a[href*="ficheserie"]').each((_, el) => {
    const href    = $(el).attr('href') || '';
    const idMatch = href.match(/ficheserie[_-]gen_cserie=(\d+)/) || href.match(/ficheserie-(\d+)/);
    if (!idMatch) return;
    const texts = [$(el).text().trim(), $(el).attr('title') || '', $(el).find('img').attr('alt') || ''];
    const title = texts.find(t => t.length > 0);
    if (!title) return;
    const key = normalizeTitle(title);
    if (!key || titleToId.has(key)) return;
    titleToId.set(key, idMatch[1]);
  });
  return titleToId;
}

/**
 * Parse une page de liste des meilleures séries AlloCiné.
 *
 * Architecture de la donnée :
 *   • genre, createur, casting → UNIQUEMENT depuis la carte de liste (htmlToLines + extractCreatCast)
 *   • enCours, anneeSortie, anneeFin → depuis le texte de la carte ("Depuis XXXX" / "XXXX – XXXX")
 *   • titreOriginal → stocké mais NON affiché (évite la pollution du filtre genre)
 *   • statut, pays, nbSaisons, providers → depuis /api/series/details (fiche individuelle)
 *
 * Deux approches :
 *   1. Sélecteurs Cheerio sur les éléments article/card (plus fiable si HTML stable)
 *   2. Fallback texte : scan ligne par ligne (robuste aux changements de DOM)
 *
 * @param  {string} html
 * @returns {Array<{ titre, titreOriginal, createur, casting, genre, anneeSortie, anneeFin, enCours, notePresse, noteSpect, synopsis, allocineId, poster }>}
 */
function parseSeries(html) {
  const $         = cheerio.load(html);
  const titleToId = extractSeriesIdsFromTopPage(html);
  const series    = [];
  const seen      = new Set();

  /** Regex de détection des noms de genres connus AlloCiné (liste exhaustive) */
  const GENRE_RE = /Drame|Comédie|Action|Thriller|Aventure|Animation|Fantastique|Science-[Ff]iction|Science|Horreur|Policier|Crime|Biopic|Romance|Historique|Documentaire|Western|Mystère|Espionnage|Médical|Sport|Guerre|Musical|Téléréalité|Famille|Jeunesse|Comédie dramatique|Comédie romantique|Super-héros|Superhéros|Mini-série|Soap Opera|Reality|Talk[ -]?[Ss]how|Manga/i;

  /** Tokens qui indiquent la fin d'une section créateur ou casting lors du scan */
  const CARD_STOP = new Set(['Avec', 'De', 'Créée par', 'Créé par', 'Créateur', 'Presse', 'Spectateurs', 'Titre original']);

  /**
   * Extrait le créateur et le casting d'un tableau de lignes (carte de liste).
   * Scan forward : dès qu'une ligne "De" / "Créée par" est trouvée, collecte
   * les noms suivants jusqu'à un token CARD_STOP ou un chiffre ou un genre.
   * @param  {string[]} cardLines
   * @returns {{ createur: string, casting: string }}
   */
  function extractCreatCast(cardLines) {
    let createur = '', casting = '';
    for (let ci = 0; ci < cardLines.length; ci++) {
      const cl = cardLines[ci];
      if ((cl === 'De' || cl === 'Créée par' || cl === 'Créé par' || cl === 'Créateur') && !createur) {
        const parts = [];
        for (let cm = ci + 1; cm < Math.min(cardLines.length, ci + 6); cm++) {
          if (!cardLines[cm] || cardLines[cm] === ',') continue;
          if (CARD_STOP.has(cardLines[cm]) || /^\d/.test(cardLines[cm]) || GENRE_RE.test(cardLines[cm])) break;
          parts.push(cardLines[cm]);
        }
        if (parts.length) createur = parts.join(', ');
      }
      if (cl === 'Avec' && !casting) {
        const actors = [];
        for (let cm = ci + 1; cm < Math.min(cardLines.length, ci + 12); cm++) {
          if (!cardLines[cm] || cardLines[cm] === ',') continue;
          if (CARD_STOP.has(cardLines[cm]) || /^\d/.test(cardLines[cm])) break;
          if (actors.length < 5) actors.push(cardLines[cm]);
        }
        if (actors.length) casting = actors.join(', ');
      }
    }
    return { createur, casting };
  }

  // ── Approche 1 : sélecteurs Cheerio (plus fiable si les classes sont stables) ──
  $('article, li.card, div.card, [class*="entity-card"]').each((_, card) => {
    const $card = $(card);
    const $link = $card.find('a[href*="ficheserie"]').first();
    if (!$link.length) return;

    const href      = $link.attr('href') || '';
    const idMatch   = href.match(/ficheserie[_-]gen_cserie=(\d+)/) || href.match(/ficheserie-(\d+)/);
    const allocineId= idMatch ? idMatch[1] : null;
    const titre     = ($link.attr('title') || $link.text()).trim();
    if (!titre || seen.has(normalizeTitle(titre))) return;
    seen.add(normalizeTitle(titre));

    // Notes presse & spectateurs (sélecteur stareval-note, ordre d'apparition)
    const ratingNotes = [];
    $card.find('.stareval-note').each((_, el) => {
      const n = parseFloat($(el).text().replace(',', '.'));
      if (!isNaN(n) && n > 0 && n <= 5) ratingNotes.push(n);
    });
    const notePresse = ratingNotes[0] ?? null;
    const noteSpect  = ratingNotes[1] ?? null;
    if (!notePresse) return; // pas de note = pas une vraie carte série

    // Genre — 3 niveaux de fallback :
    //   1. Liens /genre-XXXXX dans la carte (le plus fiable)
    //   2. Éléments .meta-genre, .genre, [class*="genre"]
    //   3. Scan des lignes texte de la carte via GENRE_RE
    const genreArr = $card.find('a[href*="genre-"]')
      .map((_, el) => $(el).text().trim().replace(/^s[eé]ries?\s+/i, '').trim())
      .get().filter(v => v && v.length > 1 && v.length < 40 && /^[A-ZÀÂÄÉÈÊËÎÏÔÙÛÜŸÆŒ]/.test(v));
    let genre = [...new Set(genreArr)].join(', ')
      || $card.find('.meta-genre, .genre, [class*="genre"]').first().text().trim().replace(/^s[eé]ries?\s+/i, '').trim();
    // Lignes texte de la carte (calculé une seule fois, réutilisé pour genre/titreOriginal/createur)
    const cardLines = htmlToLines($card.html() || '');

    // Fallback genre : regex ANCRÉE (^ … $) — la ligne entière doit être un nom de genre,
    // évite les faux positifs sur titres ("Romance à Paris") ou titres originaux ("Western Union")
    if (!genre) {
      const GENRE_STRICT = /^(Comédie dramatique|Comédie romantique|Comédie|Science-Fiction|Super-héros|Superhéros|Mini-série|Soap Opera|Talk[ -]?Show|Drame|Action|Thriller|Aventure|Animation|Fantastique|Science|Horreur|Policier|Crime|Biopic|Romance|Historique|Documentaire|Western|Mystère|Espionnage|Médical|Sport|Guerre|Musical|Téléréalité|Famille|Jeunesse|Reality|Manga)$/i;
      // Exclure la ligne qui suit "Titre original" pour ne jamais la confondre avec un genre
      const origIdxG    = cardLines.indexOf('Titre original');
      const origTitleLC = origIdxG >= 0 && cardLines[origIdxG + 1] ? cardLines[origIdxG + 1].toLowerCase() : null;
      const genreFromLines = cardLines.filter(l => GENRE_STRICT.test(l) && (!origTitleLC || l.toLowerCase() !== origTitleLC));
      if (genreFromLines.length) genre = [...new Set(genreFromLines)].join(', ');
    }

    // Dates : "Depuis XXXX" → enCours=true ; "XXXX – XXXX" → range ; "XXXX" → date unique
    const allText    = $card.text();
    const depuisM    = allText.match(/[Dd]epuis\s+(\d{4})/);
    const rangeM     = !depuisM && allText.match(/(\d{4})\s*[-–—]\s*(\d{4})/);
    const singleM    = !depuisM && !rangeM && allText.match(/(\d{4})/);
    const enCours    = !!depuisM;
    const anneeSortie= depuisM ? depuisM[1] : rangeM ? rangeM[1] : singleM ? singleM[1] : null;
    const anneeFin   = rangeM ? rangeM[2] : null;

    // Titre original (stocké dans Redis, non affiché côté client)
    const origIdx       = cardLines.indexOf('Titre original');
    const titreOriginal = origIdx >= 0 && cardLines[origIdx + 1] ? cardLines[origIdx + 1] : '';

    // Créateur & casting exclusivement depuis les lignes de la carte
    const { createur, casting } = extractCreatCast(cardLines);

    const synopsis = $card.find('.synopsis-short, [class*="synopsis"]').first().text().trim();

    // Poster (préfère data-src pour le lazy-loading)
    const $img   = $card.find('img').first();
    const rawSrc = $img.attr('data-src') || $img.attr('data-lazy-src') || $img.attr('data-original') || $img.attr('src') || '';
    let poster   = rawSrc && !/blank|placeholder|gif$/i.test(rawSrc) ? rawSrc : null;
    if (poster && poster.startsWith('/')) poster = 'https://www.allocine.fr' + poster;

    series.push({ titre, titreOriginal, createur, casting, genre, anneeSortie, anneeFin, enCours, notePresse, noteSpect, synopsis, allocineId, poster });
  });

  // ── Approche 2 (fallback) : scan de lignes de texte ───────────────────────
  // Même logique que parseFilms mais adapté aux séries (pas de marqueur " VOD")
  if (series.length === 0) {
    const SKIP = new Set(['De', 'Avec', 'Titre original', 'Presse', 'Spectateurs',
                          'En cours', 'Terminée', 'Terminé', 'Diffusion', '']);
    const lines = htmlToLines(html);

    for (let i = 0; i < lines.length; i++) {
      if (!(lines[i] === 'Presse' && /^\d[,.]\d$/.test(lines[i + 1] || ''))) continue;
      const notePresse = parseFloat(lines[i + 1].replace(',', '.'));
      const noteSpect  = lines[i + 2] === 'Spectateurs' && /^\d[,.]\d$/.test(lines[i + 3] || '')
        ? parseFloat(lines[i + 3].replace(',', '.')) : null;

      let titre = '', titreOriginal = '', anneeSortie = null, anneeFin = null, enCours = false;
      const genreParts = [];
      for (let j = i - 1; j >= Math.max(0, i - 20); j--) {
        const line = lines[j];
        if (!line || SKIP.has(line)) continue;
        if (/^\d[,.]\d$/.test(line) || /^#?\d+$/.test(line)) continue;
        if (/^[Dd]epuis\s+\d{4}/.test(line)) {
          if (!anneeSortie) { const m = line.match(/\d{4}/); if (m) { anneeSortie = m[0]; enCours = true; } }
          continue;
        }
        if (/^\d{4}/.test(line) || /^Dès\s+\d{4}/.test(line)) {
          if (!anneeSortie) {
            const rangeM = line.match(/(\d{4})\s*[-–—]\s*(\d{4})/);
            if (rangeM) { anneeSortie = rangeM[1]; anneeFin = rangeM[2]; }
            else { const m = line.match(/\d{4}/); if (m) anneeSortie = m[0]; }
          }
          continue;
        }
        if (/^\d+\s+saison/.test(line)) continue;
        // Titre original AVANT le check genre : évite qu'un titre original contenant
        // un mot de genre (ex. "Romance à Paris", "Western Union") soit ajouté en genre
        if (lines[j - 1] === 'Titre original') { if (!titreOriginal) titreOriginal = line; continue; }
        if (GENRE_RE.test(line)) { genreParts.unshift(line); continue; }
        titre = line; break;
      }

      const genre = genreParts.join(', ');
      if (!titre || seen.has(normalizeTitle(titre))) continue;
      seen.add(normalizeTitle(titre));

      const { createur, casting } = extractCreatCast(lines.slice(Math.max(0, i - 30), i));
      const synStart = noteSpect !== null ? i + 4 : i + 2;
      let synopsis = '';
      for (let k = synStart; k < Math.min(lines.length, synStart + 10); k++) {
        if (lines[k] === 'Presse') break;
        if (lines[k] && lines[k].length > 80 && !/^\d/.test(lines[k])) { synopsis = lines[k]; break; }
      }

      const allocineId = titleToId.get(normalizeTitle(titre)) || null;
      series.push({ titre, titreOriginal, createur, casting, genre, anneeSortie, anneeFin, enCours, notePresse, noteSpect, synopsis, allocineId });
    }
  }

  return series;
}

/**
 * Déduplique et trie les séries par note presse décroissante.
 * La déduplication utilise la clé "titre|notePresse".
 */
function dedupeAndSortSeries(series) {
  const seen = new Set();
  return series
    .filter(s => {
      const key = `${s.titre.toLowerCase()}|${s.notePresse}`;
      if (seen.has(key)) return false;
      seen.add(key); return true;
    })
    .sort((a, b) => b.notePresse - a.notePresse || a.titre.localeCompare(b.titre, 'fr'));
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 9 — API SÉRIES
// ══════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/series
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne la liste complète des séries en cache, accompagnée des
 *        détails en cache (statut, pays, nbSaisons, providers) pour chaque série.
 *        Même logique que /api/films : tout en une seule requête pour le client.
 *
 * Réponse : {
 *   series:    Serie[]      — liste triée par note presse
 *   lastScrape: string|null — ISO date du dernier scraping liste
 *   count:     number
 *   details:   (SerieDetail|null)[] — indexé par position (même ordre que series[])
 * }
 */
app.get('/api/series', (_req, res) => {
  const details = cachedSeries.map(s => {
    const key = s.allocineId ? `sid:${s.allocineId}` : null;
    return key ? (getCachedSeriesDetails(key) || null) : null;
  });
  res.json({ series: cachedSeries, lastScrape: lastSeriesScrape, count: cachedSeries.length, details });
});

/**
 * GET /api/series/health
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne les informations de santé du module séries.
 *        Utilisé par le client pour activer les boutons de scraping et
 *        afficher les dates du dernier scraping dans la modal "Info".
 *
 * Réponse: { ok, cachedSeries, cachedDetails, lastScrape, lastDetailsScrape,
 *            isScrapingSeries, version, lastScrapeErrors }
 */
app.get('/api/series/health', (_req, res) => {
  res.json({
    ok: true,
    cachedSeries:    cachedSeries.length,
    cachedDetails:   seriesDetailsCache.size,
    lastScrape:      lastSeriesScrape,
    lastDetailsScrape: lastSeriesDetailsScrape,
    isScrapingSeries,
    version:         VERSION,
    lastScrapeErrors: lastSeriesScrapeErrors,
  });
});

/**
 * GET /api/series/scrape-status
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne l'état d'avancement du scraping séries en cours.
 *
 * Réponse: { isScraping, pct (0-100) }
 */
app.get('/api/series/scrape-status', (_req, res) => {
  const pct = seriesProgress.total
    ? Math.round(seriesProgress.current / seriesProgress.total * 100) : 0;
  res.json({ isScraping: isScrapingSeries, pct });
});

/**
 * GET /api/series/scrape
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Lance le scraping des pages de liste AlloCiné pour les séries.
 *        Parcourt toutes les sources définies dans SERIES_SOURCES :
 *          - Top AlloCiné (10 pages)
 *          - Presse par année sur la fenêtre glissante 4 ans (5 pages chacune)
 *        Répond en SSE, persiste dans Redis à la fin.
 *
 * Événements SSE :
 *   { type: 'progress', page, total, source }
 *   { type: 'series',   series[], page, total }
 *   { type: 'error',    page, message }
 *   { type: 'done',     totalSeries, lastScrape }
 *
 * Erreur : 429 si scraping déjà en cours
 */
app.get('/api/series/scrape', requireSecret, requireRateLimit(5, 10 * 60 * 1000), async (req, res) => {
  if (isScrapingSeries) return res.status(429).json({ error: 'Scraping déjà en cours' });
  isScrapingSeries      = true;
  lastSeriesScrapeErrors = [];
  seriesProgress         = { current: 0, total: SERIES_PAGES };

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send      = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  const allSeries = [];
  let   globalPage = 0;

  for (const source of SERIES_SOURCES) {
    console.log(`[series] Source: ${source.label}`);
    for (let page = 1; page <= source.pages; page++) {
      globalPage++;
      seriesProgress.current = globalPage;
      send({ type: 'progress', page: globalPage, total: SERIES_PAGES, source: source.label });
      const url = `${source.baseUrl}?page=${page}`;
      try {
        let html = getCachedPage(url);
        if (!html) { const r = await fetchWithRetry(url); html = r.data; setCachedPage(url, html); }
        const raw = parseSeries(html);
        allSeries.push(...raw);
        send({ type: 'series', series: raw, page: globalPage, total: SERIES_PAGES });
        console.log(`[series] ${source.label} page ${page}/${source.pages} → ${raw.length} séries`);
        if (raw.length === 0) { console.log(`[series] Page vide — passage à la source suivante`); break; }
      } catch(e) {
        const msg = e.response ? `HTTP ${e.response.status}` : e.message;
        console.error(`[series] ${source.label} page ${page} erreur: ${msg}`);
        lastSeriesScrapeErrors.push({ source: source.label, page, message: msg });
        send({ type: 'error', page: globalPage, message: msg });
      }
      await sleep(1500 + Math.random() * 500);
    }
  }

  const result      = dedupeAndSortSeries(allSeries);
  cachedSeries      = result;
  lastSeriesScrape  = new Date().toISOString();
  if (redis) {
    try {
      await redis.set('series', JSON.stringify(result));
      await redis.set('lastSeriesScrape', lastSeriesScrape);
    } catch(e) { console.warn('Erreur sauvegarde series Redis:', e.message); }
  }
  send({ type: 'done', totalSeries: result.length, lastScrape: lastSeriesScrape });
  res.end();
  isScrapingSeries = false;
  console.log(`✅ ${result.length} séries scrapées`);
});

/**
 * GET /api/series/details?seriesId=XXXXX
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Récupère les données complémentaires d'une série depuis sa fiche AlloCiné.
 *
 * Champs extraits et stockés :
 *   • statut       — "En cours" | "Terminée"
 *   • pays         — nationalité de production
 *   • nbSaisons    — nombre de saisons
 *   • derniereAnnee— dernière année connue (complète la plage de dates)
 *   • providers    — plateformes streaming avec leur type (svod/location/achat)
 *
 * Champs EXCLUS intentionnellement (source = scraping de liste uniquement) :
 *   ✗ genre    → s.genre    (parseSeries, page de liste)
 *   ✗ createur → s.createur (parseSeries, page de liste)
 *   ✗ casting  → s.casting  (parseSeries, page de liste)
 *
 * Le résultat est mis en cache 7 jours (seriesDetailsCache + Redis).
 *
 * Params (query) :
 *   seriesId : ID numérique AlloCiné de la série (obligatoire)
 *
 * Réponse: { nbSaisons, statut, derniereAnnee, pays, providers[], allocineId, allocineUrl, error? }
 * Erreur : 400 si seriesId manquant
 */
app.get('/api/series/details', async (req, res) => {
  const seriesId = String(req.query.seriesId || '').trim();
  if (!seriesId) return res.status(400).json({ error: 'seriesId requis' });

  const cacheKey = `sid:${seriesId}`;
  const cached   = getCachedSeriesDetails(cacheKey);
  if (cached) { console.log(`Cache série: ${seriesId}`); return res.json(cached); }

  try {
    const url   = `https://www.allocine.fr/series/ficheserie_gen_cserie=${seriesId}.html`;
    const resp  = await rateLimitedFetch(url);
    const html  = resp.data;
    const lines = htmlToLines(html);

    let nbSaisons = null, statut = null, derniereAnnee = null, pays = null;

    for (let i = 0; i < lines.length - 1; i++) {
      const l = lines[i], n = lines[i + 1];

      if (/^Nationalités?$/i.test(l))              pays = n;
      if (/^Nationalité\s*:(.+)/i.test(l) && !pays) pays = l.replace(/^Nationalité\s*:\s*/i, '').trim();
      if (l === 'Saisons' && /^\d+$/.test(n))       nbSaisons = parseInt(n);
      if (l === 'Statut')                            statut = /en cours/i.test(n) ? 'En cours' : /termin/i.test(n) ? 'Terminée' : n;

      // Plage d'années — ex: "2008 - 2013", "2019 - en cours", "2020 − 2023"
      if (/^\d{4}\s*[-–—−]\s*(\d{4}|en cours|\.\.\.)$/i.test(l)) {
        const parts = l.split(/\s*[-–—−]\s*/);
        const yB = parts[1]?.match(/\d{4}/)?.[0];
        const yA = parts[0]?.match(/\d{4}/)?.[0];
        if (yB) derniereAnnee = yB;
        else if (yA && !derniereAnnee) derniereAnnee = yA;
      }
      // "Depuis 2020"
      if (!derniereAnnee) {
        const m = l.match(/^[Dd]epuis\s+(\d{4})$/);
        if (m) derniereAnnee = m[1];
      }
      // Plage dans du texte : "Série de 2020 à 2023"
      if (!derniereAnnee) {
        const m = l.match(/(\d{4})\s*(?:à|au|[-–—−])\s*(\d{4})/);
        if (m) { const y = parseInt(m[2]); if (y >= 1950 && y <= 2030) derniereAnnee = m[2]; }
      }
      // Année isolée : "2020"
      if (!derniereAnnee && /^\d{4}$/.test(l)) {
        const y = parseInt(l); if (y >= 1950 && y <= 2030) derniereAnnee = l;
      }
    }

    const providers = extractProviders(html);
    const data = { nbSaisons, statut, derniereAnnee, pays, providers, allocineId: seriesId, allocineUrl: url };

    // Met en cache uniquement si au moins une donnée utile a été trouvée
    if (nbSaisons || providers.length > 0 || pays || statut)
      setCachedSeriesDetails(cacheKey, data);

    const pNames = providers.map(p => `${p.name}(${p.type})`).join(', ') || '—';
    console.log(`Série ${seriesId} → ${pays || '?'} statut:${statut || '?'} saisons:${nbSaisons ?? '?'} | ${pNames}`);
    return res.json(data);

  } catch(e) {
    const status = e.response?.status;
    return res.json({
      nbSaisons: null, statut: null, derniereAnnee: null, pays: null,
      providers: [], allocineId: seriesId,
      error: status === 429 ? 'rate_limited' : e.message,
    });
  }
});

/**
 * POST /api/series/clear-cache
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vide le cache mémoire + Redis des fiches séries (statut, pays, plateformes…).
 *        Utile pour forcer un re-fetch des fiches après un soft-block ou une
 *        mise à jour des informations sur AlloCiné.
 *
 * Réponse: { ok: true, cleared: number }
 */
app.post('/api/series/clear-cache', requireSecret, async (_req, res) => {
  const count = seriesDetailsCache.size;
  seriesDetailsCache.clear();
  if (redis) {
    try { await redis.del('series_details'); }
    catch(e) { console.warn('Redis del series_details:', e.message); }
  }
  console.log(`🗑️  Cache détails séries vidé (${count} entrées)`);
  res.json({ ok: true, cleared: count });
});

/**
 * POST /api/series/clear-list
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vide le cache mémoire + Redis de la liste des séries.
 *        Force un nouveau scraping complet de la liste au prochain appel client.
 *
 * Réponse: { ok: true, cleared: number }
 */
app.post('/api/series/clear-list', requireSecret, async (_req, res) => {
  const count  = cachedSeries.length;
  cachedSeries = [];
  lastSeriesScrape = null;
  if (redis) {
    try { await redis.del('series'); await redis.del('lastSeriesScrape'); }
    catch(e) { console.warn('Redis del series:', e.message); }
  }
  console.log(`🗑️  Cache liste séries vidé (${count} entrées)`);
  res.json({ ok: true, cleared: count });
});

/**
 * POST /api/series/clear-all
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vide à la fois le cache de la liste et le cache des fiches séries.
 *        Équivalent à clear-list + clear-cache en une seule requête.
 *
 * Réponse: { ok: true, clearedList: number, clearedDetails: number }
 */
app.post('/api/series/clear-all', requireSecret, async (_req, res) => {
  const detCount  = seriesDetailsCache.size;
  const listCount = cachedSeries.length;
  seriesDetailsCache.clear();
  cachedSeries             = [];
  lastSeriesScrape         = null;
  lastSeriesDetailsScrape  = null;
  if (redis) {
    try { await redis.del('series_details'); } catch(e) {}
    try { await redis.del('series'); await redis.del('lastSeriesScrape'); } catch(e) {}
  }
  console.log(`🗑️  Cache complet séries vidé (liste: ${listCount}, détails: ${detCount})`);
  res.json({ ok: true, clearedList: listCount, clearedDetails: detCount });
});


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 10 — API DE DEBUG (diagnostic & vérification)
// ══════════════════════════════════════════════════════════════════════════════

/**
 * GET /api/series/debug-genres
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Statistiques de couverture des genres dans la liste scrapée.
 *        Source unique : s.genre depuis le scraping de liste (parseSeries).
 *
 * Réponse: { list: { total, withGenre, withoutGenre, sample[] } }
 */
app.get('/api/series/debug-genres', requireSecret, (_req, res) => {
  const listWith    = cachedSeries.filter(s => s.genre).length;
  const listWithout = cachedSeries.length - listWith;
  const listSample  = cachedSeries.slice(0, 10).map(s => ({ titre: s.titre, genre: s.genre || null }));
  res.json({
    list: { total: cachedSeries.length, withGenre: listWith, withoutGenre: listWithout, sample: listSample },
  });
});

/**
 * GET /api/series/providers
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Statistiques des plateformes de streaming présentes dans le cache des fiches.
 *        Utile pour vérifier quelles plateformes sont détectées et leur fréquence.
 *
 * Réponse: { providers: { name, type, count }[], totalDetails: number }
 */
app.get('/api/series/providers', (_req, res) => {
  const counts = {};
  for (const [, det] of seriesDetailsCache) {
    (det.providers || []).forEach(p => {
      if (!counts[p.name]) counts[p.name] = { name: p.name, type: p.type, count: 0 };
      counts[p.name].count++;
    });
  }
  const list = Object.values(counts).sort((a, b) => b.count - a.count);
  res.json({ providers: list, totalDetails: seriesDetailsCache.size });
});

/**
 * GET /api/series/debug-posters
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Vérifie l'extraction des posters depuis le scraping de liste.
 *        Affiche les 20 premières séries avec ou sans poster.
 *
 * Réponse: { total, withPoster, sample: { titre, poster }[] }
 */
app.get('/api/series/debug-posters', requireSecret, (_req, res) => {
  const sample = cachedSeries.slice(0, 20).map(s => ({ titre: s.titre, poster: s.poster || null }));
  res.json({
    total:      cachedSeries.length,
    withPoster: cachedSeries.filter(s => s.poster).length,
    sample,
  });
});

/**
 * GET /api/series/debug-lines?seriesId=XXXXX
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Télécharge une fiche série et affiche les lignes de texte extraites
 *        par htmlToLines(). Permet de déboguer l'extraction de données
 *        (statut, pays, années, genres, créateurs…) depuis une fiche spécifique.
 *
 * Params (query) :
 *   seriesId : ID numérique AlloCiné de la série (obligatoire)
 *
 * Réponse: { seriesId, url, relevant: string[], total: number }
 * Erreur : 400 si seriesId manquant, 500 si erreur réseau
 */
app.get('/api/series/debug-lines', requireSecret, async (req, res) => {
  const seriesId = String(req.query.seriesId || '').trim();
  if (!seriesId) return res.status(400).json({ error: 'seriesId requis' });
  try {
    const url      = `https://www.allocine.fr/series/ficheserie_gen_cserie=${seriesId}.html`;
    const resp     = await rateLimitedFetch(url);
    const lines    = htmlToLines(resp.data);
    // Filtre les 300 premières lignes pouvant contenir des métadonnées utiles
    const relevant = lines.slice(0, 300).filter(l =>
      /\d{4}|saison|statut|nationalit|créa|avec|depuis|en cours/i.test(l) || l.length < 60
    );
    res.json({ seriesId, url, relevant, total: lines.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 12 — BESTEVER : MEILLEURS FILMS DE TOUS LES TEMPS
//
//  URL : allocine.fr/film/meilleurs/presse/decennie-{DECADE}/?page={N}
//  Décennies : 1940 à 2020 (9 décennies × BESTEVER_PAGES_PER_DECADE pages)
//  Clés Redis : bestever, lastBesteverScrape, lastBesteverDetailsScrape
//  Plateformes : réutilise detailsCache (même infrastructure que VOD films)
// ══════════════════════════════════════════════════════════════════════════════

/**
 * Parse une page "Meilleurs films" AlloCiné (décennie) via Cheerio DOM.
 * Structure spécifique à ces pages (différente des pages VOD) :
 *   • a.meta-title-link → titre + allocineId
 *   • img.thumbnail-img → poster
 *   • .rating-item contenant "Presse"/"Spectateurs" → notes
 *   • .meta-body-info .dark-grey-link → genres
 *   • .meta-body-direction → réalisateur
 *   • .meta-body-actor → acteurs
 *   • .content-txt → synopsis
 *   • span.light contenant "Titre original" → titreOriginal
 *
 * @param  {string} html
 * @param  {string} decade  ex: "2020"
 * @returns {Array<{ titre, titreOriginal, genre, realisateur, acteurs, notePresse, noteSpect, synopsis, allocineId, poster, decade }>}
 */
function parseBesteverFilms(html, decade) {
  const $ = cheerio.load(html);
  const films = [];
  const seen  = new Set();

  $('a.meta-title-link').each((_, el) => {
    const $el     = $(el);
    const href    = $el.attr('href') || '';
    const idMatch = href.match(/fichefilm_gen_cfilm=(\d+)/);
    if (!idMatch) return;

    const allocineId = idMatch[1];
    const titre      = $el.text().trim();
    if (!titre || seen.has(allocineId)) return;
    seen.add(allocineId);

    // Remonte vers le container de la carte (max 10 niveaux)
    let $card = $el.parent();
    for (let i = 0; i < 10; i++) {
      const cls = $card.attr('class') || '';
      if (/card|entity|item|result|content/i.test(cls) || $card.is('article, li')) break;
      const $parent = $card.parent();
      if ($parent.is('body, html') || !$parent.length) break;
      $card = $parent;
    }

    // Poster
    const $imgThumb = $card.find('img.thumbnail-img').first();
    const $imgAny   = $card.find('img').first();
    const $img      = $imgThumb.length ? $imgThumb : $imgAny;
    const rawSrc    = $img.attr('data-src') || $img.attr('data-lazy-src') || $img.attr('data-original') || $img.attr('src') || '';
    const poster    = rawSrc && !/blank|placeholder|gif$/i.test(rawSrc) ? rawSrc : null;

    // Notes presse & spectateurs
    let notePresse = null, noteSpect = null;
    $card.find('.rating-item, [class*="rating-item"]').each((_, ri) => {
      const $ri  = $(ri);
      const txt  = $ri.text();
      const note = parseFloat($ri.find('.stareval-note, [class*="stareval-note"]').first().text().replace(',', '.'));
      if (isNaN(note) || note <= 0 || note > 5) return;
      if (/presse/i.test(txt) && notePresse === null)         notePresse = note;
      else if (/spectateur/i.test(txt) && noteSpect === null) noteSpect  = note;
    });

    // Genres
    const genreArr = [];
    $card.find('.meta-body-info .dark-grey-link, .meta-body-info a').each((_, a) => {
      const t = $(a).text().trim();
      if (t && t.length < 50 && !genreArr.includes(t)) genreArr.push(t);
    });
    const genre = genreArr.join(', ');

    // Réalisateur
    let realisateur = '';
    $card.find('.meta-body-direction, [class*="meta-body-direction"]').each((_, d) => {
      if (realisateur) return;
      const names = $(d).find('.dark-grey-link, a').map((_, a) => $(a).text().trim()).get().filter(Boolean);
      if (names.length) realisateur = names.join(', ');
    });

    // Acteurs
    let acteurs = '';
    $card.find('.meta-body-actor, [class*="meta-body-actor"]').each((_, a) => {
      if (acteurs) return;
      const names = $(a).find('.dark-grey-link, a').map((_, al) => $(al).text().trim()).get().filter(Boolean);
      if (names.length) acteurs = names.slice(0, 5).join(', ');
    });

    // Titre original
    let titreOriginal = '';
    $card.find('span.light, .meta-body-item span.light').each((_, span) => {
      if (/titre original/i.test($(span).text())) {
        titreOriginal = $(span).siblings('.dark-grey').first().text().trim()
          || $(span).next('.dark-grey, span').text().trim()
          || $(span).parent().find('.dark-grey').text().trim();
      }
    });

    // Synopsis
    const synopsis = $card.find('.content-txt').first().text().trim().substring(0, 400);

    // Année de sortie (text brut dans .meta-body-info, hors liens)
    let anneeSortie = null;
    const $metaInfo = $card.find('.meta-body-info').first();
    if ($metaInfo.length) {
      const metaText = $metaInfo.text();
      const yearMatch = metaText.match(/\b(19\d{2}|20\d{2})\b/);
      if (yearMatch) anneeSortie = yearMatch[1];
    }

    films.push({ titre, titreOriginal, genre, realisateur, acteurs, notePresse, noteSpect, synopsis, allocineId, poster, decade: String(decade), anneeSortie });
  });

  return films;
}

/**
 * Déduplique et trie les bestever films.
 * Ordre : décennie décroissante, puis note presse décroissante.
 * La déduplication utilise l'allocineId (ou titre|notePresse si pas d'ID).
 */
function dedupeAndSortBestever(films) {
  const seen = new Set();
  return films
    .filter(f => {
      const key = f.allocineId || `${f.titre.toLowerCase()}|${f.notePresse}`;
      if (seen.has(key)) return false;
      seen.add(key); return true;
    })
    .sort((a, b) =>
      Number(b.decade) - Number(a.decade) ||
      (b.notePresse ?? 0) - (a.notePresse ?? 0) ||
      a.titre.localeCompare(b.titre, 'fr')
    );
}


// ── API BESTEVER ────────────────────────────────────────────────────────────

/**
 * GET /api/bestever
 * ─────────────────────────────────────────────────────────────────────────────
 * Rôle : Retourne la liste complète des meilleurs films all-time en cache,
 *        accompagnée des détails plateformes (réutilise detailsCache films VOD).
 *
 * Réponse : { films, lastScrape, count, details }
 */
app.get('/api/bestever', (_req, res) => {
  const details = cachedBestever.map(film => {
    const key = film.allocineId ? `id:${film.allocineId}` : `q:${film.titre}`;
    return getCachedDetails(key) || null;
  });
  res.json({ films: cachedBestever, lastScrape: lastBesteverScrape, count: cachedBestever.length, details });
});

/**
 * GET /api/bestever/health
 * ─────────────────────────────────────────────────────────────────────────────
 * Réponse : { ok, cachedFilms, lastScrape, lastDetailsScrape, isScraping, pagesPerDecade, decades }
 */
app.get('/api/bestever/health', (_req, res) => {
  res.json({
    ok: true,
    cachedFilms:       cachedBestever.length,
    lastScrape:        lastBesteverScrape,
    lastDetailsScrape: lastBesteverDetailsScrape,
    isScraping:        isBesteverScraping,
    pagesPerDecade:    BESTEVER_PAGES_PER_DECADE,
    decades:           BESTEVER_DECADES,
    version:           VERSION,
  });
});

/**
 * GET /api/bestever/scrape-status
 * Réponse : { isScraping, pct }
 */
app.get('/api/bestever/scrape-status', (_req, res) => {
  const pct = besteverProgress.total
    ? Math.round(besteverProgress.current / besteverProgress.total * 100) : 0;
  res.json({ isScraping: isBesteverScraping, pct, current: besteverProgress.current, total: besteverProgress.total });
});

/**
 * GET /api/bestever/scrape
 * ─────────────────────────────────────────────────────────────────────────────
 * Lance le scraping des pages meilleurs films en SSE.
 * Événements : progress, films, error, done
 */
app.get('/api/bestever/scrape', requireSecret, requireRateLimit(3, 10 * 60 * 1000), async (req, res) => {
  if (isBesteverScraping) return res.status(429).json({ error: 'Scraping bestever déjà en cours' });
  isBesteverScraping = true;
  scrapingPhase      = 'bestever-list';

  const pagesParam = parseInt(req.query.pages);
  const pagesPerDecade = (Number.isFinite(pagesParam) && pagesParam >= 1 && pagesParam <= 20)
    ? pagesParam
    : BESTEVER_PAGES_PER_DECADE;

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send       = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  const allFilms   = [];
  const totalPages = BESTEVER_DECADES.length * pagesPerDecade;
  let   globalPage = 0;

  try {
    for (const decade of BESTEVER_DECADES) {
      for (let page = 1; page <= pagesPerDecade; page++) {
        globalPage++;
        besteverProgress = { current: globalPage, total: totalPages };
        send({ type: 'progress', page: globalPage, total: totalPages, decade });
        const url = `https://www.allocine.fr/film/meilleurs/presse/decennie-${decade}/?page=${page}`;
        try {
          let html = getCachedPage(url);
          if (!html) { const r = await fetchWithRetry(url); html = r.data; setCachedPage(url, html); }
          const raw = parseBesteverFilms(html, String(decade));
          allFilms.push(...raw);
          send({ type: 'films', films: raw, page: globalPage, total: totalPages, decade });
          console.log(`[bestever][${decade}] Page ${page}/${pagesPerDecade} → ${raw.length} films`);
        } catch(e) {
          const msg = e.response ? `HTTP ${e.response.status}` : e.message;
          console.error(`[bestever][${decade}] Page ${page} erreur: ${msg}`);
          send({ type: 'error', page: globalPage, message: msg });
        }
        await sleep(1500 + Math.random() * 500);
      }
    }
    const result   = dedupeAndSortBestever(allFilms);
    cachedBestever = result;
    lastBesteverScrape = new Date().toISOString();
    if (redis) {
      try { await redis.set('bestever', JSON.stringify(result)); await redis.set('lastBesteverScrape', lastBesteverScrape); }
      catch(e) { console.warn('[bestever] Redis scrape:', e.message); }
    }
    send({ type: 'done', totalFilms: result.length, lastScrape: lastBesteverScrape });
    res.end();
    console.log(`✅ Bestever : ${result.length} films`);
  } finally {
    isBesteverScraping = false;
    scrapingPhase      = null;
    besteverProgress   = { current: 0, total: 0 };
  }
});

/**
 * POST /api/bestever/clear
 * Vide le cache liste bestever (Redis + mémoire).
 */
app.post('/api/bestever/clear', requireSecret, async (_req, res) => {
  const count    = cachedBestever.length;
  cachedBestever = [];
  lastBesteverScrape = null;
  if (redis) {
    try { await redis.del('bestever'); await redis.del('lastBesteverScrape'); }
    catch(e) { console.warn('Redis del bestever:', e.message); }
  }
  console.log(`🗑️  Cache bestever vidé (${count} entrées)`);
  res.json({ ok: true, cleared: count });
});


// ── Phase 5 : liste bestever ────────────────────────────────────────────────
/**
 * Scrape la liste des meilleurs films all-time si lastBesteverScrape > AUTO_SCRAPE_DAYS jours.
 * Déclenché au démarrage et chaque nuit à 4h00 par scheduleNightlyScraping().
 */
async function autoScrapeBesteverListIfStale() {
  if (isBesteverScraping) return;
  const ageDays = lastBesteverScrape ? (Date.now() - new Date(lastBesteverScrape).getTime()) / 86400000 : Infinity;
  if (ageDays < AUTO_SCRAPE_DAYS) {
    console.log(`⏭️  Bestever liste OK (${ageDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isBesteverScraping = true;
  scrapingPhase      = 'bestever-list';
  try {
    const label      = lastBesteverScrape ? `${ageDays.toFixed(1)} jour(s)` : 'jamais';
    console.log(`\n🔄 Auto-scrape bestever (liste) — dernier il y a ${label}`);
    const allFilms   = [];
    const totalPages = BESTEVER_DECADES.length * BESTEVER_PAGES_PER_DECADE;
    let   globalPage = 0;
    besteverProgress = { current: 0, total: totalPages };
    for (const decade of BESTEVER_DECADES) {
      for (let page = 1; page <= BESTEVER_PAGES_PER_DECADE; page++) {
        globalPage++;
        besteverProgress.current = globalPage;
        const url = `https://www.allocine.fr/film/meilleurs/presse/decennie-${decade}/?page=${page}`;
        try {
          let html = getCachedPage(url);
          if (!html) { const r = await fetchWithRetry(url); html = r.data; setCachedPage(url, html); }
          const raw = parseBesteverFilms(html, String(decade));
          allFilms.push(...raw);
          console.log(`[auto][bestever][${decade}] Page ${page}/${BESTEVER_PAGES_PER_DECADE} → ${raw.length} films`);
        } catch(e) {
          console.warn(`[auto][bestever][${decade}] Page ${page} erreur: ${e.message}`);
        }
        await sleep(1500 + Math.random() * 500);
      }
    }
    const result       = dedupeAndSortBestever(allFilms);
    cachedBestever     = result;
    lastBesteverScrape = new Date().toISOString();
    if (redis) {
      try {
        await redis.set('bestever', JSON.stringify(result));
        await redis.set('lastBesteverScrape', lastBesteverScrape);
      } catch(e) { console.warn('[auto][bestever] Redis liste:', e.message); }
    }
    console.log(`✅ Bestever liste terminée — ${result.length} films\n`);
  } finally {
    isBesteverScraping = false;
    scrapingPhase      = null;
    besteverProgress   = { current: 0, total: 0 };
  }
}

// ── Phase 6 : plateformes bestever ─────────────────────────────────────────
/**
 * Scrape les plateformes des meilleurs films si lastBesteverDetailsScrape > AUTO_SCRAPE_DAYS jours.
 * Réutilise detailsCache (partagé avec VOD films) — évite les doublons.
 * Déclenché au démarrage et chaque nuit à 4h15 par scheduleNightlyScraping().
 */
async function autoScrapeBesteverDetailsIfStale() {
  if (isBesteverScraping) return;
  const ageDetDays  = lastBesteverDetailsScrape ? (Date.now() - new Date(lastBesteverDetailsScrape).getTime()) / 86400000 : Infinity;
  const ageListDays = lastBesteverScrape         ? (Date.now() - new Date(lastBesteverScrape).getTime())         / 86400000 : Infinity;
  const detStale       = ageDetDays >= AUTO_SCRAPE_DAYS;
  const listMoreRecent = ageListDays < ageDetDays;
  if (!detStale && !listMoreRecent) {
    console.log(`⏭️  Bestever plateformes OK (${ageDetDays.toFixed(1)}j — seuil ${AUTO_SCRAPE_DAYS}j)`); return;
  }
  isBesteverScraping = true;
  scrapingPhase      = 'bestever-details';
  try {
    const toFetch = cachedBestever.filter(f => f.allocineId);
    if (toFetch.length === 0) { console.log('⏭️  Bestever plateformes : aucun film avec allocineId'); return; }
    const label = lastBesteverDetailsScrape ? `${ageDetDays.toFixed(1)}j` : 'jamais';
    console.log(`\n🔄 Auto-scrape bestever (plateformes) — ${toFetch.length} films — dernier il y a ${label}`);
    besteverDetailsProgress = { current: 0, total: toFetch.length };
    let done = 0;
    for (const film of toFetch) {
      const cacheKey = `id:${film.allocineId}`;
      // Utilise le cache existant si valide (partage avec VOD films)
      const existing = getCachedDetails(cacheKey);
      if (existing) {
        done++;
        besteverDetailsProgress.current = done;
        continue; // déjà en cache (scraping VOD ou précédent bestever)
      }
      try {
        const filmUrl  = `https://www.allocine.fr/film/fichefilm_gen_cfilm=${film.allocineId}.html`;
        const filmResp = await rateLimitedFetch(filmUrl);
        const html     = filmResp.data;
        const isValid  = /fichefilm_gen_cfilm|provider-tile|Titre original|Année de production/i.test(html);
        if (!isValid) {
          console.warn(`[auto][bestever] Film ${film.allocineId} → page suspecte, ignorée`);
        } else {
          const lines = htmlToLines(html);
          let pays = null, annee = null;
          for (let i = 0; i < lines.length - 1; i++) {
            if (lines[i] === 'Nationalité' || lines[i] === 'Nationalités') pays = lines[i + 1];
            if (lines[i] === 'Année de production') annee = lines[i + 1];
            if (pays && annee) break;
          }
          const providers = extractProviders(html);
          const data = { pays, annee, allocineId: film.allocineId, allocineUrl: filmUrl, providers };
          if (pays || annee || providers.length > 0) setCachedDetails(cacheKey, data);
        }
      } catch(e) { console.warn(`[auto][bestever] Film ${film.allocineId}: ${e.message}`); }
      done++;
      besteverDetailsProgress.current = done;
      if (done % 50 === 0) console.log(`[auto][bestever] Plateformes: ${done}/${toFetch.length}`);
    }
    lastBesteverDetailsScrape = new Date().toISOString();
    if (redis) {
      try {
        await saveDetailsCache(); // sauvegarde detailsCache partagé
        await redis.set('lastBesteverDetailsScrape', lastBesteverDetailsScrape);
      } catch(e) { console.warn('[auto][bestever] Redis détails:', e.message); }
    }
    console.log(`✅ Bestever plateformes terminées — ${done} fiches\n`);
  } finally {
    isBesteverScraping      = false;
    scrapingPhase           = null;
    besteverDetailsProgress = { current: 0, total: 0 };
  }
}


// ══════════════════════════════════════════════════════════════════════════════
//  SECTION 11 — DÉMARRAGE DU SERVEUR
// ══════════════════════════════════════════════════════════════════════════════

app.listen(PORT, () => {
  console.log('\n🎬 AlloCiné VOD Scraper v9 démarré !');
  console.log(`   ➜ Application    : http://localhost:${PORT}`);
  console.log(`   ➜ Santé films    : http://localhost:${PORT}/api/health`);
  console.log(`   ➜ Santé séries   : http://localhost:${PORT}/api/series/health`);
  console.log(`   ➜ Santé bestever : http://localhost:${PORT}/api/bestever/health\n`);

  // Séquence d'initialisation :
  //   1. Charge Redis (films, séries, bestever, utilisateurs, prefs, caches)
  //   2. Crée les profils par défaut s'ils n'existent pas
  //   3–8. Lance les 6 auto-scrapes si le cache est périmé
  //   9. Programme le scraping nocturne quotidien (3h00–4h15, heure Paris)
  loadUserdata()
    .then(() => seedDefaultProfiles())
    .then(() => autoScrapeFilmsListIfStale())
    .then(() => autoScrapeFilmsDetailsIfStale())
    .then(() => autoScrapeSeriesListIfStale())
    .then(() => autoScrapeSeriesDetailsIfStale())
    .then(() => autoScrapeBesteverListIfStale())
    .then(() => autoScrapeBesteverDetailsIfStale())
    .then(() => scheduleNightlyScraping());
});
