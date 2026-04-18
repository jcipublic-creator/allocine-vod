const express = require('express');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const cheerio = require('cheerio');
const { Redis } = require('@upstash/redis');

const app = express();
const PORT = Number(process.env.PORT || 3009);
const TOTAL_PAGES = Number(process.env.TOTAL_PAGES || 25);
const DETAILS_TTL_MS    = 1000 * 60 * 60 * 24; // 24h
const AUTO_SCRAPE_DAYS  = 3;                   // re-scraper si cache > 3 jours
let   isScraping        = false;
let   scrapeProgress    = { current: 0, total: 0, annee: '' };
const BUILD = (() => { try { return require('./version.json').build; } catch(e) { return 0; } })();
const VERSION = `v9.2.${BUILD}`;
const SERVER_START = new Date().toISOString();
const DATA_DIR = process.env.DATA_DIR || __dirname;
let lastScrapeErrors = [];

// ── Séries ────────────────────────────────────────────────────────────────────
const SERIES_SOURCES = [
  { label: 'Top AlloCiné',       baseUrl: 'https://www.allocine.fr/series/top/',                       pages: 10 },
  { label: 'Meilleures 2020s',   baseUrl: 'https://www.allocine.fr/series/meilleures/decennie-2020/', pages: 5  },
];
const SERIES_PAGES           = SERIES_SOURCES.reduce((sum, s) => sum + s.pages, 0);
const SERIES_DETAILS_TTL_MS  = 1000 * 60 * 60 * 24 * 7; // 7 jours
let cachedSeries             = [];
let lastSeriesScrape         = null;
let isScrapingSeries         = false;
let seriesProgress           = { current: 0, total: 0 };
let lastSeriesScrapeErrors   = [];
const seriesDetailsCache     = new Map();

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());

// ─────────────────────────────────────────────────────────────────
//  Base de données utilisateur (vu / vouloir / nonInteresse)
//  Stockée dans Upstash Redis si dispo, sinon fichier local
// ─────────────────────────────────────────────────────────────────
const redis = process.env.UPSTASH_REDIS_REST_URL
  ? new Redis({
      url:   process.env.UPSTASH_REDIS_REST_URL,
      token: process.env.UPSTASH_REDIS_REST_TOKEN,
    })
  : null;

// Cache mémoire local (évite trop d'appels Redis)
let users    = {};  // { userId: { id, name, createdAt } }
let userdata = {};  // { userId: { allocineId: { vu, vouloir, nonInteresse } } }
let prefsDB  = {};  // { userId: { showDocumentaires, showAnimations, hideVus, hideNonInteresse } }
let lastScrape = null; // timestamp ISO du dernier scraping
let cachedFilms = [];  // derniers films scrapés, persistés dans Redis

// Détecte si userdata est dans l'ancien format plat { allocineId: { vu,... } }
// (avant multi-utilisateurs) — les valeurs ont directement un champ booléen `vu`
function isOldUserdataFormat(data) {
  if (!data || typeof data !== 'object') return false;
  return Object.values(data).some(v => v && typeof v === 'object' && typeof v.vu === 'boolean');
}

// Migre l'ancien format vers le nouveau sous un profil "Mon profil"
function migrateUserdata(old) {
  const defaultId = 'user_default';
  users[defaultId] = users[defaultId] || { id: defaultId, name: 'Mon profil', createdAt: new Date().toISOString() };
  userdata = { [defaultId]: old };
  console.log(`🔄 Migration userdata → format multi-utilisateurs (${Object.keys(old).length} entrées sous "${users[defaultId].name}")`);
}

async function loadUserdata() {
  // 1. Essayer Redis en priorité
  if (redis) {
    try {
      // Charger la liste des profils
      const usersRaw = await redis.get('users');
      if (usersRaw) {
        users = typeof usersRaw === 'string' ? JSON.parse(usersRaw) : usersRaw;
        console.log(`👤 Profils Redis chargés (${Object.keys(users).length} profils)`);
      }
      // Charger les données utilisateur
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
      const prefsRaw = await redis.get('prefs');
      if (prefsRaw) {
        prefsDB = typeof prefsRaw === 'string' ? JSON.parse(prefsRaw) : prefsRaw;
        console.log(`🔖 Prefs Redis chargées (${Object.keys(prefsDB).length} profils)`);
      }
      const ts = await redis.get('lastScrape');
      if (ts) lastScrape = ts;
      const films = await redis.get('films');
      if (films) {
        cachedFilms = typeof films === 'string' ? JSON.parse(films) : films;
        console.log(`🎬 Films Redis chargés (${cachedFilms.length} films)`);
      }
      const details = await redis.get('details');
      if (details) {
        const obj = typeof details === 'string' ? JSON.parse(details) : details;
        Object.entries(obj).forEach(([k, v]) => detailsCache.set(k, v));
        console.log(`📋 détailsCache Redis chargé (${detailsCache.size} entrées)`);
      }
      // Séries
      const seriesRaw = await redis.get('series');
      if (seriesRaw) {
        cachedSeries = typeof seriesRaw === 'string' ? JSON.parse(seriesRaw) : seriesRaw;
        console.log(`📺 Séries Redis chargées (${cachedSeries.length} séries)`);
      }
      const seriesTsRaw = await redis.get('lastSeriesScrape');
      if (seriesTsRaw) lastSeriesScrape = seriesTsRaw;
      const seriesDetailsRaw = await redis.get('series_details');
      if (seriesDetailsRaw) {
        const sdObj = typeof seriesDetailsRaw === 'string' ? JSON.parse(seriesDetailsRaw) : seriesDetailsRaw;
        Object.entries(sdObj).forEach(([k, v]) => seriesDetailsCache.set(k, v));
        console.log(`📋 seriesDetailsCache Redis chargé (${seriesDetailsCache.size} entrées)`);
      }
    } catch(e) { console.warn('Erreur chargement Redis:', e.message); }
  }
  // 2. Fallback fichier local si Redis vide ou absent
  if (Object.keys(userdata).length === 0) {
    const file = path.join(DATA_DIR, 'userdata.json');
    try {
      if (fs.existsSync(file)) {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (isOldUserdataFormat(parsed)) {
          migrateUserdata(parsed);
        } else {
          userdata = parsed;
        }
        console.log(`📂 Fallback userdata.json chargé`);
        // Resynchronise vers Redis si possible
        if (redis) {
          redis.set('users',    JSON.stringify(users)).catch(() => {});
          redis.set('userdata', JSON.stringify(userdata)).catch(() => {});
          console.log('🔄 Données resynchronisées vers Redis');
        }
      }
    } catch(e) { console.warn('Impossible de charger userdata.json:', e.message); }
  }
}

async function saveLastScrape() {
  lastScrape = new Date().toISOString();
  if (redis) {
    try { await redis.set('lastScrape', lastScrape); }
    catch(e) { console.warn('Erreur sauvegarde lastScrape:', e.message); }
  }
}

async function saveUsers() {
  if (redis) {
    try { await redis.set('users', JSON.stringify(users)); }
    catch(e) { console.warn('Erreur sauvegarde users Redis:', e.message); }
  }
}

async function savePrefsData() {
  if (redis) {
    try { await redis.set('prefs', JSON.stringify(prefsDB)); }
    catch(e) { console.warn('Erreur sauvegarde prefs Redis:', e.message); }
  }
}

async function saveUserdataFile() {
  // Toujours sauvegarder dans le fichier local (backup)
  try {
    fs.writeFileSync(path.join(DATA_DIR, 'userdata.json'), JSON.stringify(userdata, null, 2), 'utf8');
  } catch(e) { console.warn('Erreur sauvegarde fichier local:', e.message); }
  // Et dans Redis si disponible
  if (redis) {
    try { await redis.set('userdata', JSON.stringify(userdata)); }
    catch(e) { console.warn('Erreur sauvegarde Redis:', e.message); }
  }
}

const detailsCache = new Map();

const BROWSER_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
  'Accept-Encoding': 'gzip, deflate, br',
  'Connection': 'keep-alive',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'none',
};

const http = axios.create({
  headers: BROWSER_HEADERS,
  timeout: 15000,
  validateStatus: (status) => status >= 200 && status < 500,
});

// Cache des pages de liste (évite de re-scraper si l'utilisateur relance)
const pageCache = new Map();
const PAGE_TTL_MS = 1000 * 60 * 20; // 20 min

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

// Fetch avec retry automatique sur 429
async function fetchWithRetry(url, retries = 4) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await http.get(url, {
        headers: { ...BROWSER_HEADERS, Referer: 'https://www.allocine.fr/' },
      });
      if (resp.status === 429) {
        const wait = 60000; // 60s fixes — AlloCiné lève le ban après ~1 min
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

// ─────────────────────────────────────────────────────────────────
//  File d'attente pour les requêtes vers AlloCiné
//  → garantit max 1 requête toutes les ALLO_DELAY ms, même si
//    plusieurs /api/details arrivent en même temps.
// ─────────────────────────────────────────────────────────────────
const ALLO_DELAY      = 900;  // ms entre chaque requête vers AlloCiné
const ALLO_BURST_EVERY = 50;  // toutes les N requêtes, pause longue
const ALLO_BURST_PAUSE = 12000; // 12s de cooldown pour éviter le ban
let _alloQueue   = Promise.resolve();
let _alloReqCount = 0;

function rateLimitedFetch(url) {
  const ticket = _alloQueue.then(async () => {
    _alloReqCount++;
    // Pause longue tous les ALLO_BURST_EVERY requêtes
    if (_alloReqCount % ALLO_BURST_EVERY === 0) {
      console.log(`⏸  Cooldown anti-429 (${_alloReqCount} requêtes) — pause ${ALLO_BURST_PAUSE / 1000}s…`);
      await sleep(ALLO_BURST_PAUSE);
    }
    const resp = await fetchWithRetry(url);
    await sleep(ALLO_DELAY); // pause normale APRÈS la réponse
    return resp;
  });
  // La queue avance même si ce ticket échoue
  _alloQueue = ticket.catch(() => sleep(ALLO_DELAY));
  return ticket;
}

function getCachedDetails(key) {
  const cached = detailsCache.get(key);
  if (!cached) return null;
  if (Date.now() - cached.cachedAt > DETAILS_TTL_MS) {
    detailsCache.delete(key);
    return null;
  }
  return cached.value;
}

function setCachedDetails(key, value) {
  detailsCache.set(key, { value, cachedAt: Date.now() });
  scheduleDetailsBackup();
}

// Sauvegarde différée du cache détails dans Redis (debounce 8s)
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
    console.log(`💾 détailsCache sauvegardé (${detailsCache.size} entrées)`);
  } catch(e) { console.warn('Erreur sauvegarde détailsCache:', e.message); }
}

function htmlToLines(html) {
  const text = html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/?(div|p|li|ul|ol|span|a|h[1-6]|header|footer|nav|section|article|td|th|tr|button|label)[^>]*>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&nbsp;/g, ' ')
    .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(parseInt(c, 10)))
    .replace(/&[a-z]+;/gi, '');

  const lines = text
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);

  for (let i = lines.length - 1; i > 0; i -= 1) {
    if (lines[i] === 'VOD') {
      lines[i - 1] = `${lines[i - 1]} VOD`;
      lines.splice(i, 1);
    }
  }

  return lines;
}

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

// ─────────────────────────────────────────────────────────────────
//  Extrait titre → ID depuis la PAGE DE LISTE AlloCiné VOD
//  Les liens sont de la forme : /film/fichefilm-311364/telecharger-vod/
//  (différent de la page film : fichefilm_gen_cfilm=311364.html)
// ─────────────────────────────────────────────────────────────────
function extractIdsFromListingPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();

  // Liens VOD sur la page de liste : /film/fichefilm-XXXXX/telecharger-vod/
  $('a[href*="fichefilm-"][href*="telecharger-vod"]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const idMatch = href.match(/fichefilm-(\d+)/);
    if (!idMatch) return;

    // Le texte du lien est "Titre du film VOD" → on retire " VOD"
    const rawText = $(el).text().trim().replace(/\s+VOD$/i, '').trim();
    if (!rawText) return;

    const key = normalizeTitle(rawText);
    if (!key || titleToId.has(key)) return;
    titleToId.set(key, idMatch[1]);
  });

  return titleToId;
}

// ─────────────────────────────────────────────────────────────────
//  Extrait titre → ID depuis la PAGE DE RECHERCHE AlloCiné
//  Les liens sont de la forme : /film/fichefilm_gen_cfilm=311364.html
// ─────────────────────────────────────────────────────────────────
function extractIdsFromSearchPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();

  $('a[href*="fichefilm_gen_cfilm="]').each((_, el) => {
    const href = $(el).attr('href') || '';
    const idMatch = href.match(/fichefilm_gen_cfilm=(\d+)/);
    if (!idMatch) return;

    const texts = [
      $(el).text(),
      $(el).attr('title'),
      $(el).find('img').attr('alt'),
      $(el).closest('article, li, div').find('h2, h3, .meta-title, .meta-title-link').first().text(),
    ];

    const title = texts
      .map((v) => String(v || '').trim())
      .find((v) => v && !/^image:/i.test(v));

    if (!title) return;
    const key = normalizeTitle(title);
    if (!key || titleToId.has(key)) return;
    titleToId.set(key, idMatch[1]);
  });

  return titleToId;
}

// Plateformes à ne pas afficher
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

// ─────────────────────────────────────────────────────────────────
//  Extrait les plateformes VOD/streaming depuis la page fiche film
// ─────────────────────────────────────────────────────────────────
function extractProviders(html) {
  const $ = cheerio.load(html);
  const providers = [];
  const seen = new Set();

  $('.provider-tile-primary').each((_, el) => {
    const name = $(el).text().trim();
    if (!name || seen.has(name)) return;
    // Ignorer les plateformes de la liste noire
    if (PROVIDERS_BLACKLIST.has(name.toLowerCase())) return;
    seen.add(name);

    const tileText = $(el).parent().text().toLowerCase();

    let type = 'vod';
    if (/inclus|abonnement|svod/i.test(tileText))      type = 'svod';
    else if (/location/i.test(tileText))                type = 'location';
    else if (/achat/i.test(tileText))                   type = 'achat';

    // Détection complémentaire par nom de plateforme
    if (type === 'vod' && /netflix|prime video|disney\+|canal\+|ocs|paramount\+|crunchyroll|apple tv\+|molotov|arte|france\.tv/i.test(name))
      type = 'svod';

    providers.push({ name, type });
  });

  return providers;
}

function parseFilms(html) {
  // ── NOUVELLE méthode : IDs extraits directement des liens /fichefilm-XXXXX/
  const titleToId = extractIdsFromListingPage(html);

  const lines = htmlToLines(html);
  const films = [];

  for (let i = 0; i < lines.length; i += 1) {
    if (!(lines[i] === 'Presse' && lines[i + 1] && /^\d[,.]\d$/.test(lines[i + 1]))) {
      continue;
    }

    const notePresse = parseFloat(lines[i + 1].replace(',', '.'));
    const noteSpect = lines[i + 2] === 'Spectateurs' && lines[i + 3] && /^\d[,.]\d$/.test(lines[i + 3])
      ? parseFloat(lines[i + 3].replace(',', '.'))
      : null;

    let titre = '', genre = '', realisateur = '', acteurs = '', titreOriginal = '';
    let titleIdx = -1;

    for (let j = i - 1; j >= Math.max(0, i - 20); j -= 1) {
      if (lines[j].endsWith(' VOD')) { titleIdx = j; break; }
    }

    if (titleIdx >= 0) {
      titre = lines[titleIdx].slice(0, -4).trim();
      const seg = lines.slice(titleIdx + 1, i);
      const deIdx   = seg.indexOf('De');
      const avecIdx = seg.indexOf('Avec');
      const origIdx = seg.indexOf('Titre original');
      const pipeIdx = seg.findIndex((line) => line === '|');

      if (pipeIdx >= 0 && deIdx > pipeIdx) {
        genre = seg
          .slice(pipeIdx + 1, deIdx)
          .filter((line) => !/^\d+h/.test(line) && line !== '|')
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

    let synopsis = '';
    const synStart = lines[i + 2] === 'Spectateurs' ? i + 4 : i + 2;
    for (let k = synStart; k < Math.min(lines.length, synStart + 12); k += 1) {
      if (lines[k].endsWith(' VOD')) break;
      if (lines[k].length > 80 && !/^\d/.test(lines[k]) && !lines[k].startsWith('Dès ')) {
        synopsis = lines[k];
        break;
      }
    }

    if (titre) {
      const titleKey    = normalizeTitle(titre);
      const originalKey = normalizeTitle(titreOriginal);
      const allocineId  = titleToId.get(titleKey) || titleToId.get(originalKey) || null;

      films.push({
        titre, titreOriginal, genre, realisateur, acteurs,
        notePresse, noteSpect, synopsis, allocineId,
      });
    }
  }

  return films;
}

function dedupeAndSortFilms(films, noteMin) {
  const seen = new Set();
  return films
    .filter((film) => {
      const key = `${film.titre.toLowerCase()}|${film.notePresse}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .filter((film) => film.notePresse >= noteMin)
    .sort((a, b) => b.notePresse - a.notePresse || a.titre.localeCompare(b.titre, 'fr'));
}

app.get('/api/films', (_req, res) => {
  // Inclure les détails en cache (plateformes, pays, année) indexés par position
  // → le client n'a plus besoin de 500 requêtes individuelles /api/details
  const details = cachedFilms.map(film => {
    const key = film.allocineId ? `id:${film.allocineId}` : `q:${film.titre}`;
    return getCachedDetails(key) || null;
  });
  res.json({ films: cachedFilms, lastScrape, count: cachedFilms.length, details });
});

// ─── Profils utilisateurs ─────────────────────────────────────────────────────
app.get('/api/users', (_req, res) => {
  res.json(Object.values(users));
});

app.post('/api/users', async (req, res) => {
  const { name } = req.body;
  if (!name || typeof name !== 'string' || !name.trim())
    return res.status(400).json({ error: 'name requis' });
  const id = Date.now().toString(36) + Math.random().toString(36).slice(2, 7);
  const user = { id, name: name.trim(), createdAt: new Date().toISOString() };
  users[id]    = user;
  userdata[id] = userdata[id] || {};
  await saveUsers();
  await saveUserdataFile();
  console.log(`👤 Nouveau profil : "${user.name}" (${id})`);
  res.json(user);
});

// ─── Préférences d'affichage (par profil, serveur comme source de vérité) ────
app.get('/api/prefs', (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId requis' });
  res.json(prefsDB[userId] || {});
});

app.post('/api/prefs', async (req, res) => {
  const { userId, ...userPrefs } = req.body;
  if (!userId) return res.status(400).json({ error: 'userId requis' });
  prefsDB[userId] = userPrefs;
  await savePrefsData();
  res.json({ ok: true });
});

// ─── Données utilisateur (Vu / À voir / Non) ─────────────────────────────────
app.get('/api/userdata', (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId requis' });
  res.json(userdata[userId] || {});
});

app.post('/api/userdata', async (req, res) => {
  const { userId, id, vu, vouloir, nonInteresse, asuivre } = req.body;
  if (!userId || !id) return res.status(400).json({ error: 'userId et id requis' });
  if (!userdata[userId]) userdata[userId] = {};
  const entry = { vu: !!vu, vouloir: !!vouloir, nonInteresse: !!nonInteresse, asuivre: !!asuivre };
  if (!entry.vu && !entry.vouloir && !entry.nonInteresse && !entry.asuivre) {
    delete userdata[userId][id];
  } else {
    userdata[userId][id] = entry;
  }
  await saveUserdataFile();
  res.json({ ok: true });
});

app.get('/api/health', (_req, res) => {
  res.json({
    ok: true, port: PORT, totalPages: TOTAL_PAGES,
    cachedDetails: detailsCache.size, cachedFilms: cachedFilms.length, lastScrape,
    version: VERSION, serverStart: SERVER_START,
    lastScrapeErrors,
  });
});

app.get('/api/scrape-status', (_req, res) => {
  const pct = scrapeProgress.total
    ? Math.round(scrapeProgress.current / scrapeProgress.total * 100) : 0;
  res.json({ isScraping, pct, annee: scrapeProgress.annee });
});

app.get('/api/scrape', async (req, res) => {
  if (isScraping) return res.status(429).json({ error: 'Scraping déjà en cours' });
  isScraping = true;
  // Accepte annees=2023,2024,2025 ou l'ancien param annee=2025 (rétrocompat)
  const anneesParam = String(req.query.annees || req.query.annee || '2025').trim();
  const annees = anneesParam.split(',').map(s => s.trim()).filter(s => /^\d{4}$/.test(s));
  if (annees.length === 0)
    return res.status(400).json({ error: 'annees invalide' });

  const noteMin = parseFloat(String(req.query.noteMin || '3.5'));
  if (Number.isNaN(noteMin) || noteMin < 0 || noteMin > 5)
    return res.status(400).json({ error: 'noteMin invalide' });

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  const allFilms = [];
  const totalPages = annees.length * TOTAL_PAGES;
  let globalPage = 0;
  lastScrapeErrors = []; // réinitialise les erreurs

  for (const annee of annees) {
    const base = `https://www.allocine.fr/vod/films/decennie-2020/annee-${annee}/?page=`;
    for (let page = 1; page <= TOTAL_PAGES; page += 1) {
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
        const raw = parseFilms(html).map(f => ({ ...f, anneeSortie: annee }));
        // Filtre noteMin côté serveur avant d'envoyer
        const films = raw.filter(f => f.notePresse >= noteMin);
        allFilms.push(...raw);
        const withId = films.filter(f => f.allocineId).length;
        console.log(`[${annee}] Page ${page}/${TOTAL_PAGES} → ${films.length} films (${withId} avec ID)`);
        // Envoyer les films de cette page immédiatement
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
  const withId = result.filter(f => f.allocineId).length;
  await saveLastScrape();
  // Persiste les films pour les nouveaux visiteurs
  cachedFilms = result;
  if (redis) {
    try { await redis.set('films', JSON.stringify(result)); }
    catch(e) { console.warn('Erreur sauvegarde films Redis:', e.message); }
  }
  send({ type: 'done', totalFilms: result.length, lastScrape });
  res.end();
  isScraping = false;
  console.log(`✅ ${result.length} films (${withId} avec ID, note >= ${noteMin}) — années: ${annees.join(', ')}`);
});

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
  console.log(`Fetch détails: ${cacheKey}`);

  try {
    let resolvedId = allocineId;

    if (!resolvedId) {
      // Fallback : recherche par titre
      const searchUrl  = `https://www.allocine.fr/rechercher/?q=${encodeURIComponent(query)}`;
      const searchResp = await rateLimitedFetch(searchUrl);
      const titleToId  = extractIdsFromSearchPage(searchResp.data);
      resolvedId = titleToId.get(normalizeTitle(query)) || null;

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

    // Vérifier que la page renvoyée est bien une fiche film AlloCiné (pas un CAPTCHA / soft-block)
    const isValidPage = /fichefilm_gen_cfilm|provider-tile|Titre original|Année de production/i.test(filmResp.data);
    if (!isValidPage) {
      console.warn(`Fiche ${resolvedId} → page suspecte (soft-block ?), résultat non mis en cache`);
      return res.json({ pays: null, annee: null, allocineId: resolvedId, allocineUrl: filmUrl, providers: [], error: 'soft_block' });
    }

    // Pays & année
    const lines = htmlToLines(filmResp.data);
    let pays = null, annee = null;
    for (let i = 0; i < lines.length - 1; i += 1) {
      if (lines[i] === 'Nationalité' || lines[i] === 'Nationalités') pays = lines[i + 1];
      if (lines[i] === 'Année de production') annee = lines[i + 1];
      if (pays && annee) break;
    }

    // Plateformes streaming / VOD
    const providers = extractProviders(filmResp.data);

    const data = { pays, annee, allocineId: resolvedId, allocineUrl: filmUrl, providers };
    // Ne mettre en cache que si on a au moins trouvé pays ou annee (sinon la page est peut-être vide)
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
    // On informe le client de la vraie cause de l'erreur
    return res.status(200).json({
      pays: null, annee: null,
      allocineId: allocineId || null,
      allocineUrl: null,
      providers: [],
      error: status === 429 ? 'rate_limited' : message,
    });
  }
});

// Endpoint de test : vérifie que AlloCiné répond correctement
app.get('/api/ping-allocine', async (_req, res) => {
  try {
    const r = await fetchWithRetry('https://www.allocine.fr/', 0);
    res.json({ ok: true, status: r.status });
  } catch (err) {
    const status = err.response?.status || null;
    res.json({ ok: false, status, message: err.message });
  }
});

// Vider le cache des détails (utile quand des résultats vides ont été mis en cache par erreur)
app.post('/api/clear-details-cache', (_req, res) => {
  const count = detailsCache.size;
  detailsCache.clear();
  console.log(`🗑️  Cache vidé (${count} entrées supprimées)`);
  res.json({ ok: true, cleared: count });
});

// ─── Auto-scrape si le cache a plus de AUTO_SCRAPE_DAYS jours ────────────────
async function autoScrapeIfStale() {
  if (isScraping) return;
  const ageMs  = lastScrape ? Date.now() - new Date(lastScrape).getTime() : Infinity;
  const ageDays = ageMs / 86400000;

  if (ageDays < AUTO_SCRAPE_DAYS) {
    console.log(`⏭️  Auto-scrape ignoré — dernier scraping il y a ${ageDays.toFixed(1)}j`);
    return;
  }

  const label = lastScrape ? `${ageDays.toFixed(1)} jour(s)` : 'jamais';
  console.log(`\n🔄 Auto-scrape déclenché — dernier scraping il y a ${label}`);
  isScraping = true;

  const annees  = ['2026', '2025', '2024', '2023'];
  const noteMin = 3.5;
  const allFilms = [];
  lastScrapeErrors = [];
  const totalPages = annees.length * TOTAL_PAGES;
  let globalPage = 0;
  scrapeProgress = { current: 0, total: totalPages, annee: '' };

  try {
    for (const annee of annees) {
      const base = `https://www.allocine.fr/vod/films/decennie-2020/annee-${annee}/?page=`;
      for (let page = 1; page <= TOTAL_PAGES; page++) {
        globalPage++;
        scrapeProgress = { current: globalPage, total: totalPages, annee };
        const url = base + page;
        try {
          let html = getCachedPage(url);
          if (!html) {
            const r = await fetchWithRetry(url);
            html = r.data;
            setCachedPage(url, html);
          }
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

    const result = dedupeAndSortFilms(allFilms, noteMin);
    cachedFilms = result;
    await saveLastScrape();
    if (redis) {
      try { await redis.set('films', JSON.stringify(result)); }
      catch(e) { console.warn('Erreur sauvegarde films Redis (auto-scrape):', e.message); }
    }
    console.log(`✅ Auto-scrape terminé — ${result.length} films\n`);
  } finally {
    isScraping = false;
    scrapeProgress = { current: 0, total: 0, annee: '' };
  }
}

// Crée les profils par défaut s'ils n'existent pas encore (idempotent)
async function seedDefaultProfiles() {
  let changed = false;

  // Renomme le profil migré "Mon profil" → "JC"
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

// ─────────────────────────────────────────────────────────────────
//  Séries — cache mémoire, parseur, API
// ─────────────────────────────────────────────────────────────────

function getCachedSeriesDetails(key) {
  const c = seriesDetailsCache.get(key);
  if (!c) return null;
  if (Date.now() - c.cachedAt > SERIES_DETAILS_TTL_MS) { seriesDetailsCache.delete(key); return null; }
  return c.value;
}
function setCachedSeriesDetails(key, value) {
  seriesDetailsCache.set(key, { value, cachedAt: Date.now() });
  scheduleSeriesDetailsBackup();
}
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
    console.log(`💾 seriesDetailsCache sauvegardé (${seriesDetailsCache.size} entrées)`);
  } catch(e) { console.warn('Erreur sauvegarde seriesDetailsCache:', e.message); }
}

// Extrait les IDs de séries depuis la page de liste
function extractSeriesIdsFromTopPage(html) {
  const $ = cheerio.load(html);
  const titleToId = new Map();
  $('a[href*="ficheserie"]').each((_, el) => {
    const href = $(el).attr('href') || '';
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

// Parse une page de liste des meilleures séries AlloCiné
function parseSeries(html) {
  const $ = cheerio.load(html);
  const titleToId = extractSeriesIdsFromTopPage(html);
  const series = [];
  const seen = new Set();

  // Approche 1 : sélecteurs cheerio (plus fiable si les classes sont stables)
  $('article, li.card, div.card, [class*="entity-card"]').each((_, card) => {
    const $card = $(card);
    const $link = $card.find('a[href*="ficheserie"]').first();
    if (!$link.length) return;
    const href = $link.attr('href') || '';
    const idMatch = href.match(/ficheserie[_-]gen_cserie=(\d+)/) || href.match(/ficheserie-(\d+)/);
    const allocineId = idMatch ? idMatch[1] : null;
    const titre = ($link.attr('title') || $link.text()).trim();
    if (!titre || seen.has(normalizeTitle(titre))) return;
    seen.add(normalizeTitle(titre));
    const ratingNotes = [];
    $card.find('.stareval-note').each((_, el) => {
      const n = parseFloat($(el).text().replace(',', '.'));
      if (!isNaN(n) && n > 0 && n <= 5) ratingNotes.push(n);
    });
    const notePresse = ratingNotes[0] ?? null;
    const noteSpect  = ratingNotes[1] ?? null;
    if (!notePresse) return;
    const genre    = $card.find('.meta-genre').text().trim() || $card.find('[class*="genre"]').first().text().trim();
    const allText  = $card.text();
    const yearMatch = allText.match(/(?:Dès\s+)?(\d{4})/);
    const anneeSortie = yearMatch ? yearMatch[1] : null;
    const synopsis = $card.find('.synopsis-short, [class*="synopsis"]').first().text().trim();
    const $img = $card.find('img').first();
    const rawSrc = $img.attr('data-src') || $img.attr('data-lazy-src') || $img.attr('src') || '';
    const poster = rawSrc && !/blank|placeholder|gif$/i.test(rawSrc) ? rawSrc : null;
    series.push({ titre, titreOriginal: '', genre, anneeSortie, notePresse, noteSpect, synopsis, allocineId, poster });
  });

  // Approche 2 (fallback) : lignes de texte — même principe que parseFilms mais sans " VOD"
  if (series.length === 0) {
    const GENRE_RE = /Drame|Comédie|Action|Thriller|Aventure|Animation|Fantastique|Science|Horreur|Policier|Crime|Biopic|Romance|Historique|Documentaire|Western|Mystère/i;
    const SKIP = new Set(['De', 'Avec', 'Titre original', 'Presse', 'Spectateurs',
                          'En cours', 'Terminée', 'Terminé', 'Diffusion', '']);
    const lines = htmlToLines(html);

    for (let i = 0; i < lines.length; i++) {
      if (!(lines[i] === 'Presse' && /^\d[,.]\d$/.test(lines[i + 1] || ''))) continue;
      const notePresse = parseFloat(lines[i + 1].replace(',', '.'));
      const noteSpect  = lines[i + 2] === 'Spectateurs' && /^\d[,.]\d$/.test(lines[i + 3] || '')
        ? parseFloat(lines[i + 3].replace(',', '.')) : null;

      let titre = '', genre = '', anneeSortie = null;
      for (let j = i - 1; j >= Math.max(0, i - 20); j--) {
        const line = lines[j];
        if (!line || SKIP.has(line)) continue;
        if (/^\d[,.]\d$/.test(line) || /^#?\d+$/.test(line)) continue;
        if (/^\d{4}/.test(line) || /^Dès\s+\d{4}/.test(line)) {
          if (!anneeSortie) { const m = line.match(/\d{4}/); if (m) anneeSortie = m[0]; }
          continue;
        }
        if (/^\d+\s+saison/.test(line)) continue;
        if (GENRE_RE.test(line) && !genre) { genre = line; continue; }
        titre = line; break;
      }
      if (!titre || seen.has(normalizeTitle(titre))) continue;
      seen.add(normalizeTitle(titre));
      const synStart = noteSpect !== null ? i + 4 : i + 2;
      let synopsis = '';
      for (let k = synStart; k < Math.min(lines.length, synStart + 10); k++) {
        if (lines[k] === 'Presse') break;
        if (lines[k] && lines[k].length > 80 && !/^\d/.test(lines[k])) { synopsis = lines[k]; break; }
      }
      const allocineId = titleToId.get(normalizeTitle(titre)) || null;
      series.push({ titre, titreOriginal: '', genre, anneeSortie, notePresse, noteSpect, synopsis, allocineId });
    }
  }
  return series;
}

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

// ── Routes séries ─────────────────────────────────────────────────────────────

app.get('/api/series', (_req, res) => {
  const details = cachedSeries.map(s => {
    const key = s.allocineId ? `sid:${s.allocineId}` : null;
    return key ? (getCachedSeriesDetails(key) || null) : null;
  });
  res.json({ series: cachedSeries, lastScrape: lastSeriesScrape, count: cachedSeries.length, details });
});

app.get('/api/series/health', (_req, res) => {
  res.json({
    ok: true, cachedSeries: cachedSeries.length, cachedDetails: seriesDetailsCache.size,
    lastScrape: lastSeriesScrape, isScrapingSeries, version: VERSION,
    lastScrapeErrors: lastSeriesScrapeErrors,
  });
});

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

app.get('/api/series/scrape-status', (_req, res) => {
  const pct = seriesProgress.total
    ? Math.round(seriesProgress.current / seriesProgress.total * 100) : 0;
  res.json({ isScraping: isScrapingSeries, pct });
});

app.get('/api/series/scrape', async (req, res) => {
  if (isScrapingSeries) return res.status(429).json({ error: 'Scraping déjà en cours' });
  isScrapingSeries = true;
  lastSeriesScrapeErrors = [];
  seriesProgress = { current: 0, total: SERIES_PAGES };

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache, no-transform');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const send = (data) => res.write(`data: ${JSON.stringify(data)}\n\n`);
  const allSeries = [];
  let globalPage = 0;

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

  const result = dedupeAndSortSeries(allSeries);
  cachedSeries = result;
  lastSeriesScrape = new Date().toISOString();
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

// Debug : montre les lignes brutes extraites d'une fiche série
app.get('/api/series/debug-lines', async (req, res) => {
  const seriesId = String(req.query.seriesId || '').trim();
  if (!seriesId) return res.status(400).json({ error: 'seriesId requis' });
  try {
    const url  = `https://www.allocine.fr/series/ficheserie_gen_cserie=${seriesId}.html`;
    const resp = await rateLimitedFetch(url);
    const lines = htmlToLines(resp.data);
    // Filtre les 200 premières lignes non vides qui pourraient contenir des métadonnées
    const relevant = lines.slice(0, 300).filter(l =>
      /\d{4}|saison|statut|nationalit|créa|avec|depuis|en cours/i.test(l) || l.length < 60
    );
    res.json({ seriesId, url, relevant, total: lines.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/series/details', async (req, res) => {
  const seriesId = String(req.query.seriesId || '').trim();
  if (!seriesId) return res.status(400).json({ error: 'seriesId requis' });

  const cacheKey = `sid:${seriesId}`;
  const cached = getCachedSeriesDetails(cacheKey);
  // Si en cache mais sans année → on re-fetche pour bénéficier des nouvelles regex
  if (cached && cached.derniereAnnee) { console.log(`Cache série: ${seriesId}`); return res.json(cached); }
  if (cached) console.log(`Cache série sans année, re-fetch: ${seriesId}`);

  try {
    const url   = `https://www.allocine.fr/series/ficheserie_gen_cserie=${seriesId}.html`;
    const resp  = await rateLimitedFetch(url);
    const html  = resp.data;
    const lines = htmlToLines(html);

    let createur = null, nbSaisons = null, statut = null, derniereAnnee = null, pays = null;
    const castingArr = [];

    for (let i = 0; i < lines.length - 1; i++) {
      const l = lines[i], n = lines[i + 1];
      if (l === 'Nationalité' || l === 'Nationalités') pays = n;
      if (l === 'Saisons' && /^\d+$/.test(n)) nbSaisons = parseInt(n);
      if ((l === 'Créée par' || l === 'Créé par' || l === 'Créateur') && !createur) createur = n;
      if (l === 'Statut') statut = /en cours/i.test(n) ? 'En cours' : /termin/i.test(n) ? 'Terminée' : n;
      if (l === 'Avec' && castingArr.length === 0) {
        for (let k = i + 1; k < Math.min(lines.length, i + 8); k++) {
          if (['De', 'Avec', 'Nationalité', 'Saisons', 'Statut', 'Presse'].includes(lines[k])) break;
          if (lines[k] && !/^\d/.test(lines[k])) castingArr.push(lines[k].replace(/,$/, ''));
        }
      }
      // Plage d'années ex: "2008 - 2013", "2019 - en cours", "2020 − 2023" (tirets variés)
      if (/^\d{4}\s*[-–—−]\s*(\d{4}|en cours|\.\.\.)$/i.test(l)) {
        const parts = l.split(/\s*[-–—−]\s*/);
        const yB = parts[1]?.match(/\d{4}/)?.[0];
        const yA = parts[0]?.match(/\d{4}/)?.[0];
        if (yB) derniereAnnee = yB;
        else if (yA && !derniereAnnee) derniereAnnee = yA;
      }
      // "Depuis 2020" ou "depuis 2020"
      if (!derniereAnnee) {
        const m = l.match(/^[Dd]epuis\s+(\d{4})$/);
        if (m) derniereAnnee = m[1];
      }
      // Ligne contenant une plage d'années dans du texte ex: "Série de 2020 à 2023"
      if (!derniereAnnee) {
        const m = l.match(/(\d{4})\s*(?:à|au|[-–—−])\s*(\d{4})/);
        if (m) {
          const y = parseInt(m[2]);
          if (y >= 1950 && y <= 2030) derniereAnnee = m[2];
        }
      }
      // Année isolée ex: "2020" (série courte sans plage)
      if (!derniereAnnee && /^\d{4}$/.test(l)) {
        const y = parseInt(l);
        if (y >= 1950 && y <= 2030) derniereAnnee = l;
      }
    }

    const providers = extractProviders(html);
    const $d = cheerio.load(html);
    const poster = $d('meta[property="og:image"]').attr('content') || null;
    const data = {
      createur, nbSaisons, statut, derniereAnnee, pays,
      casting: castingArr.slice(0, 5).join(', '),
      providers, poster, allocineId: seriesId, allocineUrl: url,
    };
    if (createur || nbSaisons || providers.length > 0 || pays) setCachedSeriesDetails(cacheKey, data);
    const pNames = providers.map(p => `${p.name}(${p.type})`).join(', ') || '—';
    console.log(`Série ${seriesId} → ${pays || '?'} statut:${statut || '?'} saisons:${nbSaisons ?? '?'} | ${pNames}`);
    return res.json(data);
  } catch(e) {
    const status = e.response?.status;
    return res.json({
      createur: null, nbSaisons: null, statut: null, derniereAnnee: null, pays: null,
      casting: '', providers: [], allocineId: seriesId,
      error: status === 429 ? 'rate_limited' : e.message,
    });
  }
});

app.post('/api/series/clear-cache', async (_req, res) => {
  const count = seriesDetailsCache.size;
  seriesDetailsCache.clear();
  if (redis) { try { await redis.del('series_details'); } catch(e) { console.warn('Redis del series_details:', e.message); } }
  console.log(`🗑️  Cache séries vidé (${count} entrées)`);
  res.json({ ok: true, cleared: count });
});

app.listen(PORT, () => {
  console.log('\n🎬 AlloCiné VOD Scraper v9 démarré !');
  console.log(`   ➜ Ouvrez : http://localhost:${PORT}`);
  console.log(`   ➜ Santé  : http://localhost:${PORT}/api/health\n`);

  // Chargement Redis → profils par défaut → auto-scrape si besoin
  loadUserdata()
    .then(() => seedDefaultProfiles())
    .then(() => autoScrapeIfStale());
  setInterval(() => autoScrapeIfStale(), 24 * 60 * 60 * 1000);
});
