const express = require('express');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const cheerio = require('cheerio');
const { Redis } = require('@upstash/redis');

const app = express();
const PORT = Number(process.env.PORT || 3009);
const TOTAL_PAGES = Number(process.env.TOTAL_PAGES || 25);
const DETAILS_TTL_MS = 1000 * 60 * 60 * 24; // 24h
const BUILD = (() => { try { return require('./version.json').build; } catch(e) { return 0; } })();
const VERSION = `v9.2.${BUILD}`;
const SERVER_START = new Date().toISOString();
const DATA_DIR = process.env.DATA_DIR || __dirname;
let lastScrapeErrors = [];

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
let userdata = {};
let lastScrape = null; // timestamp ISO du dernier scraping
let cachedFilms = [];  // derniers films scrapés, persistés dans Redis

async function loadUserdata() {
  // 1. Essayer Redis en priorité
  if (redis) {
    try {
      const data = await redis.get('userdata');
      if (data) {
        userdata = typeof data === 'string' ? JSON.parse(data) : data;
        console.log(`📂 Userdata Redis chargé (${Object.keys(userdata).length} entrées)`);
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
    } catch(e) { console.warn('Erreur chargement Redis:', e.message); }
  }
  // 2. Fallback fichier local si Redis vide ou absent
  if (Object.keys(userdata).length === 0) {
    const file = path.join(DATA_DIR, 'userdata.json');
    try {
      if (fs.existsSync(file)) {
        userdata = JSON.parse(fs.readFileSync(file, 'utf8'));
        console.log(`📂 Fallback userdata.json (${Object.keys(userdata).length} entrées)`);
        // Resynchronise vers Redis si possible
        if (redis && Object.keys(userdata).length > 0) {
          redis.set('userdata', JSON.stringify(userdata)).catch(() => {});
          console.log('🔄 Userdata resynchronisé vers Redis');
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

loadUserdata();

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

app.get('/api/userdata', (_req, res) => {
  res.json(userdata);
});

app.post('/api/userdata', async (req, res) => {
  const { id, vu, vouloir, nonInteresse } = req.body;
  if (!id) return res.status(400).json({ error: 'id requis' });
  const entry = { vu: !!vu, vouloir: !!vouloir, nonInteresse: !!nonInteresse };
  if (!entry.vu && !entry.vouloir && !entry.nonInteresse) {
    delete userdata[id];
  } else {
    userdata[id] = entry;
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

app.get('/api/scrape', async (req, res) => {
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

app.listen(PORT, () => {
  console.log('\n🎬 AlloCiné VOD Scraper v9 démarré !');
  console.log(`   ➜ Ouvrez : http://localhost:${PORT}`);
  console.log(`   ➜ Santé  : http://localhost:${PORT}/api/health\n`);
});
