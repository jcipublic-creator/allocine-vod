// ─────────────────────────────────────────────────────────────────────────────
// shared.js — Logique métier partagée (sans DOM)
// ─────────────────────────────────────────────────────────────────────────────

// ─── Pont global vers UI (appelé depuis onchange="applyFilters()" dans le HTML) ─
function applyFilters() { UI.applyFilters(); }

// ─── État global ──────────────────────────────────────────────────────────────
let _allFilms       = [];
let _details        = {};   // filmKey → { pays, annee, providers, ... }
let _userdata       = {};   // allocineId → { vu, vouloir, nonInteresse }  (propre à _currentUserId)
let _currentUserId  = null; // profil actif sur ce device
let _allPlats       = new Set();
let _platsDone      = 0;
let _loadGen        = 0;    // compteur de génération pour annuler les workers
let _sortBy         = 'presse';
let _scrapingDone   = true;
let _genreDefaultApplied = false;

const LS_USER_ID = 'vod_user_id';

// ─── Constantes & utilitaires ─────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

const LS_FILMS   = 'vod_films';
const LS_DETAILS = 'vod_details';
const LS_DATE    = 'vod_updated';
const LS_VERSION = 'vod_cache_v82'; // incrémenter si le format du cache change

const esc = s => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

const filmKey = f => `${(f.titre||'').toLowerCase()}|${f.notePresse}`;

const FLAGS = {
  'afghanistan':'🇦🇫','afrique du sud':'🇿🇦','albanie':'🇦🇱','algérie':'🇩🇿',
  'allemagne':'🇩🇪','angola':'🇦🇴','arabie saoudite':'🇸🇦','argentine':'🇦🇷',
  'arménie':'🇦🇲','australie':'🇦🇺','autriche':'🇦🇹','azerbaïdjan':'🇦🇿',
  'belgique':'🇧🇪','bolivie':'🇧🇴','bosnie-herzégovine':'🇧🇦','brésil':'🇧🇷',
  'bulgarie':'🇧🇬','cambodge':'🇰🇭','cameroun':'🇨🇲','canada':'🇨🇦',
  'chili':'🇨🇱','chine':'🇨🇳','chypre':'🇨🇾','colombie':'🇨🇴',
  'corée du nord':'🇰🇵','corée du sud':'🇰🇷','côte d\'ivoire':'🇨🇮','croatie':'🇭🇷',
  'cuba':'🇨🇺','danemark':'🇩🇰','égypte':'🇪🇬','émirats arabes unis':'🇦🇪',
  'équateur':'🇪🇨','espagne':'🇪🇸','estonie':'🇪🇪','états-unis':'🇺🇸','etats-unis':'🇺🇸','u.s.a.':'🇺🇸','usa':'🇺🇸','united states':'🇺🇸',
  'éthiopie':'🇪🇹','finlande':'🇫🇮','france':'🇫🇷','géorgie':'🇬🇪',
  'ghana':'🇬🇭','grèce':'🇬🇷','guatemala':'🇬🇹','hongrie':'🇭🇺',
  'hong kong':'🇭🇰','inde':'🇮🇳','indonésie':'🇮🇩','irak':'🇮🇶',
  'iran':'🇮🇷','irlande':'🇮🇪','islande':'🇮🇸','israël':'🇮🇱',
  'italie':'🇮🇹','jamaïque':'🇯🇲','japon':'🇯🇵','jordanie':'🇯🇴',
  'kazakhstan':'🇰🇿','kenya':'🇰🇪','kosovo':'🇽🇰','lettonie':'🇱🇻',
  'liban':'🇱🇧','lituanie':'🇱🇹','luxembourg':'🇱🇺','macédoine':'🇲🇰',
  'malaisie':'🇲🇾','mali':'🇲🇱','maroc':'🇲🇦','mexique':'🇲🇽',
  'moldavie':'🇲🇩','mongolie':'🇲🇳','monténégro':'🇲🇪','mozambique':'🇲🇿',
  'nigeria':'🇳🇬','norvège':'🇳🇴','nouvelle-zélande':'🇳🇿','pakistan':'🇵🇰',
  'palestine':'🇵🇸','pays-bas':'🇳🇱','pérou':'🇵🇪','philippines':'🇵🇭',
  'pologne':'🇵🇱','portugal':'🇵🇹','roumanie':'🇷🇴','royaume-uni':'🇬🇧','grande-bretagne':'🇬🇧','great britain':'🇬🇧','uk':'🇬🇧',
  'russie':'🇷🇺','sénégal':'🇸🇳','serbie':'🇷🇸','singapour':'🇸🇬',
  'slovaquie':'🇸🇰','slovénie':'🇸🇮','somalie':'🇸🇴','soudan':'🇸🇩',
  'suède':'🇸🇪','suisse':'🇨🇭','taiwan':'🇹🇼','thaïlande':'🇹🇭',
  'tunisie':'🇹🇳','turquie':'🇹🇷','ukraine':'🇺🇦','uruguay':'🇺🇾',
  'venezuela':'🇻🇪','vietnam':'🇻🇳',
};

function flagFor(pays) {
  if (!pays) return '';
  const first = pays.split(',')[0].trim().toLowerCase();
  return FLAGS[first] ? FLAGS[first] + ' ' : '';
}

function renderPlatBadges(providers) {
  if (!providers || providers.length === 0) return '<span class="pb-none">—</span>';
  const filtered = providers.filter(p => !/dvd|blu.ray/i.test(p.name));
  if (filtered.length === 0) return '<span class="pb-none">—</span>';
  return filtered.map(p => `<span class="pb ${esc(p.type)}">${esc(p.name)}</span>`).join('');
}

// ─── Cache localStorage ───────────────────────────────────────────────────────
function saveCache() {
  try {
    localStorage.setItem(LS_FILMS,   JSON.stringify(_allFilms));
    localStorage.setItem(LS_DETAILS, JSON.stringify(_details));
    localStorage.setItem(LS_DATE,    new Date().toISOString());
  } catch(e) { console.warn('[cache] Erreur sauvegarde :', e.message); }
}

function loadCache() {
  try {
    // Si le cache est d'une ancienne version, on le purge pour forcer un rechargement serveur
    if (localStorage.getItem('vod_cache_version') !== LS_VERSION) {
      localStorage.removeItem(LS_FILMS);
      localStorage.removeItem(LS_DETAILS);
      localStorage.removeItem(LS_DATE);
      localStorage.setItem('vod_cache_version', LS_VERSION);
      console.log('[cache] Version obsolète — cache purgé');
      return false;
    }

    const films   = localStorage.getItem(LS_FILMS);
    const details = localStorage.getItem(LS_DETAILS);
    const date    = localStorage.getItem(LS_DATE);
    if (!films) return false;

    _allFilms = JSON.parse(films);
    _details  = details ? JSON.parse(details) : {};
    _allPlats = new Set();
    Object.values(_details).forEach(d => (d?.providers||[]).forEach(p => _allPlats.add(p.name)));

    const d = date ? new Date(date) : null;
    const label = d ? `${d.toLocaleDateString('fr-FR')} à ${d.toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit'})}` : '—';
    UI.onCacheLoaded(label);

    return true;
  } catch(e) { console.warn('[cache] Erreur chargement :', e.message); return false; }
}

// ─── Gestion des profils utilisateurs ────────────────────────────────────────

// Appelé au démarrage. Retourne un objet { mode, users? } :
//   'existing' → userId déjà en localStorage, prêt à l'emploi
//   'auto'     → un seul profil serveur, sélectionné automatiquement
//   'pick'     → plusieurs profils (ou aucun) → l'UI doit proposer de choisir
async function initUser() {
  const stored = localStorage.getItem(LS_USER_ID);
  if (stored) {
    _currentUserId = stored;
    return { mode: 'existing' };
  }
  // Pas de profil en localStorage → toujours proposer le choix
  try {
    const r = await fetch('/api/users');
    if (!r.ok) return { mode: 'pick', users: [] };
    const list = await r.json();
    return { mode: 'pick', users: list };
  } catch(e) {
    console.warn('[user] initUser error:', e.message);
    return { mode: 'pick', users: [] };
  }
}

async function loadUsers() {
  const r = await fetch('/api/users');
  if (!r.ok) throw new Error('Erreur /api/users');
  return r.json();
}

async function createUser(name) {
  const r = await fetch('/api/users', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name: name.trim() })
  });
  if (!r.ok) throw new Error('Erreur création profil');
  const user = await r.json();
  _currentUserId = user.id;
  localStorage.setItem(LS_USER_ID, _currentUserId);
  return user;
}

function switchUser(userId) {
  _currentUserId = userId;
  localStorage.setItem(LS_USER_ID, userId);
}

async function loadServerPrefs() {
  if (!_currentUserId) return null;
  try {
    const r = await fetch(`/api/prefs?userId=${encodeURIComponent(_currentUserId)}`);
    if (!r.ok) return null;
    return await r.json();
  } catch(e) { return null; }
}

async function saveServerPrefs(prefsData) {
  if (!_currentUserId) return;
  try {
    await fetch('/api/prefs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: _currentUserId, ...prefsData })
    });
  } catch(e) { console.warn('Erreur sauvegarde prefs serveur:', e.message); }
}

// ─── Base de données utilisateur ──────────────────────────────────────────────
async function loadUserdata() {
  if (!_currentUserId) return;
  try {
    const r = await fetch(`/api/userdata?userId=${encodeURIComponent(_currentUserId)}`);
    _userdata = await r.json();
    UI.reapplyUserActions();
    UI.applyFilters();
  } catch(e) { console.warn('Erreur chargement userdata:', e.message); }
}

async function saveUserdata(id) {
  if (!_currentUserId) return;
  try {
    await fetch('/api/userdata', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: _currentUserId, id, ...(_userdata[id] || {}) })
    });
  } catch(e) { console.warn('Erreur sauvegarde userdata:', e.message); }
}

function toggleUA(id, idx, field) {
  if (!_currentUserId) return; // pas de profil sélectionné
  if (!_userdata[id]) _userdata[id] = {};
  _userdata[id][field] = !_userdata[id][field];
  const film = _allFilms[idx];
  if (film) UI.renderUserActions(film, idx);
  saveUserdata(id);
}

function reapplyUserActions() {
  _allFilms.forEach((film, i) => {
    UI.renderUserActions(film, i);
  });
}

// ─── Filtres (pays) ────────────────────────────────────────────────────────────
function populatePaysFilter() {
  const paysSet = new Set();
  Object.values(_details).forEach(det => {
    if (!det || !det.pays) return;
    det.pays.split(',').map(p => p.trim()).filter(Boolean).forEach(p => paysSet.add(p));
  });
  const sel = document.getElementById('fil-pays');
  if (!sel) return;
  const cur = sel.value;
  sel.innerHTML = '<option value="">Tous les pays</option>';
  [...paysSet].sort((a, b) => a.localeCompare(b, 'fr')).forEach(p =>
    sel.insertAdjacentHTML('beforeend', `<option value="${esc(p)}"${cur===p?' selected':''}>${esc(p)}</option>`)
  );
  if (cur && [...paysSet].includes(cur)) sel.value = cur;
}

// ─── Scraping ─────────────────────────────────────────────────────────────────
function startScrape() {
  const noteMin = parseFloat(document.getElementById('inp-note').value || '3.5');
  const btn     = document.getElementById('btn-run');

  _scrapingDone = false;
  _loadGen++;

  UI.onScrapeStart(btn);

  const seen = new Set();
  const pendingFilms = [];

  const es = new EventSource(`/api/scrape?annees=2026,2025,2024,2023&noteMin=${noteMin}`);

  es.onmessage = e => {
    const d = JSON.parse(e.data);

    if (d.type === 'films') {
      const pct = Math.round(d.page / d.total * 100);
      d.films.forEach(f => {
        const key = `${f.titre.toLowerCase()}|${f.notePresse}`;
        if (!seen.has(key)) { seen.add(key); pendingFilms.push(f); }
      });
      UI.onScrapeProgress(pct, d.annee, pendingFilms.length);
    }

    if (d.type === 'error') UI.showError(`⚠ Erreur page ${d.page} : ${d.message}`);

    if (d.type === 'done') {
      es.close();
      _allFilms = pendingFilms;
      _allPlats = new Set(); _platsDone = 0;
      _scrapingDone = true;
      UI.populateGenreFilter();
      UI.populatePlatFilter?.();
      populatePaysFilter();
      UI.renderFilms(_allFilms);
      applySort();
      UI.applyFilters();
      UI.onScrapeDone(btn, d.lastScrape);
      startPlatformLoading();
    }
  };

  es.onerror = () => {
    es.close();
    _scrapingDone = true;
    UI.onScrapeError(btn);
  };
}

// ─── Chargement automatique des plateformes ────────────────────────────────────
async function startPlatformLoading() {
  const gen = _loadGen;
  _platsDone = 0;
  let errors = 0;

  // N'affiche la barre que s'il y a vraiment des détails à fetcher
  const needsFetch = _allFilms.some(f => !_details[filmKey(f)]);
  if (needsFetch) UI.onPlatStart();

  let idx = 0;

  while (true) {
    if (_loadGen !== gen) return;

    if (idx < _allFilms.length) {
      const i = idx++;
      const film = _allFilms[i];
      const key = filmKey(film);

      if (_details[key]) {
        _platsDone++;
        const el = UI.getPlatEl(film);
        if (el) el.innerHTML = renderPlatBadges(_details[key].providers || []);
        (_details[key].providers||[]).forEach(p => _allPlats.add(p.name));
        UI.onPlatProgress(_platsDone, _allFilms.length);
        continue;
      }

      const err = await fetchDetails(i, gen);
      if (_loadGen !== gen) return;
      _platsDone++;
      if (err) errors++;
      UI.onPlatProgress(_platsDone, _allFilms.length);

    } else if (_scrapingDone) {
      break;
    } else {
      await sleep(300);
    }
  }

  if (_loadGen !== gen) {
    if (needsFetch) UI.onPlatDone(errors); // toujours cacher la barre même si annulé
    return;
  }
  applySort();
  saveCache();
  if (needsFetch) UI.onPlatDone(errors);
}

async function fetchDetails(idx, gen) {
  if (_loadGen !== gen) return null;

  const film = _allFilms[idx];
  if (!film) return null;
  const key = filmKey(film);
  if (_details[key]) return null;

  const platEl = () => {
    const cur = _allFilms.indexOf(film);
    return cur >= 0 ? UI.getPlatEl(_allFilms[cur]) : null;
  };

  try {
    const params = new URLSearchParams();
    if (film.allocineId) params.set('allocineId', film.allocineId);
    else params.set('q', film.titre);

    const resp = await fetch(`/api/details?${params}`);
    if (_loadGen !== gen) return null;

    if (!resp.ok) {
      const errMsg = `HTTP ${resp.status} pour "${film.titre}"`;
      _details[key] = { providers: [] };
      const el = platEl(); if (el) el.innerHTML = '<span class="pb-none">—</span>';
      return errMsg;
    }

    const data = await resp.json();
    if (_loadGen !== gen) return null;

    if (data.error) {
      _details[key] = { providers: [] };
      const el = platEl(); if (el) el.innerHTML = `<span class="pb-none" title="${data.error}">⚠</span>`;
      return data.error;
    }

    _details[key] = data;
    if (data.allocineId && !film.allocineId) film.allocineId = data.allocineId;

    const el = platEl(); if (el) el.innerHTML = renderPlatBadges(data.providers || []);
    (data.providers || []).forEach(p => _allPlats.add(p.name));
    if (data.pays) populatePaysFilter();

    const curIdx = _allFilms.indexOf(film);
    if (curIdx >= 0) UI.onDetailFetched(curIdx, data);

    return null;

  } catch (e) {
    if (_loadGen !== gen) return null;
    const errMsg = `Réseau: ${e.message}`;
    _details[key] = { providers: [] };
    const el = platEl(); if (el) el.innerHTML = '<span class="pb-none">—</span>';
    return errMsg;
  }
}

// ─── Tri ──────────────────────────────────────────────────────────────────────
function applySort(critere) {
  if (critere) _sortBy = critere;

  UI.updateSortButtons(_sortBy);

  _allFilms.sort((a, b) => {
    if (_sortBy === 'annee') {
      const ya = parseInt(a.anneeReelle || a.anneeSortie) || 0;
      const yb = parseInt(b.anneeReelle || b.anneeSortie) || 0;
      return yb - ya || b.notePresse - a.notePresse;
    }
    if (_sortBy === 'spectateurs') {
      if (a.noteSpect == null && b.noteSpect == null) return 0;
      if (a.noteSpect == null) return 1;
      if (b.noteSpect == null) return -1;
      return b.noteSpect - a.noteSpect || b.notePresse - a.notePresse;
    }
    return b.notePresse - a.notePresse || a.titre.localeCompare(b.titre, 'fr');
  });

  UI.renderFilms(_allFilms);
  UI.reapplyAfterSort(_allFilms);
  UI.applyFilters();
}

// ─── Confirmation scraping ────────────────────────────────────────────────────
function openConfirm() {
  const el = document.getElementById('confirm-last-scrape');
  fetch('/api/health', { signal: AbortSignal.timeout(3000) })
    .then(r => r.json())
    .then(d => {
      if (d.lastScrape) {
        const dt = new Date(d.lastScrape);
        const label = dt.toLocaleDateString('fr-FR') + ' à ' + dt.toLocaleTimeString('fr-FR', {hour:'2-digit', minute:'2-digit'});
        el.textContent = `Dernier scraping : ${label}`;
      } else {
        el.textContent = 'Aucun scraping effectué.';
      }
    })
    .catch(() => { el.textContent = ''; });
  document.getElementById('confirm-modal').classList.add('open');
}

function closeConfirm() {
  document.getElementById('confirm-modal').classList.remove('open');
}

// ─── Chargement films depuis le serveur ────────────────────────────────────
// ─── Monitoring auto-scrape ───────────────────────────────────────────────────
let _pollTimer = null;

async function startAutoScrapeMonitoring() {
  try {
    const r = await fetch('/api/scrape-status');
    const d = await r.json();
    if (d.isScraping) {
      UI.showAutoScrapeBar(d.pct, d.annee);
      _startPollLoop();
    }
  } catch(e) {}
}

function _startPollLoop() {
  if (_pollTimer) return;
  _pollTimer = setInterval(async () => {
    try {
      const r = await fetch('/api/scrape-status');
      const d = await r.json();
      if (d.isScraping) {
        UI.showAutoScrapeBar(d.pct, d.annee);
      } else {
        UI.hideAutoScrapeBar();
        clearInterval(_pollTimer);
        _pollTimer = null;
        // Recharger les films une fois le scraping terminé
        await loadFilmsFromServer();
      }
    } catch(e) {}
  }, 5000);
}

async function loadFilmsFromServer() {
  try {
    const r = await fetch('/api/films', { signal: AbortSignal.timeout(8000) });
    if (!r.ok) return false;
    const d = await r.json();
    if (!d.films || d.films.length === 0) return false;
    _allFilms = d.films;
    if (d.details) {
      d.films.forEach((film, i) => {
        if (d.details[i]) {
          _details[filmKey(film)] = d.details[i];
          (d.details[i].providers || []).forEach(p => _allPlats.add(p.name));
        }
      });
    }
    UI.populateGenreFilter();
    UI.populatePlatFilter?.();
    populatePaysFilter();
    applySort();
    UI.applyFilters();
    UI.onFilmsFromServer(d);
    _scrapingDone = true;
    startPlatformLoading();
    return true;
  } catch(e) {
    console.warn('[server] Impossible de charger les films:', e.message);
    return false;
  }
}
