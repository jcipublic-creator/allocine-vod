// ─────────────────────────────────────────────────────────────────────────────
// shared.js — Logique métier partagée (sans DOM)
// ─────────────────────────────────────────────────────────────────────────────

// ─── Pont global vers UI (appelé depuis onchange="applyFilters()" dans le HTML) ─
function applyFilters() { UI.applyFilters(); }

// ─── Sécurité — Secret applicatif ────────────────────────────────────────────
// Chargé depuis /api/config au démarrage de la page.
// Ajouté automatiquement en header x-app-secret sur tous les appels /api/.
let _appSecret = '';
const _origFetch = window.fetch.bind(window);
window.fetch = function(url, opts = {}) {
  if (_appSecret && typeof url === 'string' && url.startsWith('/api/')) {
    opts = { ...opts, headers: { ...(opts.headers || {}), 'x-app-secret': _appSecret } };
  }
  return _origFetch(url, opts);
};
const LS_PROFILE_RESET = 'vod_profile_reset_at';

/** Charge le secret + vérifie si un reset de profil a été demandé côté serveur.
 *  Gère aussi les tokens de reset PIN arrivant via ?reset_token=xxx dans l'URL. */
async function loadAppSecret() {
  // ── Vérification d'un token de reset PIN dans l'URL ──────────────────────
  const urlParams = new URLSearchParams(window.location.search);
  const resetToken = urlParams.get('reset_token');
  if (resetToken) {
    // Nettoyer l'URL sans recharger la page
    const cleanUrl = window.location.pathname;
    window.history.replaceState({}, '', cleanUrl);
    // Soumettre le token au serveur
    try {
      const r = await _origFetch('/api/reset-pin-by-token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ token: resetToken }),
        signal: AbortSignal.timeout(5000)
      });
      if (r.ok) {
        const d = await r.json();
        // Forcer la sélection de profil + afficher un message de succès
        localStorage.removeItem('vod_user_id');
        // Afficher un toast après chargement de la page
        window._pinResetSuccess = d.userName || 'ton profil';
      } else {
        window._pinResetError = true;
      }
    } catch(e) { window._pinResetError = true; }
  }

  try {
    const r = await _origFetch('/api/config', { signal: AbortSignal.timeout(3000) });
    if (r.ok) {
      const d = await r.json();
      _appSecret = d.secret || '';
      // Si le serveur a demandé un reset plus récent que le dernier vu par ce client,
      // on efface le profil mémorisé pour forcer la modal de sélection.
      if (d.profileResetAt) {
        const localReset = localStorage.getItem(LS_PROFILE_RESET);
        if (!localReset || d.profileResetAt > localReset) {
          localStorage.removeItem('vod_user_id');
          localStorage.setItem(LS_PROFILE_RESET, d.profileResetAt);
        }
      }
    }
  } catch(e) { /* pas de secret → mode dev local */ }
  // Afficher le feedback de reset PIN après que le DOM soit prêt
  setTimeout(_showPinResetFeedback, 800);
}

/** Affiche un toast de confirmation si un PIN vient d'être réinitialisé via email. */
function _showPinResetFeedback() {
  if (window._pinResetSuccess) {
    const name = window._pinResetSuccess;
    delete window._pinResetSuccess;
    const t = document.createElement('div');
    t.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:9999;background:rgba(68,204,153,.15);border:1px solid rgba(68,204,153,.3);border-radius:10px;padding:14px 20px;font-size:13px;color:#4c9;box-shadow:0 4px 20px rgba(0,0,0,.5);text-align:center;max-width:300px';
    t.textContent = `✅ PIN de "${name}" supprimé. Choisis ton profil pour te connecter.`;
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 7000);
  }
  if (window._pinResetError) {
    delete window._pinResetError;
    const t = document.createElement('div');
    t.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:9999;background:rgba(229,85,85,.15);border:1px solid rgba(229,85,85,.3);border-radius:10px;padding:14px 20px;font-size:13px;color:#e55;box-shadow:0 4px 20px rgba(0,0,0,.5);text-align:center;max-width:300px';
    t.textContent = '❌ Lien de réinitialisation invalide ou expiré.';
    document.body.appendChild(t);
    setTimeout(() => t.remove(), 6000);
  }
}

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
const LS_VERSION = 'vod_cache_v1'; // ⚠️ changer UNIQUEMENT si le format des données évolue

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
  _connectionPinged = false; // réinitialise pour que le prochain loadUserdata() pinge le nouveau profil
}

// ─── Protection PIN ───────────────────────────────────────────────────────────

// ── Pavé numérique partagé (sans <input>, invisible pour les MDM) ─────────────
const _numpadStyle = {
  overlay: 'display:none;position:fixed;inset:0;z-index:9500;background:rgba(4,14,27,.92);align-items:center;justify-content:center',
  box:     'background:var(--card);border-radius:16px;padding:24px 20px;width:min(280px,calc(100vw - 40px));text-align:center;user-select:none',
  dots:    'font-size:26px;letter-spacing:10px;min-height:38px;margin:12px 0 4px;color:var(--text)',
  err:     'color:#e55;font-size:12px;min-height:16px;margin-bottom:8px',
  grid:    'display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px',
  key:     'padding:14px 0;border-radius:10px;border:1px solid rgba(255,255,255,.12);background:rgba(255,255,255,.07);color:var(--text);font-size:18px;font-weight:600;cursor:pointer',
  cancel:  'flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:transparent;color:var(--muted);cursor:pointer;font-size:13px',
  ok:      'flex:1;padding:10px;border-radius:8px;border:none;background:var(--gold);color:#000;font-weight:700;cursor:pointer;font-size:13px',
};

function _buildNumpad(containerId) {
  const wrap = document.createElement('div');
  wrap.id = containerId;
  wrap.style.cssText = _numpadStyle.overlay;
  wrap.innerHTML = `
    <div style="${_numpadStyle.box}">
      <div id="${containerId}-title"  style="font-size:15px;font-weight:600;color:var(--text);margin-bottom:2px"></div>
      <div id="${containerId}-sub"    style="font-size:12px;color:var(--muted);margin-bottom:4px"></div>
      <div id="${containerId}-dots"   style="${_numpadStyle.dots}">○○○○</div>
      <div id="${containerId}-err"    style="${_numpadStyle.err}"></div>
      <div style="${_numpadStyle.grid}" id="${containerId}-grid"></div>
      <div style="display:flex;gap:10px">
        <button id="${containerId}-cancel" style="${_numpadStyle.cancel}">Annuler</button>
        <button id="${containerId}-ok"     style="${_numpadStyle.ok}">Valider</button>
      </div>
      <div id="${containerId}-forgot" style="display:none;text-align:center;margin-top:10px">
        <button style="background:none;border:none;color:var(--muted);font-size:12px;cursor:pointer;text-decoration:underline;padding:4px">PIN oublié ?</button>
      </div>
    </div>`;
  const grid = wrap.querySelector(`#${containerId}-grid`);
  [1,2,3,4,5,6,7,8,9,'⌫',0,''].forEach(k => {
    const btn = document.createElement('button');
    btn.dataset.k = k;
    btn.style.cssText = k === '' ? 'visibility:hidden' : _numpadStyle.key;
    btn.textContent = k === '' ? '' : k;
    grid.appendChild(btn);
  });
  document.body.appendChild(wrap);
  return wrap;
}

function _runNumpad(id, { title, sub, maxLen = 8, onOk, onCancel, extraBtn, forgotFn }) {
  let val = '';
  const el     = document.getElementById(id);
  const dots   = el.querySelector(`#${id}-dots`);
  const err    = el.querySelector(`#${id}-err`);
  const btnOk  = el.querySelector(`#${id}-ok`);
  const btnCan = el.querySelector(`#${id}-cancel`);
  const grid   = el.querySelector(`#${id}-grid`);

  el.querySelector(`#${id}-title`).textContent = title || '';
  el.querySelector(`#${id}-sub`).textContent   = sub   || '';
  err.textContent = '';
  val = '';
  _updateDots();
  el.style.display = 'flex';

  if (extraBtn) {
    let eb = el.querySelector(`#${id}-extra`);
    if (!eb) {
      eb = document.createElement('button');
      eb.id = `${id}-extra`;
      eb.style.cssText = 'width:100%;margin-top:8px;padding:9px;border-radius:8px;border:1px solid rgba(255,80,80,.3);background:transparent;color:#e55;cursor:pointer;font-size:13px';
      // Insérer à l'intérieur de la carte (firstElementChild), avant le slot forgot
      const forgotEl = el.querySelector(`#${id}-forgot`);
      if (forgotEl) el.firstElementChild.insertBefore(eb, forgotEl);
      else el.firstElementChild.appendChild(eb);
    }
    eb.textContent = extraBtn.label;
    eb.onclick = extraBtn.action;
    eb.style.display = '';
  } else {
    const eb = el.querySelector(`#${id}-extra`);
    if (eb) eb.style.display = 'none';
  }

  // Lien "PIN oublié ?" — visible uniquement si forgotFn est fournie
  const forgotWrap = el.querySelector(`#${id}-forgot`);
  if (forgotWrap) {
    if (forgotFn) {
      forgotWrap.style.display = '';
      forgotWrap.querySelector('button').onclick = () => { el.style.display = 'none'; forgotFn(); };
    } else {
      forgotWrap.style.display = 'none';
    }
  }

  function _updateDots() {
    dots.textContent = val.length ? '●'.repeat(val.length) : '○○○○';
  }

  grid.onclick = e => {
    const k = e.target.dataset.k;
    if (k === undefined) return;
    err.textContent = '';
    if (k === '⌫') { val = val.slice(0, -1); }
    else if (val.length < maxLen) { val += k; }
    _updateDots();
  };

  btnOk.onclick  = () => { if (!val) { err.textContent = 'Saisis un code.'; return; } onOk(val, err, () => { val=''; _updateDots(); }); };
  btnCan.onclick = () => { el.style.display = 'none'; onCancel(); };
}

/**
 * Affiche un pavé numérique pour vérifier le PIN d'un profil.
 * Retourne une Promise<boolean>.
 */
function promptPinModal(userId) {
  return new Promise(resolve => {
    const ID = 'pin-numpad';
    if (!document.getElementById(ID)) _buildNumpad(ID);
    _runNumpad(ID, {
      title: '🔒 Profil protégé',
      sub:   'Entre ton code pour accéder à ce profil.',
      onCancel: () => resolve(false),
      onOk: async (pin, errEl, reset) => {
        try {
          const r = await fetch(`/api/users/${encodeURIComponent(userId)}/verify-pin`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ pin })
          });
          const { ok } = await r.json();
          if (ok) { document.getElementById(ID).style.display = 'none'; resolve(true); }
          else { errEl.textContent = 'Code incorrect.'; reset(); }
        } catch(e) { errEl.textContent = 'Erreur réseau.'; }
      },
      forgotFn: () => { _requestPinReset(userId); resolve(false); }
    });
  });
}

/**
 * Demande la réinitialisation du PIN par email.
 * Affiche un toast informatif selon le résultat.
 */
async function _requestPinReset(userId) {
  // Afficher un overlay d'attente simple
  const toastId = '_pin-reset-toast';
  let toast = document.getElementById(toastId);
  if (!toast) {
    toast = document.createElement('div');
    toast.id = toastId;
    toast.style.cssText = 'position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);z-index:9999;background:var(--card);border-radius:10px;padding:14px 20px;font-size:13px;color:var(--text);box-shadow:0 4px 20px rgba(0,0,0,.5);text-align:center;max-width:280px;display:none';
    document.body.appendChild(toast);
  }

  const show = (msg, color = 'var(--text)', ms = 4000) => {
    toast.style.color = color;
    toast.textContent = msg;
    toast.style.display = 'block';
    clearTimeout(toast._t);
    toast._t = setTimeout(() => { toast.style.display = 'none'; }, ms);
  };

  show('⏳ Envoi en cours…', 'var(--muted)', 30000);
  try {
    const r = await fetch(`/api/users/${encodeURIComponent(userId)}/request-pin-reset`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}'
    });
    const d = await r.json();
    if (d.ok) {
      show('✅ Email envoyé ! Vérifie ta boîte mail.', '#4c9', 6000);
    } else if (d.error === 'no_email') {
      show('⚠️ Aucun email enregistré pour ce profil. Contacte JC.', '#e5a', 6000);
    } else if (d.error === 'no_pin') {
      show('ℹ️ Ce profil n\'a pas de PIN.', 'var(--muted)', 4000);
    } else {
      show('❌ L\'envoi a échoué. Réessaie plus tard.', '#e55', 5000);
    }
  } catch(e) { show('❌ Erreur réseau.', '#e55', 5000); }
}

/**
 * Ouvre un pavé numérique pour définir (ou supprimer) le PIN d'un profil.
 * Etape 1 : saisie du nouveau code. Etape 2 : confirmation.
 */
function openSetPin(userId, userName) {
  return new Promise(resolve => {
    const ID = 'setpin-numpad';
    if (!document.getElementById(ID)) _buildNumpad(ID);
    let step1val = '';

    function showStep1() {
      _runNumpad(ID, {
        title: '🔐 Nouveau code',
        sub:   `Profil : ${userName}`,
        onCancel: () => resolve(false),
        extraBtn: { label: '✕ Supprimer le code existant', action: () => savePin('') },
        onOk: (val) => { step1val = val; showStep2(); }
      });
    }

    function showStep2() {
      _runNumpad(ID, {
        title: '🔐 Confirmer le code',
        sub:   'Saisis à nouveau le même code.',
        onCancel: () => resolve(false),
        onOk: async (val, errEl) => {
          if (val !== step1val) { errEl.textContent = 'Les codes ne correspondent pas.'; return; }
          savePin(val);
        }
      });
    }

    async function savePin(pin) {
      try {
        const r = await fetch(`/api/users/${encodeURIComponent(userId)}/set-pin`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-app-secret': _appSecret || '' },
          body: JSON.stringify({ pin })
        });
        document.getElementById(ID).style.display = 'none';
        if (r.ok) resolve(true); else resolve(false);
      } catch(e) { resolve(false); }
    }

    showStep1();
  });
}


// ─── Gestion des utilisateurs (JC only) ──────────────────────────────────────

async function openUserManagement() {
  const ID = 'user-mgmt-modal';
  let el = document.getElementById(ID);
  if (!el) {
    el = document.createElement('div');
    el.id = ID;
    el.style.cssText = 'display:none;position:fixed;inset:0;z-index:9000;background:rgba(4,14,27,.88);align-items:flex-end;justify-content:center;padding:20px';
    el.innerHTML = `
      <div style="background:var(--card);border-radius:16px;padding:24px 20px;width:100%;max-width:460px;max-height:82vh;overflow-y:auto">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:18px">
          <span style="font-size:15px;font-weight:700;color:var(--text)">👥 Gestion des utilisateurs</span>
          <button onclick="closeUserManagement()" style="background:none;border:none;color:var(--muted);font-size:20px;cursor:pointer;line-height:1">×</button>
        </div>
        <div id="user-mgmt-content"><p style="color:var(--muted);font-size:13px">Chargement…</p></div>
      </div>`;
    el.addEventListener('click', e => { if (e.target === el) closeUserManagement(); });
    document.body.appendChild(el);
  }
  el.style.display = 'flex';
  await _renderUserMgmt();
}

function closeUserManagement() {
  const el = document.getElementById('user-mgmt-modal');
  if (el) el.style.display = 'none';
}

async function _renderUserMgmt() {
  const el = document.getElementById('user-mgmt-content');
  if (!el) return;
  try {
    const [usersResp, statsResp] = await Promise.all([
      fetch('/api/users'),
      fetch('/api/userdata/stats'),
    ]);
    const usersList = usersResp.ok ? await usersResp.json() : [];
    const stats     = statsResp.ok ? await statsResp.json() : {};
    const fmtDate = iso => iso
      ? new Date(iso).toLocaleDateString('fr-FR') + ' ' +
        new Date(iso).toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' })
      : '—';
    const btnStyle = 'font-size:11px;padding:3px 10px;border-radius:6px;cursor:pointer;';
    const rows = usersList.map(u => {
      const p = stats[u.id] || {};
      const hasPin = u.hasPin;
      const pinBadge = hasPin
        ? `<span style="font-size:11px;color:#4c9;background:rgba(68,204,153,.12);padding:2px 7px;border-radius:10px">🔒 PIN actif</span>`
        : `<span style="font-size:11px;color:var(--muted);background:rgba(255,255,255,.06);padding:2px 7px;border-radius:10px">🔓 Sans PIN</span>`;
      const setPinBtn = `<button onclick="_umSetPin('${u.id}','${esc(u.name)}')" style="${btnStyle}border:1px solid rgba(255,255,255,.2);background:transparent;color:var(--muted)">✏️ ${hasPin ? 'Changer' : 'Définir'}</button>`;
      const delPinBtn = hasPin
        ? `<button onclick="_umDelPin('${u.id}','${esc(u.name)}')" style="${btnStyle}border:1px solid rgba(255,80,80,.3);background:transparent;color:#e55">✕ Suppr.</button>`
        : '';
      const editContactBtn = `<button onclick="_umEditContact('${u.id}','${esc(u.name)}')" style="${btnStyle}border:1px solid rgba(255,255,255,.2);background:transparent;color:var(--muted)">📋 Contact</button>`;
      const mobileVal = u.mobile || '';
      const emailVal  = u.email  || '';
      const mobileDisp = mobileVal
        ? `<a href="tel:${esc(mobileVal)}" style="color:#7ecfff;text-decoration:none;font-weight:600">${esc(mobileVal)}</a>`
        : '<span style="color:rgba(255,255,255,.2);font-style:italic">non renseigné</span>';
      const emailDisp  = emailVal
        ? `<a href="mailto:${esc(emailVal)}" style="color:#7ecfff;text-decoration:none;font-weight:600">${esc(emailVal)}</a>`
        : '<span style="color:rgba(255,255,255,.2);font-style:italic">non renseigné</span>';
      return `
      <div style="padding:14px 0;border-bottom:1px solid rgba(42,79,112,.3)">
        <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;margin-bottom:8px">
          <span style="font-size:14px;font-weight:600;color:var(--text)">${esc(u.name)}</span>
          <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap">${pinBadge}${setPinBtn}${delPinBtn}${editContactBtn}</div>
        </div>
        <div style="background:rgba(255,255,255,.04);border-radius:8px;padding:8px 10px;margin-bottom:8px;font-size:12px">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
            <span style="color:var(--muted);min-width:60px">📱 Mobile</span><span>${mobileDisp}</span>
          </div>
          <div style="display:flex;align-items:center;gap:8px">
            <span style="color:var(--muted);min-width:60px">✉️ Email</span><span style="word-break:break-all">${emailDisp}</span>
          </div>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:3px 12px;font-size:12px;color:var(--muted)">
          <span>Connexions</span><span style="color:var(--text)">${p.connectionCount ?? '—'}</span>
          <span>Dernière connexion</span><span style="color:var(--text)">${fmtDate(p.lastConnection)}</span>
          <span>🎬 Films vus</span><span style="color:var(--text)">${p.films?.vu ?? 0}</span>
          <span>🔖 Films à voir</span><span style="color:var(--text)">${p.films?.vouloir ?? 0}</span>
          <span>✕ Films non</span><span style="color:var(--text)">${p.films?.nonInteresse ?? 0}</span>
          <span>📺 Séries vues</span><span style="color:var(--text)">${p.series?.vu ?? 0}</span>
          <span>🔖 Séries à voir</span><span style="color:var(--text)">${p.series?.vouloir ?? 0}</span>
          <span>⏳ Séries à suivre</span><span style="color:var(--text)">${p.series?.asuivre ?? 0}</span>
          <span>✕ Séries non</span><span style="color:var(--text)">${p.series?.nonInteresse ?? 0}</span>
        </div>
      </div>`;
    }).join('');
    el.innerHTML = (rows || '<p style="color:var(--muted);font-size:13px">Aucun utilisateur.</p>') + `
      <div style="margin-top:20px;padding-top:16px;border-top:1px solid rgba(42,79,112,.3)">
        <button onclick="_umResetProfiles()" style="width:100%;padding:10px;border-radius:8px;border:1px solid rgba(255,200,0,.3);background:transparent;color:var(--gold);cursor:pointer;font-size:13px">
          🔄 Forcer la re-sélection du profil pour tous
        </button>
      </div>`;
  } catch(e) { el.innerHTML = '<p style="color:#e55;font-size:13px">Erreur de chargement.</p>'; }
}

async function _umEditContact(userId, userName) {
  // Supprimer l'ancienne instance pour reconstruire proprement
  const old = document.getElementById('contact-modal');
  if (old) old.remove();

  const el = document.createElement('div');
  el.id = 'contact-modal';
  el.style.cssText = 'display:none;position:fixed;inset:0;z-index:9500;background:rgba(4,14,27,.9);align-items:center;justify-content:center;padding:16px;box-sizing:border-box';

  const inputStyle = 'width:100%;box-sizing:border-box;padding:9px 12px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.07);color:var(--text);font-size:14px;outline:none';
  const labelStyle = 'font-size:11px;color:var(--muted);display:block;margin-bottom:3px;margin-top:10px';

  el.innerHTML = `
    <div style="background:var(--card);border-radius:14px;padding:24px 20px;width:100%;max-width:320px;max-height:90vh;overflow-y:auto">
      <div style="font-size:15px;font-weight:600;color:var(--text);margin-bottom:2px">📋 Coordonnées</div>
      <div id="cm-sub" style="font-size:12px;color:var(--muted);margin-bottom:14px"></div>

      <label style="${labelStyle}">📱 Mobile</label>
      <input id="cm-mob1" type="tel" autocomplete="off" maxlength="20" placeholder="ex : 06 12 34 56 78"
        style="${inputStyle}">
      <label style="${labelStyle}">📱 Confirmer le mobile</label>
      <input id="cm-mob2" type="tel" autocomplete="off" maxlength="20" placeholder="même numéro"
        style="${inputStyle}">

      <label style="${labelStyle};margin-top:16px">✉️ Email</label>
      <input id="cm-mail1" type="email" autocomplete="off" maxlength="80" placeholder="ex : prenom@example.com"
        style="${inputStyle}">
      <label style="${labelStyle}">✉️ Confirmer l'email</label>
      <input id="cm-mail2" type="email" autocomplete="off" maxlength="80" placeholder="même adresse"
        style="${inputStyle}">

      <div id="cm-err" style="color:#e55;font-size:12px;min-height:16px;margin-top:10px;margin-bottom:8px"></div>
      <div style="display:flex;gap:10px;margin-top:4px">
        <button id="cm-cancel" style="flex:1;padding:10px;border-radius:8px;border:1px solid rgba(255,255,255,.15);background:transparent;color:var(--muted);cursor:pointer;font-size:13px">Annuler</button>
        <button id="cm-ok" style="flex:1;padding:10px;border-radius:8px;border:none;background:var(--gold);color:#000;font-weight:700;cursor:pointer;font-size:13px">Enregistrer</button>
      </div>
    </div>`;

  el.addEventListener('click', e => { if (e.target === el) el.style.display = 'none'; });
  document.body.appendChild(el);

  // Pré-remplir avec les valeurs actuelles
  const users = await fetch('/api/users').then(r => r.json());
  const u = users.find(x => x.id === userId) || {};
  el.querySelector('#cm-sub').textContent  = `Profil : ${userName}`;
  el.querySelector('#cm-mob1').value  = u.mobile || '';
  el.querySelector('#cm-mob2').value  = u.mobile || '';
  el.querySelector('#cm-mail1').value = u.email  || '';
  el.querySelector('#cm-mail2').value = u.email  || '';
  el.querySelector('#cm-err').textContent = '';
  el.style.display = 'flex';
  setTimeout(() => el.querySelector('#cm-mob1').focus(), 50);

  const showErr = msg => { el.querySelector('#cm-err').textContent = msg; };

  el.querySelector('#cm-ok').onclick = async () => {
    const mob1  = el.querySelector('#cm-mob1').value.trim();
    const mob2  = el.querySelector('#cm-mob2').value.trim();
    const mail1 = el.querySelector('#cm-mail1').value.trim();
    const mail2 = el.querySelector('#cm-mail2').value.trim();
    showErr('');

    if (mob1 !== mob2)  return showErr('Les numéros de mobile ne correspondent pas.');
    if (mail1 !== mail2) return showErr('Les adresses email ne correspondent pas.');

    try {
      const r = await fetch(`/api/users/${encodeURIComponent(userId)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', 'x-app-secret': _appSecret || '' },
        body: JSON.stringify({ mobile: mob1, email: mail1 })
      });
      if (r.ok) { el.style.display = 'none'; await _renderUserMgmt(); }
      else { showErr('Erreur serveur.'); }
    } catch(e) { showErr('Erreur réseau.'); }
  };
  el.querySelector('#cm-cancel').onclick = () => { el.style.display = 'none'; };
}

async function _umSetPin(userId, userName) {
  await openSetPin(userId, userName);
  await _renderUserMgmt();
}

async function _umDelPin(userId, userName) {
  if (!confirm(`Supprimer le PIN de "${userName}" ?`)) return;
  try {
    await fetch(`/api/users/${encodeURIComponent(userId)}/set-pin`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-app-secret': _appSecret || '' },
      body: JSON.stringify({ pin: '' })
    });
    await _renderUserMgmt();
  } catch(e) { alert('Erreur lors de la suppression du PIN.'); }
}

async function _umResetProfiles() {
  if (!confirm('Forcer tous les utilisateurs à re-choisir leur profil à la prochaine visite ?')) return;
  try {
    const r = await fetch('/api/reset-profile-choice', {
      method: 'POST',
      headers: { 'x-app-secret': _appSecret || '' }
    });
    if (r.ok) alert('✓ Fait. Tous les utilisateurs devront re-sélectionner leur profil à la prochaine ouverture de l\'appli.');
    else alert('Erreur serveur.');
  } catch(e) { alert('Erreur réseau.'); }
}

/** Ouvre la modal de synchronisation des notes AlloCiné (bookmarklet) */
function openACSync() {
  const ID = 'ac-sync-modal';
  if (!document.getElementById(ID)) {
    const style = document.createElement('style');
    style.textContent = [
      `#ac-sync-modal{display:none;position:fixed;inset:0;z-index:9000;background:rgba(4,14,27,.85);align-items:flex-end;justify-content:center;padding:20px}`,
      `#ac-sync-modal.open{display:flex}`,
      `#ac-sync-modal .ac-sheet{background:#0f2540;border-radius:18px;padding:20px;width:100%;max-width:480px;max-height:80vh;overflow-y:auto}`,
      `#ac-sync-modal h3{color:var(--gold,#f0c040);font-size:15px;margin-bottom:16px}`,
      `#ac-sync-modal ol{color:var(--muted,#5a8fc0);font-size:12px;padding-left:18px;line-height:2.2;margin-top:12px}`,
      `#ac-sync-modal .ac-close{width:100%;margin-top:16px;padding:12px;border-radius:10px;background:var(--blue3,#1a3a5c);border:none;color:var(--text,#c8dcf0);font-size:14px;font-weight:600;cursor:pointer}`
    ].join('');
    document.head.appendChild(style);

    const modal = document.createElement('div');
    modal.id = ID;
    modal.onclick = e => { if (e.target === modal) modal.classList.remove('open'); };
    modal.innerHTML = `
      <div class="ac-sheet">
        <h3>🔄 Sync notes AlloCiné</h3>
        <p style="font-size:13px;color:var(--text,#c8dcf0);line-height:1.7;margin-bottom:8px">
          Glisse ce signet dans ta barre de favoris.<br>
          Fonctionne sur <strong style="color:var(--gold,#f0c040)">Mes films vus</strong>
          et <strong style="color:var(--gold,#f0c040)">Mes séries vues</strong> —
          il détecte automatiquement la page.
        </p>
        <div id="ac-bm-container" style="text-align:center;margin:20px 0"></div>
        <ol>
          <li>Glisse le bouton jaune dans ta barre de favoris</li>
          <li>Va sur <a href="https://mon.allocine.fr/mes-films/vus/" target="_blank"
              style="color:var(--gold,#f0c040)">Mes films vus</a>
              ou <a href="https://mon.allocine.fr/mes-series/vues/" target="_blank"
              style="color:var(--gold,#f0c040)">Mes séries vues</a></li>
          <li>Clique sur le signet "🔄 Sync VOD"</li>
        </ol>
        <button class="ac-close"
          onclick="document.getElementById('ac-sync-modal').classList.remove('open')">Fermer</button>
      </div>`;
    document.body.appendChild(modal);

    // Bookmarklet incrémental : détecte films ou séries selon la page AlloCiné
    const bm = '(async()=>{' +
      // Détecter si on est sur la page séries
      'const isSeries=window.location.href.includes(\'/mes-series/\');' +
      'const LS=isSeries?\'vod_ac_sync_marker_series\':\'vod_ac_sync_marker_films\';' +
      'const re=new RegExp(isSeries?\'ficheserie_gen_cserie=(\\\\d+)\':\'fichefilm_gen_cfilm=(\\\\d+)\');' +
      'const type=isSeries?\'série(s)\':\'film(s)\';' +
      'const marker=localStorage.getItem(LS);' +
      // Notification flottante
      'const n=m=>{let e=document.getElementById(\'_vod\');' +
      'if(!e){e=document.createElement(\'div\');e.id=\'_vod\';' +
      'Object.assign(e.style,{position:\'fixed\',top:\'20px\',right:\'20px\',' +
      'background:\'#0f2540\',color:\'#f0c040\',padding:\'12px 16px\',' +
      'borderRadius:\'8px\',zIndex:\'99999\',fontSize:\'13px\',' +
      'boxShadow:\'0 4px 12px rgba(0,0,0,.5)\'});' +
      'document.body.appendChild(e)}e.textContent=m};' +
      // Extrait l'allocineId d'une card (films ou séries selon re)
      'const getId=c=>{const sel=isSeries?\'a[href*="ficheserie"]\':\'a[href*="fichefilm"]\';' +
      'const l=c.querySelector(sel);' +
      'const m=(l?.getAttribute(\'href\')||\'\').match(re);' +
      'return m?m[1]:null};' +
      'n(marker?\'⏳ Sync incrémentale...\':\'⏳ Chargement complet...\');' +
      // Charger les pages jusqu'à trouver le marqueur (ou tout charger si première fois)
      'for(let i=0;i<80;i++){' +
      'if(marker&&[...document.querySelectorAll(\'.card-userspace\')].some(c=>getId(c)===marker))break;' +
      'const b=document.querySelector(\'.load-more-button\');' +
      'if(!b)break;' +
      'b.click();await new Promise(r=>setTimeout(r,1200))}' +
      // Extraire uniquement les entrées avant le marqueur
      'n(\'🔍 Extraction des notes...\');' +
      'const allCards=[...document.querySelectorAll(\'.card-userspace\')];' +
      'const markerIdx=marker?allCards.findIndex(c=>getId(c)===marker):-1;' +
      'const toProcess=markerIdx>=0?allCards.slice(0,markerIdx):allCards;' +
      'const f=[];toProcess.forEach(c=>{' +
      'const id=getId(c);' +
      'const s=c.querySelectorAll(\'.rating-star.active\').length;' +
      'if(id&&s)f.push({allocineId:id,noteAC:s/2})});' +
      // Déjà à jour
      'if(!f.length){n(\'✅ Déjà à jour !\');' +
      'setTimeout(()=>document.getElementById(\'_vod\')?.remove(),3000);return}' +
      // Envoyer
      'n(\'📤 Envoi de \'+f.length+\' \'+type+\'...\');' +
      'const r=await fetch(\'https://allocine-vod-production.up.railway.app/api/userdata/import-ac-notes\',' +
      '{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},' +
      'body:JSON.stringify({userId:\'user_default\',films:f,isSeries:isSeries})});' +
      'const d=await r.json();' +
      // Mettre à jour le marqueur = première entrée actuelle
      'const newMarker=getId(allCards[0]);' +
      'if(newMarker)localStorage.setItem(LS,newMarker);' +
      'n(\'✅ \'+d.imported+\' \'+type+\' synchronisé(s) !\');' +
      'setTimeout(()=>document.getElementById(\'_vod\')?.remove(),5000)' +
      '})()';
    const link = document.createElement('a');
    link.href = 'javascript:' + bm;
    link.textContent = '🔄 Sync VOD';
    Object.assign(link.style, {
      display: 'inline-block', background: 'var(--gold,#f0c040)', color: '#000',
      padding: '10px 24px', borderRadius: '8px', fontWeight: '700',
      fontSize: '14px', textDecoration: 'none', cursor: 'grab', userSelect: 'none',
      boxShadow: '0 2px 8px rgba(240,192,64,.3)'
    });
    document.getElementById('ac-bm-container').appendChild(link);
  }
  document.getElementById(ID).classList.add('open');
}

/** Nom du profil courant — mis à jour par updateDebugVisibility */
let _currentUserName = '';

/** Retourne true si le profil actif est JC (seul profil avec notes AlloCiné) */
function isJCProfile() {
  return _currentUserName.trim().toUpperCase() === 'JC';
}

/**
 * Retourne le HTML du badge de note AlloCiné.
 * N'afficher que si isJCProfile() === true.
 * La demi-étoile est simulée en CSS : étoile pleine rognée à 50% sur étoile vide.
 */
function renderNoteAC(noteAC) {
  if (!noteAC) return '';
  const full  = Math.floor(noteAC);
  const half  = (noteAC % 1) >= 0.5 ? 1 : 0;
  const empty = 5 - full - half;
  const fullStars  = '★'.repeat(full);
  const halfStar   = half
    ? '<span style="position:relative;display:inline-block">☆' +
        '<span style="position:absolute;left:0;top:0;overflow:hidden;width:50%">★</span>' +
      '</span>'
    : '';
  const emptyStars = '☆'.repeat(empty);
  return `<span class="ua-note-ac" title="Ma note AlloCiné">${fullStars}${halfStar}${emptyStars} <span class="ua-note-val">${noteAC}/5</span></span>`;
}

/** Masque le menu Debug + option de tri "Ma note" pour tous les profils sauf "JC" */
function updateDebugVisibility(userName) {
  _currentUserName = (userName || '').trim();
  const show = _currentUserName.toUpperCase() === 'JC';

  // Menu debug
  ['btn-debug-toggle', 'submenu-debug', 'debug-separator'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.style.display = show ? '' : 'none';
  });

  // Bouton gestion des utilisateurs (JC only)
  const btnUsr = document.getElementById('btn-user-mgmt');
  if (btnUsr) btnUsr.style.display = show ? '' : 'none';

  // Option de tri "↓ Ma note" — ajoutée uniquement pour JC
  const sortSel = document.getElementById('fil-sort');
  if (sortSel) {
    const existing = sortSel.querySelector('option[value="noteAC"]');
    if (show && !existing) {
      const opt = document.createElement('option');
      opt.value = 'noteAC';
      opt.textContent = '↓ Ma note';
      sortSel.appendChild(opt);
    } else if (!show && existing) {
      if (sortSel.value === 'noteAC') sortSel.value = 'presse';
      existing.remove();
    }
  }
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

// ─── Préférences ──────────────────────────────────────────────────────────────
let _prefs = { showDocumentaires: false, showAnimations: false, hideVus: true, hideNonInteresse: true };

function getPrefsKey() { return 'vod_prefs_' + (_currentUserId || 'anon'); }

function loadPrefs() {
  _prefs = { showDocumentaires: false, showAnimations: false, hideVus: true, hideNonInteresse: true };
  try { Object.assign(_prefs, JSON.parse(localStorage.getItem(getPrefsKey()) || '{}')); } catch(e) {}
  loadServerPrefs().then(serverPrefs => {
    if (!serverPrefs || Object.keys(serverPrefs).length === 0) return;
    Object.assign(_prefs, serverPrefs);
    localStorage.setItem(getPrefsKey(), JSON.stringify(_prefs));
    applyFilters();
  });
}
function savePrefs() {
  localStorage.setItem(getPrefsKey(), JSON.stringify(_prefs));
  saveServerPrefs(_prefs);
}
function setPref(key, val) { _prefs[key] = val; savePrefs(); applyFilters(); }

function openPrefs() { renderPrefs(); document.getElementById('prefs-modal').classList.add('open'); }
function closePrefs() { document.getElementById('prefs-modal').classList.remove('open'); }
function renderPrefs() {
  const row = (key, label) => `
    <div class="pref-row">
      <div class="pref-label">${label}</div>
      <label class="toggle">
        <input type="checkbox" ${_prefs[key] ? 'checked' : ''} onchange="setPref('${key}', this.checked)">
        <span class="toggle-slider"></span>
      </label>
    </div>`;
  document.getElementById('prefs-content').innerHTML = `
    <p style="font-size:12px;color:var(--muted);margin-bottom:16px;line-height:1.5">Ces réglages s'appliquent aux 3 sections : Films, Séries et Best ever.</p>
    <div class="prefs-section">
      <div class="prefs-section-title">Genres</div>
      ${row('showDocumentaires', 'Afficher les documentaires')}
      ${row('showAnimations',    'Afficher les animations')}
    </div>
    <div class="prefs-section">
      <div class="prefs-section-title">Ma liste</div>
      ${row('hideVus',          'Masquer les déjà vus')}
      ${row('hideNonInteresse', 'Masquer les "Non intéressé"')}
    </div>`;
}

// ─── Base de données utilisateur ──────────────────────────────────────────────
let _connectionPinged = false;

async function loadUserdata() {
  if (!_currentUserId) return;
  try {
    const r = await fetch(`/api/userdata?userId=${encodeURIComponent(_currentUserId)}`);
    _userdata = await r.json();
    UI.reapplyUserActions();
    UI.applyFilters();
  } catch(e) { console.warn('Erreur chargement userdata:', e.message); }
  // Ping connexion une seule fois par session
  if (!_connectionPinged) {
    _connectionPinged = true;
    fetch(`/api/users/${encodeURIComponent(_currentUserId)}/ping`, { method: 'POST' }).catch(() => {});
  }
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
    if (_loadGen !== gen) { if (needsFetch) UI.onPlatDone(errors); return; }

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
      if (_loadGen !== gen) { if (needsFetch) UI.onPlatDone(errors); return; }
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

  // Si tri par noteAC et _userdata ne contient pas encore de notes → recharger
  // (peut arriver si la page était ouverte avant l'import des notes)
  if (_sortBy === 'noteAC') {
    const hasNotes = Object.values(_userdata).some(u => u.noteAC);
    if (!hasNotes) {
      loadUserdata().then(() => applySort());
      return;
    }
  }

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
    if (_sortBy === 'combinee') {
      const sa = (a.notePresse ?? 0) + (a.noteSpect ?? 0);
      const sb = (b.notePresse ?? 0) + (b.noteSpect ?? 0);
      return sb - sa || (b.notePresse ?? 0) - (a.notePresse ?? 0) || a.titre.localeCompare(b.titre, 'fr');
    }
    if (_sortBy === 'noteAC') {
      // Les notes vont de 0.5 à 5 → sans note = 0, toujours en dessous
      const na = _userdata[a.allocineId]?.noteAC || 0;
      const nb = _userdata[b.allocineId]?.noteAC || 0;
      return (nb - na) || (b.notePresse - a.notePresse);
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

// ─────────────────────────────────────────────────────────────────────────────
// Modal ℹ Info — commun aux 3 pages (Films / Séries / Best ever)
// ─────────────────────────────────────────────────────────────────────────────
let _infoData         = null;
let _infoRefreshTimer = null;

async function _fetchInfoData() {
  try {
    const [rf, rs, rb, rst, rstat] = await Promise.all([
      fetch('/api/health',           { signal: AbortSignal.timeout(4000) }),
      fetch('/api/series/health',    { signal: AbortSignal.timeout(4000) }),
      fetch('/api/bestever/health',  { signal: AbortSignal.timeout(4000) }),
      fetch('/api/scraping-status',  { signal: AbortSignal.timeout(4000) }),
      fetch('/api/userdata/stats',   { signal: AbortSignal.timeout(4000) }),
    ]);
    _infoData = {
      films:    rf.ok    ? await rf.json()    : null,
      series:   rs.ok    ? await rs.json()    : null,
      bestever: rb.ok    ? await rb.json()    : null,
      status:   rst.ok   ? await rst.json()   : null,
      udStats:  rstat.ok ? await rstat.json() : null,
    };
  } catch(e) { _infoData = null; }
}

function _startInfoRefresh() {
  _stopInfoRefresh();
  _infoRefreshTimer = setInterval(async () => {
    if (!document.getElementById('info-modal')?.classList.contains('open')) { _stopInfoRefresh(); return; }
    if (!_infoData?.status?.phase) return;
    try {
      const r = await fetch('/api/scraping-status', { signal: AbortSignal.timeout(3000) });
      if (r.ok) { _infoData.status = await r.json(); renderInfo(); }
    } catch(e) {}
  }, 3000);
}
function _stopInfoRefresh() {
  if (_infoRefreshTimer) { clearInterval(_infoRefreshTimer); _infoRefreshTimer = null; }
}

async function openInfo() {
  document.getElementById('info-modal').classList.add('open');
  document.getElementById('info-content').innerHTML = 'Chargement…';
  await _fetchInfoData();
  renderInfo();
  _startInfoRefresh();
}
function closeInfo() {
  document.getElementById('info-modal').classList.remove('open');
  _stopInfoRefresh();
}

function renderInfo() {
  const el = document.getElementById('info-content');
  if (!_infoData) { el.textContent = 'Serveur non disponible.'; return; }
  const fmt = iso => iso
    ? new Date(iso).toLocaleDateString('fr-FR') + ' ' + new Date(iso).toLocaleTimeString('fr-FR', {hour:'2-digit', minute:'2-digit'})
    : '—';
  const f = _infoData.films, s = _infoData.series, b = _infoData.bestever, st = _infoData.status;
  const udStats = _infoData.udStats || {};
  const fErrors = f?.lastScrapeErrors || [];

  function progressBlock(info) {
    if (!info?.active) return '';
    const pct   = info.pct ?? 0;
    const extra = info.annee ? ` · ${info.annee}` : (info.total ? ` · ${info.total} entrées` : '');
    return `<div class="info-progress">
      <div class="info-progress-label"><span>En cours${extra}</span><span>${pct}%</span></div>
      <div class="info-progress-bar-wrap"><div class="info-progress-bar" style="width:${pct}%"></div></div>
    </div>`;
  }
  function badge(active) {
    return active ? '<span class="info-scraping-badge">⟳ en cours</span>' : '';
  }

  // Compteurs notes AlloCiné (profil JC uniquement, calculé depuis _userdata local)
  const notesBlock = (() => {
    if (!isJCProfile()) return '';
    const filmsAvecNote  = Object.entries(_userdata).filter(([k, v]) => !k.startsWith('s:') && v.noteAC).length;
    const seriesAvecNote = Object.entries(_userdata).filter(([k, v]) =>  k.startsWith('s:') && v.noteAC).length;
    return `
    <div class="info-section-title">⭐ Mes notes AlloCiné</div>
    <div class="info-row"><span class="lbl">Films notés</span><span class="val">${filmsAvecNote}</span></div>
    <div class="info-row"><span class="lbl">Séries notées</span><span class="val">${seriesAvecNote}</span></div>`;
  })();

  // Bloc stats profil courant (tous les profils sauf JC)
  const myStatsBlock = (() => {
    if (isJCProfile() || !_currentUserId) return '';
    const p = udStats[_currentUserId];
    if (!p) return '';
    return `
    <div class="info-section-title">👤 Mon profil</div>
    <div class="info-row"><span class="lbl">🎬 Films vus</span><span class="val">${p.films.vu}</span></div>
    <div class="info-row"><span class="lbl">🔖 Films à voir</span><span class="val">${p.films.vouloir}</span></div>
    <div class="info-row"><span class="lbl">✕ Films non</span><span class="val">${p.films.nonInteresse}</span></div>
    <div class="info-row"><span class="lbl">📺 Séries vues</span><span class="val">${p.series.vu}</span></div>
    <div class="info-row"><span class="lbl">🔖 Séries à voir</span><span class="val">${p.series.vouloir}</span></div>
    <div class="info-row"><span class="lbl">⏳ Séries à suivre</span><span class="val">${p.series.asuivre}</span></div>
    <div class="info-row"><span class="lbl">✕ Séries non</span><span class="val">${p.series.nonInteresse}</span></div>`;
  })();

  el.innerHTML = `
    ${notesBlock}
    <div class="info-section-title">Serveur</div>
    <div class="info-row"><span class="lbl">Version</span><span class="val">${esc(f?.version || s?.version || b?.version || '—')}</span></div>
    <div class="info-row"><span class="lbl">Démarré</span><span class="val">${fmt(f?.serverStart)}</span></div>
    <div class="info-section-title">🎬 Films récents</div>
    <div class="info-row"><span class="lbl">En base</span><span class="val">${f?.cachedFilms ?? '—'} films</span></div>
    <div class="info-row"><span class="lbl">Scraping liste${badge(st?.filmsList?.active)}</span><span class="val">${fmt(f?.lastScrape)}</span></div>
    ${progressBlock(st?.filmsList)}
    <div class="info-row"><span class="lbl">Scraping plateformes${badge(st?.filmsDetails?.active)}</span><span class="val">${fmt(f?.lastDetailsScrape)}</span></div>
    ${progressBlock(st?.filmsDetails)}
    ${fErrors.length > 0 ? `<div class="info-row"><span class="lbl">Erreurs</span><span class="val">⚠️ ${fErrors.length}</span></div><div class="info-errors">${fErrors.map(e => `Page ${e.page} (${e.annee}) : ${esc(e.message)}`).join('<br>')}</div>` : ''}
    <div class="info-section-title">📺 Séries</div>
    <div class="info-row"><span class="lbl">En base</span><span class="val">${s?.cachedSeries ?? '—'} séries</span></div>
    <div class="info-row"><span class="lbl">Fiches détails</span><span class="val">${s?.cachedDetails ?? '—'} en mémoire</span></div>
    <div class="info-row"><span class="lbl">Scraping liste${badge(st?.seriesList?.active)}</span><span class="val">${fmt(s?.lastScrape)}</span></div>
    ${progressBlock(st?.seriesList)}
    <div class="info-row"><span class="lbl">Scraping détails${badge(st?.seriesDetails?.active)}</span><span class="val">${fmt(s?.lastDetailsScrape)}</span></div>
    ${progressBlock(st?.seriesDetails)}
    <div class="info-section-title">🏆 Best ever</div>
    <div class="info-row"><span class="lbl">En base</span><span class="val">${b?.cachedFilms ?? '—'} films</span></div>
    <div class="info-row"><span class="lbl">Pages / décennie</span><span class="val">${b?.pagesPerDecade ?? '—'} × ${b?.decades?.length ?? '—'} décennies</span></div>
    <div class="info-row"><span class="lbl">Scraping liste${badge(st?.besteverList?.active)}</span><span class="val">${fmt(b?.lastScrape)}</span></div>
    ${progressBlock(st?.besteverList)}
    <div class="info-row"><span class="lbl">Scraping plateformes${badge(st?.besteverDetails?.active)}</span><span class="val">${fmt(b?.lastDetailsScrape)}</span></div>
    ${progressBlock(st?.besteverDetails)}
    ${myStatsBlock}
  `;
}
