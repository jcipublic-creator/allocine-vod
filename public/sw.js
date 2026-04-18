const CACHE = `vod-shell-v62`;

const SHELL = [
  '/manifest.json',
  '/icon-192.svg',
];

// Fichiers JS toujours rechargés depuis le réseau (comme les HTML)
const NETWORK_FIRST = ['/shared.js'];

// Installation : mise en cache des assets statiques (pas mobile.html)
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(SHELL))
  );
  self.skipWaiting();
});

// Activation : supprime les anciens caches
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Les appels API passent toujours par le réseau
  if (url.pathname.startsWith('/api/')) return;

  // Les fichiers HTML et JS partagés : réseau en priorité, cache en fallback (offline)
  if (url.pathname.endsWith('.html') || url.pathname === '/' || NETWORK_FIRST.includes(url.pathname)) {
    e.respondWith(
      fetch(e.request)
        .then(resp => {
          if (resp.ok) {
            const clone = resp.clone();
            caches.open(CACHE).then(c => c.put(e.request, clone));
          }
          return resp;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // Autres assets : cache en priorité, réseau en fallback
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(resp => {
        if (resp.ok) {
          const clone = resp.clone();
          caches.open(CACHE).then(c => c.put(e.request, clone));
        }
        return resp;
      });
    })
  );
});
