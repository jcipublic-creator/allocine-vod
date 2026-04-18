const CACHE = `vod-shell-${new Date().toISOString().slice(0,16).replace(/[-:T]/g,'')}`;

const SHELL = [
  '/mobile.html',
  '/manifest.json',
  '/icon-192.svg',
];

// Installation : mise en cache de l'app shell
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

// Fetch : app shell depuis le cache, API depuis le réseau
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Les appels API passent toujours par le réseau
  if (url.pathname.startsWith('/api/')) return;

  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      // Pas en cache → réseau, puis mise en cache
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
