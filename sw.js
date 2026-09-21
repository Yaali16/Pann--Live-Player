// Pann -- app-shell service worker
// ---------------------------------------------------------------------
// This exists for exactly one reason: Chrome/Android only offers the
// "Install app" prompt when a service worker with a fetch handler is
// controlling the page. It does NOT try to fix gateway headers or cache
// audio -- an earlier version of this file did that, and intercepting
// the audio/image requests turned out to be unreliable and hard to
// verify, so that approach was dropped entirely (see script.js's
// fetchAudioBlob, which loads audio as plain whole-file fetches with no
// service worker involved). This file only ever touches the small,
// fixed set of files that make up the page itself, so the player keeps
// working offline once it's been opened before -- it never intercepts
// anything going to the IPFS gateways.
const CACHE_NAME = 'pann-shell-v1';
const APP_SHELL = [
    './',
    'index.html',
    'style.css',
    'script.js',
    'data.js',
    'manifest.json',
    'icon-192.png',
    'icon-512.png',
    'covers/strings.jpg',
    'covers/winds.jpg',
    'covers/ambience.jpg',
    'covers/rhythm.jpg',
    'covers/traditional.jpg',
    'covers/voices.jpg',
    'covers/guitars.jpg',
    'covers/keys.jpg',
    'covers/electronic.jpg'
];

self.addEventListener('install', (event) => {
    self.skipWaiting();
    event.waitUntil(
        caches.open(CACHE_NAME)
            .then((cache) => cache.addAll(APP_SHELL))
            .catch(() => { /* offline caching is a nice-to-have, never block install on it */ })
    );
});

self.addEventListener('activate', (event) => {
    event.waitUntil(
        caches.keys()
            .then((keys) => Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k))))
            .then(() => self.clients.claim())
    );
});

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);
    // Only ever serve same-origin GETs for the fixed app-shell list above.
    // Everything else -- and in particular every audio/image request to
    // pann.mypinata.cloud or any fallback gateway -- passes straight
    // through untouched, exactly as if no service worker existed.
    if (event.request.method !== 'GET' || url.origin !== self.location.origin) return;
    event.respondWith(
        caches.match(event.request).then((cached) => cached || fetch(event.request))
    );
});
