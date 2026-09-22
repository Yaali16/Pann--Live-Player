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
//
// The fetch handler below is deliberately network-FIRST, not cache-first.
// An earlier version served the cached app shell before ever touching the
// network, which is what "installable offline" usually means -- but it
// also meant that once a phone had this page open once, every css/js/html
// fix pushed afterward was invisible on that phone forever: the browser
// only re-checks sw.js itself for changes (byte-for-byte), and this file's
// own bytes don't change just because index.html/style.css/script.js did,
// so the old service worker kept quietly serving its old cached copies of
// those files on every visit, install prompts and all. Network-first
// fixes that -- every visit with a connection gets the live files, and the
// cache is only ever a fallback for the rare case of no network at all.
const CACHE_NAME = 'pann-shell-v2';
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
        fetch(event.request)
            .then((response) => {
                // Keep the offline fallback cache up to date with whatever
                // the network just served, so offline mode never lags too
                // far behind what people have actually been using.
                const copy = response.clone();
                caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy)).catch(() => {});
                return response;
            })
            .catch(() => caches.match(event.request))
    );
});
