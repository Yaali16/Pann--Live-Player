(async function () {
    // ---------------------------------------------------------------------
    // Asset source resolution
    // ---------------------------------------------------------------------
    // Every stem/frame is fetched straight from IPFS via a dedicated Pinata
    // gateway (pann.mypinata.cloud) instead of the Audio/Visuals folders
    // this player originally shipped with. The gateway is in "restricted"
    // mode -- it only serves CIDs actually pinned to this account's PANN
    // group -- so if a layer never loads, that CID likely isn't pinned yet.
    // `data.js` (loaded before this file) supplies every CID, grouped by
    // layer name and variant index; PANN_ASSETS below is merged with it at
    // startup in mergeCidsIntoAssets().
    const IPFS_GATEWAY = (typeof PANN_DATA !== "undefined" && PANN_DATA.gateway) || "https://pann.mypinata.cloud/ipfs/";
    // Primary is the dedicated gateway (fast, no rate limit, but restricted
    // -- only serves CIDs actually pinned to this account). Public gateways
    // are kept only as a fallback for a CID that times out there (not yet
    // pinned, or stuck in the pin queue) -- loadResilient below only moves
    // on to these after the primary candidate times out or 4xx/5xxs.
    function buildCandidates(cid) {
        if (!cid) return [];
        return [
            IPFS_GATEWAY + cid,
            `https://gateway.pinata.cloud/ipfs/${cid}`,
            `https://ipfs.io/ipfs/${cid}`
        ];
    }
    function url(cid) {
        return cid ? IPFS_GATEWAY + cid : "";
    }

    // ---------------------------------------------------------------------
    // Load concurrency gate
    // ---------------------------------------------------------------------
    // Originally capped at 4 because streaming <audio src> could issue
    // dozens of small range sub-requests per file, and firing too many of
    // those at once onto one gateway connection could exceed its HTTP/2
    // concurrent-stream limit. Audio no longer streams -- it's one plain
    // fetch() per stem (see fetchAudioBlob) -- so that failure mode is
    // gone, and there are at most 9 stems total; letting all of them run
    // at once is safe and finishes sooner (each one otherwise pays its
    // own connection ramp-up cost in series). Cover art is now bundled
    // locally too, so this gate is only audio blob fetches plus the
    // occasional gallery/lightbox artwork image.
    const MAX_CONCURRENT_LOADS = 9;
    let activeLoads = 0;
    const loadQueue = [];
    function acquireLoadSlot() {
        return new Promise((resolve) => {
            const tryGo = () => {
                if (activeLoads < MAX_CONCURRENT_LOADS) { activeLoads++; resolve(); }
                else loadQueue.push(tryGo);
            };
            tryGo();
        });
    }
    function releaseLoadSlot() {
        activeLoads--;
        const next = loadQueue.shift();
        if (next) next();
    }

    // Assigns candidates to a media/img element and cascades through them
    // on error OR on a stall timeout (some gateways hang instead of
    // erroring), calling onReady once one actually loads, or onGiveUp if
    // every candidate fails. The whole attempt (including its candidate
    // fallbacks) holds one concurrency slot from start to finish.
    function loadResilient(el, candidates, { isAudio = false, timeoutMs = 10000, onReady, onGiveUp } = {}) {
        let idx = -1;
        let timer = null;
        let slotHeld = false;

        function cleanup() {
            if (timer) { clearTimeout(timer); timer = null; }
        }
        function releaseIfHeld() {
            if (slotHeld) { slotHeld = false; releaseLoadSlot(); }
        }

        function succeed() {
            cleanup();
            el.onerror = null;
            if (isAudio) el.oncanplay = null; else el.onload = null;
            releaseIfHeld();
            if (onReady) onReady();
        }

        function tryNext() {
            cleanup();
            idx++;
            if (idx >= candidates.length) {
                el.onerror = null;
                if (isAudio) el.oncanplay = null; else el.onload = null;
                console.error("[Pann] All sources failed:", candidates);
                // Actually abort the element instead of leaving its src
                // pointed at a URL that keeps erroring -- otherwise the
                // browser's own media-fetch retry logic (below our
                // onerror/timeout hooks entirely) can keep hammering that
                // same URL indefinitely, especially on a looping <audio>
                // element still nominally "wanting" to play.
                el.removeAttribute('src');
                if (isAudio) el.load();
                releaseIfHeld();
                if (onGiveUp) onGiveUp();
                return;
            }
            const url = candidates[idx];
            el.onerror = tryNext;
            if (isAudio) el.oncanplay = succeed; else el.onload = succeed;
            el.src = url;
            if (isAudio) el.load();
            if (timeoutMs) {
                timer = setTimeout(() => {
                    console.warn(`[Pann] Timed out loading ${url}, trying next source...`);
                    tryNext();
                }, timeoutMs);
            }
        }

        acquireLoadSlot().then(() => {
            slotHeld = true;
            tryNext();
        });
    }

    // ---------------------------------------------------------------------
    // Audio: whole-file fetch, cached per CID
    // ---------------------------------------------------------------------
    // pann.mypinata.cloud (and its fallbacks) send the wrong content-type
    // on every range request after a file's first, which broke streaming
    // <audio src> playback -- most visibly as a freeze/loop right after
    // seeking, since a seek always issues a fresh range request. Fetching
    // each variant whole with a plain fetch() has no range requests, so
    // the bug never triggers, and once loaded, seeking is purely local
    // (no network at all). Caching the resulting blob URL by CID means a
    // variant is only ever downloaded once per session -- repeat
    // shuffles back to an already-played variant cost nothing.
    // Reads a fetch() response's body as a stream instead of res.blob(), so
    // real byte progress (0..1) can be reported as chunks arrive -- this is
    // what drives the actual 0-100% fill on each tile and the overlay bar,
    // instead of guessing. Falls back to a plain await-the-whole-thing read
    // if the server doesn't send Content-Length or streaming isn't
    // available. `onProgress` resets a stall timer on every chunk, so a
    // connection that's still receiving data (just slow) is never killed,
    // but one that's gone completely silent is aborted after
    // `stallTimeoutMs` instead of hanging forever.
    async function fetchBlobAsObjectUrl(src, onProgress, stallTimeoutMs = 20000) {
        const controller = new AbortController();
        let timer;
        const resetTimer = () => {
            clearTimeout(timer);
            timer = setTimeout(() => controller.abort(), stallTimeoutMs);
        };
        resetTimer();
        try {
            const res = await fetch(src, { signal: controller.signal });
            if (!res.ok) throw new Error(`${res.status} on ${src}`);
            const total = Number(res.headers.get('content-length')) || 0;
            if (!res.body || !total || !res.body.getReader) {
                const blob = await res.blob();
                clearTimeout(timer);
                if (onProgress) onProgress(1);
                return URL.createObjectURL(blob);
            }
            const reader = res.body.getReader();
            const chunks = [];
            let received = 0;
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                resetTimer();
                chunks.push(value);
                received += value.length;
                // Capped below 1 here on purpose: Content-Length can
                // describe a compressed size while the bytes we receive
                // are the decompressed ones (the browser gunzips
                // transparently), so received/total can reach or pass 1
                // before the stream is actually finished. Reporting 100%
                // early looked exactly like "the bar says done but it's
                // still loading" -- so 1 is only ever reported once the
                // read loop genuinely ends (below), never from this math.
                if (onProgress) onProgress(Math.min(0.99, received / total));
            }
            clearTimeout(timer);
            if (onProgress) onProgress(1); // now it's actually, truly done
            return URL.createObjectURL(new Blob(chunks));
        } finally {
            clearTimeout(timer);
        }
    }

    // Per-layer generation counter: whenever a layer's selected variant
    // changes again before the previous change has finished loading, the
    // in-flight (now stale) load must never win against the newer one --
    // whichever finishes downloading/decoding LAST used to simply
    // overwrite whatever the other one had already applied, so a layer
    // cycled 3 times quickly could end up playing (and previewing) an
    // earlier click instead of the latest one, purely by network timing.
    // Bumping this per layer.id and having every in-flight task check
    // "is my token still current?" before touching shared UI/audio state
    // makes the latest change always win, regardless of completion order.
    const layerToken = {};
    function bumpLayerToken(id) {
        layerToken[id] = (layerToken[id] || 0) + 1;
        return layerToken[id];
    }

    const audioBlobCache = new Map(); // cid -> Promise<objectURL>
    // `onProgress` only fires for the caller that actually triggers the
    // fetch (a cache miss); a second caller awaiting an already-in-flight
    // or already-cached fetch just sees it jump straight to 1 -- fine in
    // practice since prefetch usually wins the race before Play is even
    // pressed.
    function fetchAudioBlob(cid, onProgress) {
        if (!cid) return Promise.reject(new Error("no cid"));
        if (audioBlobCache.has(cid)) {
            if (onProgress) onProgress(1);
            return audioBlobCache.get(cid);
        }

        const promise = acquireLoadSlot().then(async () => {
            try {
                let lastErr;
                for (const src of buildCandidates(cid)) {
                    try {
                        return await fetchBlobAsObjectUrl(src, onProgress);
                    } catch (e) {
                        lastErr = e;
                    }
                }
                throw lastErr || new Error(`All sources failed for ${cid}`);
            } finally {
                releaseLoadSlot();
            }
        });
        // On failure, drop the cache entry so a later retry (e.g. next
        // shuffle back to this variant) gets a fresh attempt instead of
        // being stuck on a cached rejection.
        promise.catch(() => audioBlobCache.delete(cid));
        audioBlobCache.set(cid, promise);
        return promise;
    }

    // ---------------------------------------------------------------------
    // Layer / variant data
    // ---------------------------------------------------------------------
    // `subtitle` is the poetic name Async Art gave each layer on its own
    // NFT listing (e.g. "the continuum" for Strings) -- confirmed by the
    // project owner against the full 9-layer OpenSea listing.
    // `credit` on each variant is the real performer name(s), sourced from
    // the layer's official Async Art / OpenSea description, or confirmed
    // directly by the project owner (Traditional/Veena -- Ramana
    // Balachandhran). Winds/Penny Whistle had no named performer in either
    // source, so it's left uncredited rather than guessed.
    // `dataName` is the layer name as it appears in data.js/tokenURI
    // metadata (used to look up CIDs by index -- see mergeCidsIntoAssets).
    // audioCid/visualCid on each variant, and coverCid on each layer, are
    // filled in at startup rather than hardcoded here, so this block stays
    // the single source of truth for names/subtitles/credits and data.js
    // stays the single source of truth for CIDs.
    const PANN_ASSETS = [
        {
            id: "strings", name: "Strings", subtitle: "the continuum", dataName: "Strings",
            variants: [
                { label: "Bright", credit: "Rithu Vysakh" },
                { label: "Dark", credit: "Rithu Vysakh" },
                { label: "Ambient", credit: "Rithu Vysakh" }
            ]
        },
        {
            id: "winds", name: "Winds", subtitle: "the seasons", dataName: "Winds",
            variants: [
                { label: "Bamboo Flute", credit: "Nikhil Ram" },
                { label: "Penny Whistle", credit: "" },
                { label: "Melodica", credit: "M S Yeshwanth" },
                { label: "Nadaswaram", credit: "Mylai M Karthikeyan" }
            ]
        },
        {
            id: "ambience", name: "Ambience", subtitle: "sound of the land", dataName: "Ambience",
            variants: [
                { label: "Kurunji", credit: "Manoj Y D & Pravekha" },
                { label: "Mullai", credit: "Manoj Y D & Pravekha" },
                { label: "Marudham", credit: "Manoj Y D & Pravekha" },
                { label: "Neidhal", credit: "Manoj Y D & Pravekha" },
                { label: "Paalai", credit: "Manoj Y D & Pravekha" }
            ]
        },
        {
            id: "rhythm", name: "Rhythm", subtitle: "the medium", dataName: "Rhythm",
            variants: [
                { label: "Mridangam & Latin", credit: "Praveen Sparsh & Kanaxx" },
                { label: "Acoustic Drums", credit: "Tapass Naresh" },
                { label: "Folk", credit: "Praveen Sparsh & Kanaxx" }
            ]
        },
        {
            id: "traditional", name: "Traditional Melody", subtitle: "the soul", dataName: "Traditional",
            variants: [
                { label: "Sarangi", credit: "Manonmani" },
                { label: "Veena", credit: "Ramana Balachandhran" },
                { label: "Slide Guitar", credit: "Pradeep Kumar (live at Dreamverse)" }
            ]
        },
        {
            id: "voices", name: "Voices", subtitle: "the spirit", dataName: "Voices",
            variants: [
                { label: "Solo", credit: "Pradeep Kumar, Susha & Kalyani Nair" },
                { label: "Folk Voice", credit: "Anthony Daasan" },
                { label: "Choir", credit: "16-piece choir" }
            ]
        },
        {
            id: "guitars", name: "Guitar & Bass", subtitle: "the force", dataName: "Guitars",
            variants: [
                { label: "Acoustic", credit: "Keba Jeremiah, Shallu Varun, M S Yeshwanth & Jhanu" },
                { label: "Electric", credit: "Keba Jeremiah, Shallu Varun, M S Yeshwanth & Jhanu" }
            ]
        },
        {
            id: "keys", name: "Keys", subtitle: "the sparkle", dataName: "Keys",
            variants: [
                { label: "Piano", credit: "M S Yeshwanth" },
                { label: "Mallet", credit: "M S Yeshwanth" }
            ]
        },
        {
            id: "electronic", name: "Electronic", subtitle: "the moment", dataName: "Electronic",
            variants: [
                { label: "Synth and Bass", credit: "Bharath Sankar" },
                { label: "Modular", credit: "Amrit" },
                { label: "Live Reactive Layer", credit: "Radar with a K & Aarvay" }
            ]
        }
    ];

    // Fills audioCid/visualCid (per variant) and coverCid (per layer, = its
    // first variant's visual CID -- there's no separate dedicated cover
    // photo pinned, unlike the old local-asset cover images) from data.js.
    // Runs once at startup, before anything tries to render or load media.
    function mergeCidsIntoAssets() {
        PANN_ASSETS.forEach(layer => {
            const src = (typeof PANN_DATA !== "undefined" ? PANN_DATA.layers : []).find(l => l.name === layer.dataName);
            if (!src) {
                console.error(`[Pann] No CID data found for layer "${layer.name}" (looked for "${layer.dataName}" in data.js)`);
                return;
            }
            layer.variants.forEach((variant, i) => {
                variant.audioCid = src.audio[i] ? src.audio[i].cid : null;
                variant.visualCid = src.visual[i] ? src.visual[i].cid : null;
                if (!variant.audioCid || !variant.visualCid) {
                    console.warn(`[Pann] Missing CID for ${layer.name} / ${variant.label}`);
                }
            });
            layer.coverCid = layer.variants[0] ? layer.variants[0].visualCid : null;
            layer.coverLocal = `covers/${layer.id}.jpg`;
        });
    }

    const ARTISTS_LIST = [
        "Pradeep Kumar", "Anthony Daasan", "Kalyani Nair", "Susha", "Ghana NB",
        "Vidhya Vijay", "Sujith Sreedhar", "Rakesh", "Manoj Y D", "Pravekha",
        "M S Yeshwanth", "Praveen Sparsh", "Tapass Naresh", "Kanaxx", "Manonmani",
        "Ramana Balachandhran", "Padmaja Sreenivasan", "Samanvitha G. Sasidaran", "Sushmita Narasimhan", "Nidhi Saraogi",
        "Sriradha Bharath", "Avantika K", "Fathima Henna", "Pranjal Thakore", "Manoj Krishna",
        "Himanshu Barot", "Manikandan Chembai", "Aditya Ravindran", "Solomon Ravindar", "Karthik Manickavasakam",
        "Naveen Narendranath", "Rithu Vysakh", "Nikhil Ram", "Mylai M Karthikeyan", "Bharath Sankar",
        "Amrit", "Aarvay", "Radar with a K", "Keba Jeremiah", "Shallu Varun",
        "Jhanu", "Metapurse"
    ];

    // ---------------------------------------------------------------------
    // "Continuing" shuffle
    // ---------------------------------------------------------------------
    // Earlier attempt walked a single deterministic sequence across all
    // 19,440 possible combinations using a golden-ratio step. That never
    // literally reset to the first mix, but the jump size was so large
    // that almost every layer changed on every press -- which *looks*
    // and *feels* exactly like a fresh random reshuffle each time, not a
    // continuation. What "continue from where it left off" actually means
    // is simpler and more literal: keep most of the current mix, and
    // nudge a handful of layers forward by one variant. Two shuffles in a
    // row then visibly build on each other, because most of the tiles
    // genuinely don't change.
    // Starts fetching (and caching) whatever's currently selected without
    // touching any <audio> element -- used to warm the cache in the
    // background while the listener is still browsing the mix grid,
    // before Play. By the time they press Play, some or all of it is
    // already downloaded, since fetchAudioBlob's cache is shared with
    // loadAudioStreams. The grid is visible at this point (mix overlay
    // still open), so each layer's tile dot lights up while its stem is
    // in flight -- same feedback as an actual load, just earlier.
    function prefetchSelectedAudio() {
        PANN_ASSETS.forEach(layer => {
            const idx = state.selections[layer.id];
            const variant = idx !== undefined ? layer.variants[idx] : null;
            if (!variant || !variant.audioCid) return;
            const loader = mixTileLoaders[layer.id];
            const fill = mixTileFills[layer.id];
            if (loader) loader.classList.add('is-active');
            if (fill) fill.style.width = '0%';
            fetchAudioBlob(variant.audioCid, (frac) => {
                if (fill) fill.style.width = `${Math.round(frac * 100)}%`;
            })
                .catch(() => {})
                .finally(() => { if (loader) loader.classList.remove('is-active'); });
        });
    }

    // Fills the inline indicator's small square (a 2x2 collage of the
    // changing layers' art -- a quick visual for "this is what's coming
    // up") and its "changing: ..." line, from the ids of whichever layers
    // this reload actually touches. Each cell shows the LAYER's generic
    // cover instantly (so the indicator never looks empty), then swaps to
    // the actual selected VARIANT's own artwork the moment that image
    // resolves from IPFS -- a real preview of the incoming mix, not just
    // a static per-layer icon, without making the indicator wait on a
    // network round trip before it can even appear.
    //
    // `tokens` (one per changed layer.id, from reloadAudioIfPlaying) gates
    // the async image swap: if the same layer gets touched again before
    // this probe resolves, its token is no longer current and the late
    // image is dropped instead of overwriting the newer one -- otherwise
    // cycling one layer's variant a few times quickly could leave the
    // collage showing an earlier click's art instead of the latest.
    function updateNowLoadingInfo(changedIds, tokens) {
        const ids = changedIds && changedIds.length ? changedIds : PANN_ASSETS.map(l => l.id);
        const layers = ids.map(id => PANN_ASSETS.find(l => l.id === id)).filter(Boolean);
        if (layers.length === 0) return;

        if (UI.nowLoadingThumb) {
            UI.nowLoadingThumb.innerHTML = '';
            for (let i = 0; i < 4; i++) {
                const layer = layers[i % layers.length];
                const selectionIndex = state.selections[layer.id];
                const variant = selectionIndex !== undefined ? layer.variants[selectionIndex] : null;
                const myToken = tokens ? tokens[layer.id] : undefined;

                const cell = document.createElement('div');
                cell.className = 'now-loading-thumb-cell';
                cell.style.backgroundImage = `url("${layer.coverLocal}")`;
                UI.nowLoadingThumb.appendChild(cell);

                if (variant && variant.visualCid) {
                    const probe = new Image();
                    probe.onload = () => {
                        if (myToken !== undefined && layerToken[layer.id] !== myToken) return; // superseded by a later click on this layer
                        cell.style.backgroundImage = `url("${probe.src}")`;
                    };
                    const candidates = buildCandidates(variant.visualCid);
                    let idx = 0;
                    probe.onerror = () => { idx++; if (idx < candidates.length) probe.src = candidates[idx]; };
                    probe.src = candidates[0];
                }
            }
        }

        if (UI.nowLoadingChanges) {
            const names = layers.map(l => l.name);
            const shown = names.slice(0, 3).join(', ');
            const extra = names.length > 3 ? ` +${names.length - 3} more` : '';
            UI.nowLoadingChanges.textContent = `${shown}${extra}`;
        }
    }

    // Shows the small inline "loading next" indicator (under the title,
    // not a full-screen overlay) while any changed layers reload, so the
    // current mix keeps playing and visible the whole time -- a shuffle
    // or tile click never lets a new variant play from a half-loaded or
    // still-old audio source, but nothing else is interrupted either.
    // `changedIds` (which layers this particular reload touches) drives
    // the little "what's changing" preview above.
    async function reloadAudioIfPlaying(changedIds) {
        if (!state.hasStartedPlaying) {
            prefetchSelectedAudio(); // background warm-up, no loading UI
            return;
        }
        // Bump a fresh token for every layer this reload touches, *before*
        // anything async starts -- this makes the click order the single
        // source of truth for "which change is the latest", so a layer
        // cycled several times in quick succession always ends up on the
        // last click's variant, no matter which of the overlapping loads
        // happens to finish downloading/decoding first.
        const ids = changedIds && changedIds.length ? changedIds : PANN_ASSETS.map(l => l.id);
        const tokens = {};
        ids.forEach(id => { tokens[id] = bumpLayerToken(id); });
        updateNowLoadingInfo(changedIds, tokens);
        if (UI.nowLoading) UI.nowLoading.classList.remove('hidden');
        await loadAudioStreams(tokens);
        if (UI.nowLoading) UI.nowLoading.classList.add('hidden');
    }

    // The very first load (nothing playing yet, mix overlay still open):
    // shows one unambiguous 0-100% bar above the button row, and holds on
    // "Ready" for a beat once every stem is actually in. Play (the button
    // that triggered this) and Shuffle (which could race with the load
    // that's already in flight) are disabled meanwhile -- but Gallery is
    // deliberately left alone. The mix overlay covers the entire page
    // while open, so Gallery is the only way out if a download ever
    // stalls; nothing about loading should ever be able to trap someone
    // on this screen with no escape.
    async function runFirstLoadWithProgress() {
        if (UI.overlayLoadingFill) UI.overlayLoadingFill.style.width = '0%';
        if (UI.overlayLoadingText) UI.overlayLoadingText.textContent = '0%';
        if (UI.overlayLoading) UI.overlayLoading.classList.remove('hidden');
        if (UI.overlayPlayBtn) UI.overlayPlayBtn.disabled = true;
        if (UI.shuffleAllBtn) UI.shuffleAllBtn.disabled = true;

        await loadAudioStreams();

        if (UI.overlayLoadingFill) UI.overlayLoadingFill.style.width = '100%';
        if (UI.overlayLoadingText) UI.overlayLoadingText.textContent = 'Ready';
        await new Promise(resolve => setTimeout(resolve, 500));

        if (UI.overlayLoading) UI.overlayLoading.classList.add('hidden');
        if (UI.overlayPlayBtn) UI.overlayPlayBtn.disabled = false;
        if (UI.shuffleAllBtn) UI.shuffleAllBtn.disabled = false;
    }

    async function shuffleContinue() {
        // A locked layer never gets touched -- shuffle only draws from the
        // unlocked pool.
        const ids = PANN_ASSETS.map(l => l.id).filter(id => !state.locked[id]);
        if (ids.length === 0) return; // everything is locked -- nothing to shuffle
        for (let i = ids.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [ids[i], ids[j]] = [ids[j], ids[i]];
        }
        // Advance somewhere between a third and two-thirds of the unlocked
        // layers; never zero (nothing would change) and never all of them
        // (that's a full reshuffle, not a continuation), except when too
        // few are left unlocked for that distinction to make sense.
        const minAdvance = Math.min(3, ids.length);
        const maxAdvance = Math.max(minAdvance, ids.length - 2);
        const howMany = minAdvance + Math.floor(Math.random() * (maxAdvance - minAdvance + 1));

        const changedIds = ids.slice(0, howMany);
        changedIds.forEach(id => {
            const layer = PANN_ASSETS.find(l => l.id === id);
            const count = layer.variants.length;
            state.selections[id] = ((state.selections[id] || 0) + 1) % count;
        });

        persistSelections();
        renderMixGrid();          // immediate: tile shows the new pick + its own loading bar
        updateFormationPreview(); // immediate: small preview thumbnail in the overlay panel
        // Keep the "what's changing" preview in the same layer order as
        // the grid, rather than shuffle's internal random order.
        const orderedChangedIds = PANN_ASSETS.map(l => l.id).filter(id => changedIds.includes(id));
        await reloadAudioIfPlaying(orderedChangedIds);
        // Only now -- once the new stems have actually finished loading --
        // does the big scene artwork and the "now playing" tags flip over,
        // so what you see and what you hear always change together, never
        // one ahead of the other.
        renderTags();
        updateVisuals();
    }

    const state = {
        audioContext: null,
        masterGain: null,
        masterCompressor: null,
        audioPool: {},
        visualSlots: {},
        previewSlots: {},
        selections: {},
        locked: {},
        isPlaying: false,
        hasStartedPlaying: false,
        isStopped: true,
        duration: 0,
        syncInterval: null,
        isSeeking: false,
        masterVolume: 1,
        isMuted: false,
        volumeBeforeMute: 1,
        currentMixName: null
    };

    let animationFrameId = null;

    const UI = {
        gatewayPage: document.getElementById("gateway-page"),
        gatewayGrid: document.getElementById("gateway-grid-inner"),
        playerPage: document.getElementById("player-page"),
        enterBtn: document.getElementById("enterBtn"),
        artistsContainer: document.getElementById("artists-container"),
        activeTags: document.getElementById("active-tags"),
        infoToggleBtn: document.getElementById("infoToggleBtn"),
        playPauseBtn: document.getElementById("playPauseBtn"),
        stopBtn: document.getElementById("stopBtn"),
        mixBtn: document.getElementById("mixBtn"),
        layersBtn: document.getElementById("layersBtn"),
        iconPlay: document.getElementById("icon-play"),
        iconPause: document.getElementById("icon-pause"),
        progressBar: document.getElementById("progressBar"),
        progressFill: document.getElementById("progress-fill"),
        currentTimeEl: document.getElementById("current-time"),
        totalTimeEl: document.getElementById("total-time"),
        playerBg: document.getElementById("player-bg"),
        layerContainer: document.getElementById("layer-container"),
        learnMoreBtn: document.getElementById("learnMoreBtn"),
        moreText: document.getElementById("moreText"),
        nowLoading: document.getElementById("now-loading"),
        nowLoadingThumb: document.getElementById("nowLoadingThumb"),
        nowLoadingText: document.getElementById("nowLoadingText"),
        nowLoadingFill: document.getElementById("nowLoadingFill"),
        nowLoadingChanges: document.getElementById("nowLoadingChanges"),
        overlayLoading: document.getElementById("overlay-loading"),
        overlayLoadingFill: document.getElementById("overlayLoadingFill"),
        overlayLoadingText: document.getElementById("overlayLoadingText"),
        mixActions: document.querySelector(".mix-actions"),
        creditsBtn: document.getElementById("creditsBtn"),
        backToGatewayBtn: document.getElementById("backToGatewayBtn"),
        mixOverlay: document.getElementById("mix-overlay"),
        mixGrid: document.getElementById("mix-grid"),
        formationPreview: document.getElementById("formation-preview"),
        shuffleAllBtn: document.getElementById("shuffleAllBtn"),
        overlayStopBtn: document.getElementById("overlayStopBtn"),
        overlayPlayBtn: document.getElementById("overlayPlayBtn"),
        galleryPlayerBtn: document.getElementById("galleryPlayerBtn"),
        seekBackBtn: document.getElementById("seekBackBtn"),
        seekFwdBtn: document.getElementById("seekFwdBtn"),
        muteBtn: document.getElementById("muteBtn"),
        volumeSlider: document.getElementById("volumeSlider"),
        iconVolOn: document.getElementById("icon-vol-on"),
        iconVolOff: document.getElementById("icon-vol-off"),
        previewLightbox: document.getElementById("preview-lightbox"),
        lightboxStage: document.getElementById("lightbox-stage"),
        saveMixBtn: document.getElementById("saveMixBtn"),
        galleryNavBtn: document.getElementById("galleryNavBtn"),
        galleryBackBtn: document.getElementById("galleryBackBtn"),
        galleryPage: document.getElementById("gallery-page"),
        galleryGrid: document.getElementById("gallery-grid"),
        galleryEmpty: document.getElementById("gallery-empty"),
        galleryTabAll: document.getElementById("galleryTabAll"),
        galleryTabFavorites: document.getElementById("galleryTabFavorites"),
        galleryBg: document.getElementById("gallery-bg"),
        galleryOverlayBtn: document.getElementById("galleryOverlayBtn"),
        mixTitleInput: document.getElementById("mixTitleInput"),
        nowPlayingTitle: document.getElementById("nowPlayingTitle"),
        quickSaveBtn: document.getElementById("quickSaveBtn"),
        nameMixModal: document.getElementById("name-mix-modal"),
        nameMixInput: document.getElementById("nameMixInput"),
        nameMixCancelBtn: document.getElementById("nameMixCancelBtn"),
        nameMixSaveBtn: document.getElementById("nameMixSaveBtn"),
        mixInfoModal: document.getElementById("mix-info-modal"),
        mixInfoTitle: document.getElementById("mixInfoTitle"),
        mixInfoDate: document.getElementById("mixInfoDate"),
        mixInfoLayers: document.getElementById("mixInfoLayers"),
        mixInfoCloseBtn: document.getElementById("mixInfoCloseBtn")
    };

    function initMasteringAudioContext() {
        if (!state.audioContext) {
            const AudioCtx = window.AudioContext || window.webkitAudioContext;
            state.audioContext = new AudioCtx();

            state.masterGain = state.audioContext.createGain();
            state.masterGain.gain.value = 1.0;

            state.masterCompressor = state.audioContext.createDynamicsCompressor();
            state.masterCompressor.threshold.value = -18.5;
            state.masterCompressor.ratio.value = 2.0;
            state.masterCompressor.attack.value = 0.0012;
            state.masterCompressor.release.value = 0.087;

            const lowShelf = state.audioContext.createBiquadFilter();
            lowShelf.type = 'lowshelf'; lowShelf.frequency.value = 66; lowShelf.gain.value = 1.33;

            const eq1 = state.audioContext.createBiquadFilter();
            eq1.type = 'peaking'; eq1.frequency.value = 282.2; eq1.Q.value = 6.786; eq1.gain.value = -2.38;

            const eq2 = state.audioContext.createBiquadFilter();
            eq2.type = 'peaking'; eq2.frequency.value = 881.3; eq2.Q.value = 8.860; eq2.gain.value = -0.9;

            const eq3 = state.audioContext.createBiquadFilter();
            eq3.type = 'peaking'; eq3.frequency.value = 2910; eq3.Q.value = 11.04; eq3.gain.value = -0.9;

            const highShelf = state.audioContext.createBiquadFilter();
            highShelf.type = 'highshelf'; highShelf.frequency.value = 10661; highShelf.Q.value = 1.675; highShelf.gain.value = 1.03;

            state.masterCompressor
                .connect(eq1)
                .connect(eq2)
                .connect(eq3)
                .connect(lowShelf)
                .connect(highShelf)
                .connect(state.masterGain)
                .connect(state.audioContext.destination);
        }

        if (state.audioContext.state === 'suspended') {
            state.audioContext.resume();
        }
    }

    function populateArtists() {
        if (!UI.artistsContainer) return;
        UI.artistsContainer.innerHTML = '';
        ARTISTS_LIST.forEach(artist => {
            const tag = document.createElement("span");
            tag.className = "tag";
            tag.textContent = artist;
            UI.artistsContainer.appendChild(tag);
        });
    }

    function formatTime(seconds) {
        if (!seconds || isNaN(seconds)) return "0:00";
        const m = Math.floor(seconds / 60);
        const s = Math.floor(seconds % 60);
        return `${m}:${s.toString().padStart(2, '0')}`;
    }

    // Cover art ships as small local images bundled with the page (the
    // original bespoke covers, not the IPFS layer art), so it's instant
    // and costs zero gateway requests.
    function loadCoverInto(el, layer) {
        if (!layer || !layer.coverLocal) return;
        el.style.backgroundImage = `url("${layer.coverLocal}")`;
    }

    // Static 3x3 grid on the landing page -- one cover photo per layer,
    // no rotation, no shuffle. Purely a backdrop for the "Enter" moment.
    function renderGatewayGrid() {
        if (!UI.gatewayGrid) return;
        UI.gatewayGrid.innerHTML = '';
        PANN_ASSETS.forEach(layer => {
            const cell = document.createElement('div');
            cell.className = 'gateway-grid-cell';
            loadCoverInto(cell, layer);
            UI.gatewayGrid.appendChild(cell);
        });
    }

    // The mix-selection grid: same 3x3 layout, now interactive. Each tile
    // shows the layer's name/subtitle, which variant is currently chosen,
    // and who performed it. Clicking a tile cycles to the next variant for
    // that layer only. The cover photo gets its own full space at the top
    // of the tile -- the caption sits below it, outside the picture
    // entirely, rather than printed over the artwork.
    // Keyed by layer id -- lets loadAudioStreams drive a real per-tile
    // 0-100% progress bar on exactly the layers it's currently fetching,
    // without re-rendering the grid. mixTileLoaders holds the track
    // (shown/hidden via .is-active); mixTileFills holds the actual bar
    // whose width is set directly from download progress.
    const mixTileLoaders = {};
    const mixTileFills = {};
    function renderMixGrid() {
        if (!UI.mixGrid) return;
        UI.mixGrid.innerHTML = '';
        PANN_ASSETS.forEach(layer => {
            const idx = state.selections[layer.id] || 0;
            const variant = layer.variants[idx];

            const isLocked = !!state.locked[layer.id];

            const tile = document.createElement('div');
            tile.className = 'mix-tile';

            const image = document.createElement('div');
            image.className = 'mix-tile-image' + (isLocked ? ' is-locked' : '');
            loadCoverInto(image, layer);
            image.setAttribute('role', 'button');
            image.setAttribute('tabindex', '0');
            image.setAttribute('aria-label', `${layer.name}: ${variant.label}. Tap to change.`);
            const cycle = () => cycleLayerVariant(layer.id, 1);
            image.addEventListener('click', cycle);
            image.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); cycle(); }
            });
            tile.appendChild(image);

            // Lock: keeps this one layer out of Shuffle's reach entirely,
            // so a favorite stem can stay put while the rest keep moving.
            const lockBtn = document.createElement('button');
            lockBtn.type = 'button';
            lockBtn.className = 'mix-tile-lock' + (isLocked ? ' is-locked' : '');
            lockBtn.setAttribute('aria-pressed', String(isLocked));
            lockBtn.setAttribute('aria-label', isLocked ? `Unlock ${layer.name} for shuffle` : `Lock ${layer.name} so shuffle skips it`);
            lockBtn.innerHTML = isLocked
                ? '<svg viewBox="0 0 24 24" width="14" height="14" fill="white"><path d="M12 17a2 2 0 002-2 2 2 0 00-2-2 2 2 0 00-2 2 2 2 0 002 2zm6-9h-1V6a5 5 0 00-10 0h2a3 3 0 016 0v2H6a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V10a2 2 0 00-2-2z"/></svg>'
                : '<svg viewBox="0 0 24 24" width="14" height="14" fill="white"><path d="M18 8h-1V6a5 5 0 00-9.9-1h2.03A3 3 0 0117 6v2H6a2 2 0 00-2 2v10a2 2 0 002 2h12a2 2 0 002-2V10a2 2 0 00-2-2zm-6 9a2 2 0 01-2-2 2 2 0 012-2 2 2 0 012 2 2 2 0 01-2 2z"/></svg>';
            lockBtn.addEventListener('click', (e) => {
                e.stopPropagation();
                state.locked[layer.id] = !state.locked[layer.id];
                renderMixGrid();
            });
            image.appendChild(lockBtn);

            const loader = document.createElement('div');
            loader.className = 'mix-tile-loader';
            const loaderFill = document.createElement('div');
            loaderFill.className = 'mix-tile-loader-fill';
            loader.appendChild(loaderFill);
            image.appendChild(loader);
            mixTileLoaders[layer.id] = loader;
            mixTileFills[layer.id] = loaderFill;

            const text = document.createElement('div');
            text.className = 'mix-tile-text';

            const heading = document.createElement('div');
            heading.className = 'mix-tile-heading';
            const nameEl = document.createElement('span');
            nameEl.className = 'mix-tile-name';
            nameEl.textContent = layer.name;
            heading.appendChild(nameEl);
            if (layer.subtitle) {
                const subEl = document.createElement('span');
                subEl.className = 'mix-tile-subtitle';
                subEl.textContent = layer.subtitle;
                heading.appendChild(subEl);
            }
            text.appendChild(heading);

            const variantRow = document.createElement('div');
            variantRow.className = 'mix-tile-variant';
            variantRow.textContent = variant.label;
            const countEl = document.createElement('span');
            countEl.className = 'mix-tile-count';
            countEl.textContent = `${idx + 1}/${layer.variants.length}`;
            variantRow.appendChild(countEl);
            text.appendChild(variantRow);

            if (variant.credit) {
                const creditEl = document.createElement('div');
                creditEl.className = 'mix-tile-credit';
                creditEl.textContent = variant.credit;
                text.appendChild(creditEl);
            }

            tile.appendChild(text);
            UI.mixGrid.appendChild(tile);
        });
    }

    // Live preview of the actual composited artwork, shown in the right-hand
    // column of the mix overlay so a tap on the left is visible immediately
    // -- same layer stack, same z-order, as what plays behind the scenes,
    // just rendered un-blurred instead of hidden behind the glass. Each
    // layer gets one persistent slot (built once in init); updating a
    // single layer only touches that slot's image, so picking a new
    // Strings variant doesn't tear down and refade the other 8 layers too.
    function updatePreviewLayer(layer, variant, slot) {
        if (!slot || slot.dataset.currentVisual === variant.visualCid) return;
        slot.dataset.currentVisual = variant.visualCid;

        const img = document.createElement('img');
        img.alt = '';

        const candidates = buildCandidates(variant.visualCid);
        loadResilient(img, candidates, {
            isAudio: false,
            timeoutMs: 8000,
            onReady: () => {
                const oldImages = Array.from(slot.querySelectorAll('img'));
                oldImages.forEach(oldImg => {
                    oldImg.classList.remove('layer-visible');
                    setTimeout(() => { if (oldImg.parentNode) oldImg.remove(); }, 700);
                });
                slot.appendChild(img);
                requestAnimationFrame(() => img.classList.add('layer-visible'));
            }
        });
    }

    function updateFormationPreview(changedLayerId = null) {
        PANN_ASSETS.forEach(layer => {
            if (changedLayerId && changedLayerId !== layer.id) return;
            const idx = state.selections[layer.id];
            const slot = state.previewSlots[layer.id];
            if (idx === undefined) return;
            updatePreviewLayer(layer, layer.variants[idx], slot);
        });
    }

    async function cycleLayerVariant(layerId, dir) {
        const layer = PANN_ASSETS.find(l => l.id === layerId);
        if (!layer) return;
        const count = layer.variants.length;
        const current = state.selections[layerId] || 0;
        state.selections[layerId] = (current + dir + count) % count;
        persistSelections();

        renderMixGrid();
        updateFormationPreview(layerId);
        await reloadAudioIfPlaying([layerId]);
        // Same as shuffleContinue: the scene and tags only flip once the
        // new stem has actually loaded, not the instant it's picked.
        renderTags();
        updateVisuals(layerId);
    }

    // The player has no server and no login, so "remembering" the current
    // mix across an accidental reload can only live in this browser tab.
    // sessionStorage (not localStorage) is used deliberately: it survives
    // a refresh but clears when the tab actually closes, so continuing to
    // shuffle after a refresh doesn't also mean a returning visitor is
    // stuck on someone else's last mix forever.
    const SELECTIONS_STORAGE_KEY = 'pann.selections';
    function persistSelections() {
        try { sessionStorage.setItem(SELECTIONS_STORAGE_KEY, JSON.stringify(state.selections)); } catch (e) { /* private mode, ignore */ }
    }
    function restoreSelections() {
        try {
            const saved = sessionStorage.getItem(SELECTIONS_STORAGE_KEY);
            if (!saved) return null;
            const parsed = JSON.parse(saved);
            for (const layer of PANN_ASSETS) {
                const v = parsed[layer.id];
                if (!Number.isInteger(v) || v < 0 || v >= layer.variants.length) return null;
            }
            return parsed;
        } catch (e) { return null; }
    }

    // The "now playing" strip above the transport controls. Each tag shows
    // the layer + chosen variant, with the performer credit tucked
    // underneath, revealed only when the info toggle is switched on.
    function renderTags() {
        if (!UI.activeTags) return;
        UI.activeTags.innerHTML = '';
        PANN_ASSETS.forEach(layer => {
            const idx = state.selections[layer.id];
            if (idx === undefined) return;
            const variant = layer.variants[idx];

            const tag = document.createElement("div");
            tag.className = "playing-tag";

            const main = document.createElement("span");
            main.className = "playing-tag-main";
            main.textContent = `${layer.name} — ${variant.label}`;
            tag.appendChild(main);

            const credit = document.createElement("span");
            credit.className = "playing-tag-credit";
            credit.textContent = variant.credit || "Uncredited";
            tag.appendChild(credit);

            UI.activeTags.appendChild(tag);
        });
    }

    function updateVisuals(changedLayerId = null) {
        PANN_ASSETS.forEach((layer) => {
            if (changedLayerId && changedLayerId !== layer.id) return;

            const selectionIndex = state.selections[layer.id];
            const slot = state.visualSlots[layer.id];
            if (!slot || selectionIndex === undefined) return;

            const variant = layer.variants[selectionIndex];

            if (slot.dataset.currentVisual === variant.visualCid) return;
            slot.dataset.currentVisual = variant.visualCid;

            const isString = layer.id === 'strings';
            const img = new Image();
            img.className = isString ? 'bg-layer-cover' : 'layerImage';

            const candidates = buildCandidates(variant.visualCid);
            loadResilient(img, candidates, {
                isAudio: false,
                timeoutMs: 8000,
                onReady: () => {
                    const oldImages = Array.from(slot.querySelectorAll('img'));
                    oldImages.forEach(oldImg => {
                        oldImg.classList.remove('layer-visible');
                        setTimeout(() => { if (oldImg.parentNode) oldImg.remove(); }, 1200);
                    });
                    slot.appendChild(img);
                    requestAnimationFrame(() => img.classList.add('layer-visible'));
                },
                onGiveUp: () => {
                    console.warn(`[Pann] Could not load artwork for ${layer.name} / ${variant.label} from any gateway.`);
                }
            });
        });
    }

    // ---------------------------------------------------------------------
    // Audio loading: whole-file fetch via fetchAudioBlob, cached by CID
    // ---------------------------------------------------------------------
    // See fetchAudioBlob above for why: streaming <audio src> hit a real
    // gateway bug on every range request after the first (broke seeking),
    // and a naive full download on every shuffle cost ~650MB/session. A
    // per-CID cache bounds total bandwidth to whatever's actually been
    // explored -- a variant you've already heard costs nothing to return
    // to, whether from shuffle or a manual tile click.
    // `tokens` (optional, one per layer.id -- from reloadAudioIfPlaying)
    // says which generation of that layer's change this call is loading
    // for. Without it (the very first load, before anything is playing),
    // each layer just gets a fresh token of its own here, which is fine
    // since nothing can race with a load that hasn't started playing yet.
    async function loadAudioStreams(tokens = {}) {
        const toLoad = [];
        PANN_ASSETS.forEach(layer => {
            const selectionIndex = state.selections[layer.id];
            const audioNode = state.audioPool[layer.id];
            if (selectionIndex === undefined || !audioNode) return;
            const variant = layer.variants[selectionIndex];
            if (audioNode.dataset.loadedKey === variant.audioCid) return; // already on this variant
            const token = tokens[layer.id] !== undefined ? tokens[layer.id] : bumpLayerToken(layer.id);
            toLoad.push({ layer, variant, audioNode, token });
        });

        // Captured once, up front -- not per-node -- so every reloaded
        // layer resumes on the exact same tick once ALL of them are
        // ready, instead of each one resuming for itself the moment it
        // individually finishes (which used to let a fast layer play
        // alone for seconds while the other 8 were still loading).
        const wasPlaying = state.isPlaying;

        // Byte-weighted progress across every stem in this batch -- each
        // layer contributes its own 0..1 fraction (from actual bytes
        // received, see fetchBlobAsObjectUrl), averaged for one overall
        // percentage. Real progress, not a per-file counter.
        const layerFrac = {};
        toLoad.forEach(({ layer }) => { layerFrac[layer.id] = 0; });
        const total = toLoad.length;
        const reportProgress = () => {
            const ids = Object.keys(layerFrac);
            const pct = ids.length
                ? Math.round((ids.reduce((sum, id) => sum + layerFrac[id], 0) / ids.length) * 100)
                : 100;
            // Inline indicator under the title, for background reloads
            // (shuffle/tile-click while already playing -- the grid isn't
            // visible then, so this is the only feedback shown).
            if (UI.nowLoadingText) UI.nowLoadingText.textContent = total ? `Loading next ${pct}%` : "";
            if (UI.nowLoadingFill) UI.nowLoadingFill.style.width = `${pct}%`;
            // The overlay's own 0-100% readout, for the very first load
            // (see runFirstLoadWithProgress below).
            if (UI.overlayLoadingFill) UI.overlayLoadingFill.style.width = `${pct}%`;
            if (UI.overlayLoadingText) UI.overlayLoadingText.textContent = `${pct}%`;
        };
        reportProgress();

        const pending = toLoad.map(async ({ layer, variant, audioNode, token }) => {
            audioNode.dataset.loadedKey = variant.audioCid;
            const loader = mixTileLoaders[layer.id];
            const fill = mixTileFills[layer.id];
            // True only while no *later* change to this same layer has
            // started since this task began -- once a newer one has
            // (layerToken[layer.id] moves on), this task's own result is
            // stale and must never touch the audio node or its UI, no
            // matter when its download/decode actually finishes. This is
            // what makes the LATEST click always win, instead of whichever
            // overlapping load happens to finish last.
            const isCurrent = () => layerToken[layer.id] === token;
            if (loader) loader.classList.add('is-active');
            if (fill) fill.style.width = '0%';

            // The download itself only ever fills the bar up to 92% --
            // the last stretch is reserved for the browser actually
            // decoding/buffering the file (oncanplay below), which for a
            // ~70MB stem is real, separate work with no byte-progress of
            // its own. Without this gap the bar hit 100% the instant the
            // download finished while playback still wasn't ready --
            // looked done, wasn't, exactly the "still loading" confusion.
            const onDownloadProgress = (frac) => {
                if (!isCurrent()) return;
                const scaled = frac * 0.92;
                layerFrac[layer.id] = scaled;
                if (fill) fill.style.width = `${Math.round(scaled * 100)}%`;
                reportProgress();
            };
            const markReady = () => {
                if (!isCurrent()) return;
                layerFrac[layer.id] = 1;
                if (fill) fill.style.width = '100%';
                reportProgress();
            };

            let ok = false;
            const ATTEMPTS = 3; // a flaky gateway hiccup shouldn't cost a whole layer
            for (let attempt = 1; attempt <= ATTEMPTS && !ok; attempt++) {
                try {
                    const objectUrl = await fetchAudioBlob(variant.audioCid, onDownloadProgress);
                    if (!isCurrent()) break; // a newer click on this layer has already taken over
                    await new Promise((resolve, reject) => {
                        const canplayTimer = setTimeout(() => reject(new Error('canplay timed out')), 15000);
                        audioNode.onerror = () => { clearTimeout(canplayTimer); audioNode.onerror = null; reject(new Error('decode failed')); };
                        audioNode.oncanplay = () => { clearTimeout(canplayTimer); audioNode.oncanplay = null; resolve(); };
                        audioNode.src = objectUrl;
                        audioNode.load();
                    });
                    if (!isCurrent()) break; // superseded while decoding -- don't resurrect an old variant
                    if (audioNode.duration > state.duration) {
                        state.duration = audioNode.duration;
                        if (UI.totalTimeEl) UI.totalTimeEl.textContent = formatTime(state.duration);
                    }
                    ok = true;
                } catch (e) {
                    console.warn(`[Pann] Attempt ${attempt}/${ATTEMPTS} failed for ${layer.name} / ${variant.label}.`, e);
                }
            }
            if (!ok && isCurrent()) {
                console.error(`[Pann] Giving up on ${layer.name} / ${variant.label} after ${ATTEMPTS} attempts -- it will stay silent.`);
                delete audioNode.dataset.loadedKey;
            }

            markReady(); // only ever hits 100% here -- once truly ready (or truly given up) -- no-op if superseded
            if (isCurrent() && loader) loader.classList.remove('is-active');
            // "applied" (not just "ok") is what the resume step below must
            // check -- a load that succeeded but was superseded before it
            // could finish must never be allowed to jump in and resume
            // playback on its now-stale variant.
            return { ok, applied: ok && isCurrent() };
        });

        const results = await Promise.all(pending);

        // Resume together, all at once, only once every layer in this
        // batch has either succeeded or exhausted its retries -- never
        // one at a time as each finishes.
        if (wasPlaying) {
            const masterTime = state.audioPool['strings'] ? state.audioPool['strings'].currentTime : 0;
            toLoad.forEach(({ audioNode }, i) => {
                if (!results[i].applied) return; // failed, or superseded -- leave whatever's already playing alone
                audioNode.currentTime = masterTime;
            });
            toLoad.forEach(({ audioNode }, i) => {
                if (!results[i].applied) return;
                audioNode.play().catch(() => {});
            });
        }

        return results.map(r => r.ok);
    }

    function enforceSync() {
        if (state.isSeeking) return;
        const nodes = Object.values(state.audioPool).filter(n => !n.paused && n.src);
        if (nodes.length <= 1) return;

        const master = nodes[0];
        nodes.forEach((node, i) => {
            if (i === 0) return;
            const drift = node.currentTime - master.currentTime;

            if (Math.abs(drift) > 0.4) {
                node.currentTime = master.currentTime;
            } else if (Math.abs(drift) > 0.03) {
                node.playbackRate = master.playbackRate - (drift * 0.5);
            } else {
                node.playbackRate = 1.0;
            }
        });
    }

    function playAudio(targetTime = null) {
        initMasteringAudioContext();

        const nodes = Object.values(state.audioPool).filter(n => n.src);
        if (nodes.length === 0) return;

        const timeToSet = targetTime !== null ? targetTime : (nodes[0].currentTime || 0);

        nodes.forEach(node => {
            node.currentTime = timeToSet;
            node.play().catch(() => {});
        });

        state.isPlaying = true;
        state.isStopped = false;
        document.body.classList.add('playing');
        if (UI.iconPlay) UI.iconPlay.classList.add('hidden');
        if (UI.iconPause) UI.iconPause.classList.remove('hidden');
        renderTags();
        syncOverlayButtons();

        if (state.syncInterval) clearInterval(state.syncInterval);
        state.syncInterval = setInterval(enforceSync, 600);
        requestAnimationFrame(updateLoop);
    }

    function pauseAudio() {
        Object.values(state.audioPool).forEach(node => node.pause());
        state.isPlaying = false;
        document.body.classList.remove('playing');
        if (UI.iconPlay) UI.iconPlay.classList.remove('hidden');
        if (UI.iconPause) UI.iconPause.classList.add('hidden');
        if (state.syncInterval) clearInterval(state.syncInterval);
        syncOverlayButtons();
    }

    function stopAudio() {
        Object.values(state.audioPool).forEach(node => {
            node.pause();
            node.currentTime = 0;
            node.playbackRate = 1.0;
        });
        state.isPlaying = false;
        state.isStopped = true;
        document.body.classList.remove('playing');
        if (UI.iconPlay) UI.iconPlay.classList.remove('hidden');
        if (UI.iconPause) UI.iconPause.classList.add('hidden');
        if (UI.progressFill) UI.progressFill.style.width = '0%';
        if (UI.currentTimeEl) UI.currentTimeEl.textContent = '0:00';
        if (state.syncInterval) clearInterval(state.syncInterval);
        cancelAnimationFrame(animationFrameId);
        syncOverlayButtons();
    }

    let seekDebounceTimeout = null;

    function seekTo(targetTime) {
        if (!state.duration || isNaN(targetTime)) return;
        state.isSeeking = true;
        Object.values(state.audioPool).filter(n => n.src).forEach(node => { node.currentTime = targetTime; });
        state.isSeeking = false;
    }

    // Jump every layer's playhead together by a relative offset, clamped to
    // the track's bounds. Works whether playing or paused -- the transport
    // UI (progress fill / current time) is nudged immediately rather than
    // waiting for the next updateLoop tick.
    function seekBy(deltaSeconds) {
        if (!state.duration) return;
        const nodes = Object.values(state.audioPool).filter(n => n.src);
        if (nodes.length === 0) return;
        const current = nodes[0].currentTime || 0;
        const target = Math.max(0, Math.min(state.duration, current + deltaSeconds));
        seekTo(target);
        if (UI.progressFill) UI.progressFill.style.width = `${(target / state.duration) * 100}%`;
        if (UI.currentTimeEl) UI.currentTimeEl.textContent = formatTime(target);
    }

    // ---------------------------------------------------------------------
    // Master volume
    // ---------------------------------------------------------------------
    // A single slider scales every layer's HTMLAudioElement.volume together
    // (they all need to move as one -- there's no separate per-layer mix
    // here, just the overall listening level). Mute remembers the level it
    // interrupted so un-muting restores it rather than snapping to 100.
    const VOLUME_STORAGE_KEY = 'pann.volume';
    function applyVolume() {
        const effective = state.isMuted ? 0 : state.masterVolume;
        Object.values(state.audioPool).forEach(node => { node.volume = effective; });
        if (UI.iconVolOn) UI.iconVolOn.classList.toggle('hidden', state.isMuted || state.masterVolume === 0);
        if (UI.iconVolOff) UI.iconVolOff.classList.toggle('hidden', !(state.isMuted || state.masterVolume === 0));
        if (UI.volumeSlider) UI.volumeSlider.value = String(Math.round(state.masterVolume * 100));
        try { sessionStorage.setItem(VOLUME_STORAGE_KEY, JSON.stringify({ v: state.masterVolume, m: state.isMuted })); } catch (e) { /* ignore */ }
    }
    function restoreVolume() {
        try {
            const saved = JSON.parse(sessionStorage.getItem(VOLUME_STORAGE_KEY) || 'null');
            if (saved && typeof saved.v === 'number') {
                state.masterVolume = Math.max(0, Math.min(1, saved.v));
                state.isMuted = !!saved.m;
            }
        } catch (e) { /* ignore */ }
    }

    // ---------------------------------------------------------------------
    // Enlarged formation preview (lightbox)
    // ---------------------------------------------------------------------
    // A plain, non-crossfading rebuild of the current selections' artwork
    // at full size -- opened on demand rather than kept live, since the
    // small preview already handles the animated case.
    function openLightbox() {
        if (!UI.previewLightbox || !UI.lightboxStage) return;
        UI.lightboxStage.innerHTML = '';
        PANN_ASSETS.forEach((layer, index) => {
            const idx = state.selections[layer.id];
            if (idx === undefined) return;
            const variant = layer.variants[idx];
            const img = document.createElement('img');
            img.src = url(variant.visualCid);
            img.alt = '';
            img.style.zIndex = index + 1;
            UI.lightboxStage.appendChild(img);
        });
        UI.previewLightbox.classList.remove('hidden');
        requestAnimationFrame(() => UI.previewLightbox.classList.add('open'));
    }
    function closeLightbox() {
        if (!UI.previewLightbox) return;
        UI.previewLightbox.classList.remove('open');
        setTimeout(() => UI.previewLightbox.classList.add('hidden'), 250);
    }

    // ---------------------------------------------------------------------
    // Saved mixes gallery
    // ---------------------------------------------------------------------
    // localStorage (not sessionStorage) -- these are meant to survive a
    // closed tab or browser restart, unlike the in-progress-mix continuity
    // above. Each saved mix is just a name, a timestamp, a favorite flag,
    // and the selections needed to reproduce it exactly.
    const GALLERY_STORAGE_KEY = 'pann.gallery';
    let galleryFilter = 'all';

    function loadGallery() {
        try {
            const raw = JSON.parse(localStorage.getItem(GALLERY_STORAGE_KEY) || '[]');
            return Array.isArray(raw) ? raw : [];
        } catch (e) { return []; }
    }
    function saveGallery(list) {
        try { localStorage.setItem(GALLERY_STORAGE_KEY, JSON.stringify(list)); } catch (e) { /* private mode / quota, ignore */ }
    }

    // A fresh default name for a not-yet-saved mix: "Pann #1", "Pann #2"...
    // counting up from however many are already in the gallery, so it
    // reads as the next one in the series rather than colliding with an
    // existing name.
    function suggestMixName() {
        return `Pann #${loadGallery().length + 1}`;
    }

    // The mix name lives in three places at once -- the editable title
    // above the preview, the now-playing corner during playback, and
    // whatever gets written to the gallery on Save -- so one setter keeps
    // all three in sync instead of drifting apart.
    function setMixName(name) {
        state.currentMixName = name;
        if (UI.mixTitleInput && document.activeElement !== UI.mixTitleInput) UI.mixTitleInput.value = name;
        if (UI.nowPlayingTitle) UI.nowPlayingTitle.textContent = name;
    }

    function persistMixToGallery(name) {
        const list = loadGallery();
        const entry = {
            id: `mix-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
            name,
            createdAt: Date.now(),
            favorite: false,
            selections: { ...state.selections }
        };
        list.unshift(entry);
        saveGallery(list);
        setMixName(name);
    }

    function renameMix(id, name) {
        const list = loadGallery();
        const entry = list.find(m => m.id === id);
        if (entry && name) entry.name = name;
        saveGallery(list);
        renderGallery();
    }

    // ---------------------------------------------------------------------
    // Save-to-gallery name prompt
    // ---------------------------------------------------------------------
    function openNameModal() {
        if (!UI.nameMixModal || !UI.nameMixInput) return;
        UI.nameMixInput.value = state.currentMixName || suggestMixName();
        UI.nameMixModal.classList.remove('hidden');
        requestAnimationFrame(() => {
            UI.nameMixModal.classList.add('open');
            UI.nameMixInput.focus();
            UI.nameMixInput.select();
        });
    }
    function closeNameModal() {
        if (!UI.nameMixModal) return;
        UI.nameMixModal.classList.remove('open');
        setTimeout(() => UI.nameMixModal.classList.add('hidden'), 200);
    }
    function confirmSaveFromModal() {
        const name = (UI.nameMixInput && UI.nameMixInput.value.trim()) || suggestMixName();
        persistMixToGallery(name);
        closeNameModal();

        [UI.saveMixBtn, UI.quickSaveBtn].forEach(btn => {
            if (!btn) return;
            btn.classList.add('is-saved');
            setTimeout(() => btn.classList.remove('is-saved'), 1200);
        });
    }

    // ---------------------------------------------------------------------
    // Gallery card details (the "i" info panel)
    // ---------------------------------------------------------------------
    function openMixInfoModal(entry) {
        if (!UI.mixInfoModal) return;
        if (UI.mixInfoTitle) UI.mixInfoTitle.textContent = entry.name;
        if (UI.mixInfoDate) UI.mixInfoDate.textContent = `Saved ${new Date(entry.createdAt).toLocaleString()}`;
        if (UI.mixInfoLayers) {
            UI.mixInfoLayers.innerHTML = '';
            PANN_ASSETS.forEach(layer => {
                const v = entry.selections[layer.id];
                if (!Number.isInteger(v) || v < 0 || v >= layer.variants.length) return;
                const variant = layer.variants[v];

                const row = document.createElement('div');
                row.className = 'modal-layer-row';

                const nameEl = document.createElement('span');
                nameEl.className = 'modal-layer-name';
                nameEl.textContent = layer.name;
                if (layer.subtitle) {
                    const sub = document.createElement('span');
                    sub.textContent = layer.subtitle;
                    nameEl.appendChild(sub);
                }
                row.appendChild(nameEl);

                const variantEl = document.createElement('span');
                variantEl.className = 'modal-layer-variant';
                variantEl.textContent = variant.label;
                if (variant.credit) {
                    const credit = document.createElement('span');
                    credit.className = 'modal-layer-credit';
                    credit.textContent = variant.credit;
                    variantEl.appendChild(credit);
                }
                row.appendChild(variantEl);

                UI.mixInfoLayers.appendChild(row);
            });
        }
        UI.mixInfoModal.classList.remove('hidden');
        requestAnimationFrame(() => UI.mixInfoModal.classList.add('open'));
    }
    function closeMixInfoModal() {
        if (!UI.mixInfoModal) return;
        UI.mixInfoModal.classList.remove('open');
        setTimeout(() => UI.mixInfoModal.classList.add('hidden'), 200);
    }

    function toggleFavorite(id) {
        const list = loadGallery();
        const entry = list.find(m => m.id === id);
        if (entry) entry.favorite = !entry.favorite;
        saveGallery(list);
        renderGallery();
    }

    function deleteMix(id) {
        const list = loadGallery().filter(m => m.id !== id);
        saveGallery(list);
        renderGallery();
    }

    async function loadMixFromGallery(id) {
        const entry = loadGallery().find(m => m.id === id);
        if (!entry) return;
        const changedIds = [];
        PANN_ASSETS.forEach(layer => {
            const v = entry.selections[layer.id];
            const newIdx = Number.isInteger(v) && v >= 0 && v < layer.variants.length ? v : 0;
            if (state.selections[layer.id] !== newIdx) changedIds.push(layer.id);
            state.selections[layer.id] = newIdx;
        });
        persistSelections();
        setMixName(entry.name);
        renderMixGrid();
        updateFormationPreview();
        renderTags();
        updateVisuals();
        await reloadAudioIfPlaying(changedIds);

        goToPlayerFromGallery();
        openMixOverlay();
    }

    // Builds a small, static composite of a saved mix's artwork for its
    // gallery card -- same layered-stack idea as the live preview, but
    // built once with no crossfade since nothing here is changing live.
    // Shared by the gallery-card thumbnails and the gallery page's own
    // blurred backdrop -- both are just a static stack of each layer's
    // current image, same z-order as everywhere else in the piece.
    function fillCompositeStack(container, selections) {
        container.innerHTML = '';
        PANN_ASSETS.forEach((layer, index) => {
            const v = selections[layer.id];
            if (!Number.isInteger(v) || v < 0 || v >= layer.variants.length) return;
            const variant = layer.variants[v];
            const img = document.createElement('img');
            img.src = url(variant.visualCid);
            img.alt = '';
            img.style.zIndex = index + 1;
            container.appendChild(img);
        });
    }

    function buildGalleryThumb(entry) {
        const thumb = document.createElement('div');
        thumb.className = 'gallery-card-thumb';
        fillCompositeStack(thumb, entry.selections);
        return thumb;
    }

    // Whatever mix is currently loaded (playing, paused, or just sitting
    // selected) becomes the gallery's backdrop -- heavily blurred behind
    // the scrim, so the page still feels like part of the same piece
    // rather than a plain settings screen.
    function renderGalleryBackground() {
        if (!UI.galleryBg) return;
        fillCompositeStack(UI.galleryBg, state.selections);
    }

    function renderGallery() {
        if (!UI.galleryGrid) return;
        const list = loadGallery().filter(m => galleryFilter === 'all' || m.favorite);
        UI.galleryGrid.innerHTML = '';

        if (UI.galleryEmpty) {
            UI.galleryEmpty.classList.toggle('hidden', list.length > 0);
            UI.galleryEmpty.textContent = galleryFilter === 'favorites'
                ? "No favorites yet -- tap the star on a saved mix to add one."
                : "No saved mixes yet -- press Save on a mix you like and it'll show up here.";
        }

        list.forEach(entry => {
            const card = document.createElement('div');
            card.className = 'gallery-card';

            const thumb = buildGalleryThumb(entry);
            thumb.setAttribute('role', 'button');
            thumb.setAttribute('tabindex', '0');
            thumb.setAttribute('aria-label', `Load ${entry.name}`);
            const load = () => loadMixFromGallery(entry.id);
            thumb.addEventListener('click', load);
            thumb.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); load(); }
            });
            card.appendChild(thumb);

            const row = document.createElement('div');
            row.className = 'gallery-card-row';

            const nameWrap = document.createElement('div');
            nameWrap.className = 'gallery-card-name-wrap';

            const nameEl = document.createElement('span');
            nameEl.className = 'gallery-card-name';
            nameEl.textContent = entry.name;
            nameWrap.appendChild(nameEl);

            // Editing swaps the name span for a text input in place; Enter
            // or blur commits it, Escape backs out without saving.
            const editBtn = document.createElement('button');
            editBtn.type = 'button';
            editBtn.className = 'gallery-edit-btn';
            editBtn.setAttribute('aria-label', `Rename ${entry.name}`);
            editBtn.innerHTML = '<svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor"><path d="M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 000-1.41l-2.34-2.34a1 1 0 00-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z"/></svg>';
            editBtn.addEventListener('click', () => {
                const input = document.createElement('input');
                input.type = 'text';
                input.className = 'gallery-name-input';
                input.maxLength = 60;
                input.value = entry.name;
                nameWrap.replaceChild(input, nameEl);
                input.focus();
                input.select();
                const commit = () => {
                    const val = input.value.trim();
                    renameMix(entry.id, val || entry.name);
                };
                input.addEventListener('blur', commit);
                input.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
                    else if (e.key === 'Escape') { e.preventDefault(); input.value = entry.name; input.blur(); }
                });
            });
            nameWrap.appendChild(editBtn);
            row.appendChild(nameWrap);

            const dateEl = document.createElement('span');
            dateEl.className = 'gallery-card-date';
            dateEl.textContent = new Date(entry.createdAt).toLocaleDateString();
            row.appendChild(dateEl);

            card.appendChild(row);

            const actions = document.createElement('div');
            actions.className = 'gallery-card-actions';

            const favBtn = document.createElement('button');
            favBtn.type = 'button';
            favBtn.className = 'gallery-favorite-btn' + (entry.favorite ? ' is-favorite' : '');
            favBtn.setAttribute('aria-label', entry.favorite ? 'Remove from favorites' : 'Add to favorites');
            favBtn.innerHTML = '<svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor"><path d="M12 17.27 18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/></svg>';
            favBtn.addEventListener('click', () => toggleFavorite(entry.id));
            actions.appendChild(favBtn);

            const infoBtn = document.createElement('button');
            infoBtn.type = 'button';
            infoBtn.className = 'gallery-info-btn';
            infoBtn.setAttribute('aria-label', `About ${entry.name}`);
            infoBtn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M11 7h2v2h-2zm0 4h2v6h-2zm1-9C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8z"/></svg>';
            infoBtn.addEventListener('click', () => openMixInfoModal(entry));
            actions.appendChild(infoBtn);

            const loadBtn = document.createElement('button');
            loadBtn.type = 'button';
            loadBtn.className = 'secondary-btn';
            loadBtn.textContent = 'Load';
            loadBtn.addEventListener('click', () => loadMixFromGallery(entry.id));
            actions.appendChild(loadBtn);

            const delBtn = document.createElement('button');
            delBtn.type = 'button';
            delBtn.className = 'gallery-delete-btn';
            delBtn.setAttribute('aria-label', 'Delete this mix');
            delBtn.innerHTML = '<svg viewBox="0 0 24 24" width="16" height="16" fill="currentColor"><path d="M6 19a2 2 0 002 2h8a2 2 0 002-2V7H6zM19 4h-3.5l-1-1h-5l-1 1H5v2h14z"/></svg>';
            delBtn.addEventListener('click', () => deleteMix(entry.id));
            actions.appendChild(delBtn);

            card.appendChild(actions);
            UI.galleryGrid.appendChild(card);
        });
    }

    function setGalleryFilter(filter) {
        galleryFilter = filter;
        if (UI.galleryTabAll) UI.galleryTabAll.classList.toggle('is-active', filter === 'all');
        if (UI.galleryTabFavorites) UI.galleryTabFavorites.classList.toggle('is-active', filter === 'favorites');
        renderGallery();
    }

    let cameFromPlayer = false;

    function openGalleryPage() {
        if (!UI.galleryPage) return;
        cameFromPlayer = !!(UI.playerPage && UI.playerPage.classList.contains('active'));
        renderGalleryBackground();
        renderGallery();
        const hidePage = cameFromPlayer ? UI.playerPage : UI.gatewayPage;
        if (hidePage) hidePage.classList.remove('active');
        setTimeout(() => {
            if (hidePage) hidePage.classList.add('hidden');
            UI.galleryPage.classList.remove('hidden');
            setTimeout(() => UI.galleryPage.classList.add('active'), 50);
        }, 600);
    }

    function closeGalleryPage() {
        if (!UI.galleryPage) return;
        const showPage = cameFromPlayer ? UI.playerPage : UI.gatewayPage;
        UI.galleryPage.classList.remove('active');
        setTimeout(() => {
            UI.galleryPage.classList.add('hidden');
            if (showPage) {
                showPage.classList.remove('hidden');
                setTimeout(() => showPage.classList.add('active'), 50);
            }
        }, 600);
    }

    // Loading a mix from the gallery always lands the listener in the
    // player (with the mix overlay open on the loaded selections),
    // whichever page the gallery was opened from.
    function goToPlayerFromGallery() {
        if (!UI.galleryPage || !UI.playerPage) return;
        UI.galleryPage.classList.remove('active');
        UI.gatewayPage.classList.remove('active');
        setTimeout(() => {
            UI.galleryPage.classList.add('hidden');
            UI.gatewayPage.classList.add('hidden');
            UI.playerPage.classList.remove('hidden');
            setTimeout(() => UI.playerPage.classList.add('active'), 50);
        }, 600);
    }

    function handleProgressInteraction(e) {
        if (!state.duration) return;
        const rect = UI.progressBar.getBoundingClientRect();

        let clientX = e.clientX;
        if (e.touches && e.touches.length > 0) clientX = e.touches[0].clientX;
        else if (e.changedTouches && e.changedTouches.length > 0) clientX = e.changedTouches[0].clientX;

        if (clientX === undefined || clientX === null) return;

        const percentage = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
        const targetTime = percentage * state.duration;

        if (UI.progressFill) UI.progressFill.style.width = `${percentage * 100}%`;
        if (UI.currentTimeEl) UI.currentTimeEl.textContent = formatTime(targetTime);

        clearTimeout(seekDebounceTimeout);
        seekDebounceTimeout = setTimeout(() => seekTo(targetTime), 50);
    }

    function updateLoop() {
        if (!state.isPlaying || state.isSeeking) return;
        const nodes = Object.values(state.audioPool).filter(n => !n.paused && n.src);
        if (nodes.length > 0 && UI.progressFill && UI.currentTimeEl) {
            const current = nodes[0].currentTime;
            UI.progressFill.style.width = `${(current / state.duration) * 100}%`;
            UI.currentTimeEl.textContent = formatTime(current);
        }
        animationFrameId = requestAnimationFrame(updateLoop);
    }

    // ---------------------------------------------------------------------
    // Mix overlay (glass panel shown before first Play, and again on
    // pause/stop): a translucent blur over the currently-selected artwork,
    // with the same 3x3 grid on top so the listener can change layers,
    // shuffle, or resume/stop/play.
    // ---------------------------------------------------------------------
    function openMixOverlay() {
        if (!UI.mixOverlay) return;
        if (!state.currentMixName) setMixName(suggestMixName());
        renderMixGrid();
        syncOverlayButtons();
        UI.mixOverlay.classList.add('open');
    }

    function closeMixOverlay() {
        if (!UI.mixOverlay) return;
        UI.mixOverlay.classList.remove('open');
    }

    function syncOverlayButtons() {
        if (UI.overlayPlayBtn) {
            UI.overlayPlayBtn.textContent = (state.hasStartedPlaying && state.isStopped === false && !state.isPlaying)
                ? 'Resume'
                : (state.hasStartedPlaying ? 'Resume' : 'Play');
        }
        if (UI.overlayStopBtn) {
            const canStop = state.hasStartedPlaying && !state.isStopped;
            UI.overlayStopBtn.classList.toggle('hidden', !canStop);
        }
    }

    async function init() {
        mergeCidsIntoAssets();
        populateArtists();

        if (UI.layerContainer) UI.layerContainer.innerHTML = '';

        PANN_ASSETS.forEach((layer, index) => {
            const audio = new Audio();
            audio.loop = true;
            audio.preload = "auto";

            state.audioPool[layer.id] = audio;

            const slot = document.createElement('div');
            slot.className = 'layer-slot';
            slot.style.zIndex = index + 5;

            if (layer.id === 'strings' && UI.playerBg) {
                UI.playerBg.appendChild(slot);
            } else if (UI.layerContainer) {
                UI.layerContainer.appendChild(slot);
            }
            state.visualSlots[layer.id] = slot;

            if (UI.formationPreview) {
                const previewSlot = document.createElement('div');
                previewSlot.className = 'preview-layer-slot';
                previewSlot.style.zIndex = index + 1;
                UI.formationPreview.appendChild(previewSlot);
                state.previewSlots[layer.id] = previewSlot;
            }
        });

        // Deterministic default: every layer starts on its first variant,
        // unless this tab already has a mix in progress from before an
        // accidental reload -- in which case shuffling should still feel
        // like it's continuing on, not starting over.
        const restored = restoreSelections();
        PANN_ASSETS.forEach(layer => { state.selections[layer.id] = 0; });
        if (restored) state.selections = restored;

        restoreVolume();
        applyVolume();
        setMixName(suggestMixName());

        renderGatewayGrid();
        renderMixGrid();
        updateFormationPreview();
        renderTags();
        updateVisuals();
    }

    if (UI.learnMoreBtn && UI.moreText) {
        UI.learnMoreBtn.addEventListener('click', () => {
            UI.moreText.classList.toggle('hidden');
            UI.learnMoreBtn.textContent = UI.moreText.classList.contains('hidden') ? "Learn more" : "Show less";
        });
    }

    if (UI.creditsBtn && UI.artistsContainer) {
        UI.creditsBtn.addEventListener('click', () => {
            const nowHidden = UI.artistsContainer.classList.toggle('hidden');
            UI.creditsBtn.textContent = nowHidden ? `Credits — ${ARTISTS_LIST.length} artists` : "Hide credits";
        });
    }

    if (UI.infoToggleBtn && UI.activeTags) {
        UI.infoToggleBtn.addEventListener('click', () => {
            UI.activeTags.classList.toggle('expanded');
        });
    }

    // Enter moves to the player page and opens the mix overlay with the
    // default selection already made. Playback itself still waits for
    // Play, but the default mix starts downloading right away in the
    // background (prefetchSelectedAudio) so the time spent looking at
    // the grid isn't wasted -- by the time Play is pressed, some or all
    // of it is already cached.
    if (UI.enterBtn && UI.gatewayPage && UI.playerPage) {
        UI.enterBtn.addEventListener('click', async () => {
            initMasteringAudioContext();
            prefetchSelectedAudio();

            UI.gatewayPage.classList.remove('active');
            setTimeout(() => {
                UI.gatewayPage.classList.add('hidden');
                UI.playerPage.classList.remove('hidden');
                setTimeout(() => {
                    UI.playerPage.classList.add('active');
                    openMixOverlay();
                }, 50);
            }, 600);
        });
    }

    if (UI.backToGatewayBtn && UI.gatewayPage && UI.playerPage) {
        UI.backToGatewayBtn.addEventListener('click', () => {
            stopAudio();
            closeMixOverlay();
            UI.playerPage.classList.remove('active');
            setTimeout(() => {
                UI.playerPage.classList.add('hidden');
                UI.gatewayPage.classList.remove('hidden');
                setTimeout(() => UI.gatewayPage.classList.add('active'), 50);
            }, 600);
        });
    }

    // Transport dock: Play/Pause toggles playback and the overlay together
    // -- pausing re-opens the glass panel, resuming closes it.
    if (UI.playPauseBtn) {
        UI.playPauseBtn.addEventListener('click', async () => {
            if (state.isPlaying) {
                pauseAudio();
                openMixOverlay();
                return;
            }
            if (!state.hasStartedPlaying) {
                await runFirstLoadWithProgress();
                state.hasStartedPlaying = true;
            }
            closeMixOverlay();
            playAudio();
        });
    }

    if (UI.stopBtn) {
        UI.stopBtn.addEventListener('click', () => {
            stopAudio();
            openMixOverlay();
        });
    }

    // Dedicated "change layers" button in the transport dock -- pauses (if
    // playing) and brings up the mix overlay without stopping playback
    // position, so the listener can audition a different mix before
    // committing.
    if (UI.layersBtn) {
        UI.layersBtn.addEventListener('click', () => {
            if (state.isPlaying) pauseAudio();
            openMixOverlay();
        });
    }

    // Transport dock shuffle: continues the deterministic walk from
    // wherever the current mix sits, rather than restarting from scratch.
    if (UI.mixBtn) {
        UI.mixBtn.addEventListener('click', async () => {
            await shuffleContinue();
        });
    }

    // Overlay controls
    if (UI.shuffleAllBtn) {
        UI.shuffleAllBtn.addEventListener('click', async () => {
            await shuffleContinue();
        });
    }

    if (UI.overlayStopBtn) {
        UI.overlayStopBtn.addEventListener('click', () => {
            stopAudio();
        });
    }

    if (UI.overlayPlayBtn) {
        UI.overlayPlayBtn.addEventListener('click', async () => {
            if (!state.hasStartedPlaying) {
                await runFirstLoadWithProgress();
                state.hasStartedPlaying = true;
            }
            closeMixOverlay();
            playAudio();
        });
    }

    if (UI.progressBar) {
        UI.progressBar.addEventListener('click', handleProgressInteraction);
        UI.progressBar.addEventListener('touchstart', handleProgressInteraction, { passive: true });
    }

    if (UI.seekBackBtn) UI.seekBackBtn.addEventListener('click', () => seekBy(-10));
    if (UI.seekFwdBtn) UI.seekFwdBtn.addEventListener('click', () => seekBy(10));

    if (UI.muteBtn) {
        UI.muteBtn.addEventListener('click', () => {
            state.isMuted = !state.isMuted;
            applyVolume();
        });
    }
    if (UI.volumeSlider) {
        UI.volumeSlider.addEventListener('input', (e) => {
            state.masterVolume = Math.max(0, Math.min(1, Number(e.target.value) / 100));
            if (state.masterVolume > 0) state.isMuted = false;
            applyVolume();
        });
    }

    // Formation preview: click or Enter/Space enlarges the current
    // composited artwork in a lightbox.
    if (UI.formationPreview) {
        const openIt = () => openLightbox();
        UI.formationPreview.addEventListener('click', openIt);
        UI.formationPreview.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); openIt(); }
        });
    }
    if (UI.previewLightbox) {
        UI.previewLightbox.addEventListener('click', (e) => {
            if (e.target === UI.previewLightbox) closeLightbox();
        });
    }
    document.addEventListener('keydown', (e) => {
        if (e.key !== 'Escape') return;
        if (UI.previewLightbox && UI.previewLightbox.classList.contains('open')) closeLightbox();
        else if (UI.nameMixModal && UI.nameMixModal.classList.contains('open')) closeNameModal();
        else if (UI.mixInfoModal && UI.mixInfoModal.classList.contains('open')) closeMixInfoModal();
    });

    // Editable mix title above the preview -- typed live into state so the
    // now-playing corner and the save prompt both pick it up immediately.
    if (UI.mixTitleInput) {
        UI.mixTitleInput.addEventListener('input', (e) => setMixName(e.target.value));
        UI.mixTitleInput.addEventListener('blur', () => {
            if (!UI.mixTitleInput.value.trim()) setMixName(suggestMixName());
        });
    }

    // Both Save entry points (the overlay's "Save Mix" and the quick-save
    // icon on the playing page) open the same naming prompt rather than
    // saving silently, so every gallery entry gets a deliberate name.
    if (UI.saveMixBtn) UI.saveMixBtn.addEventListener('click', openNameModal);
    if (UI.quickSaveBtn) UI.quickSaveBtn.addEventListener('click', openNameModal);
    if (UI.nameMixSaveBtn) UI.nameMixSaveBtn.addEventListener('click', confirmSaveFromModal);
    if (UI.nameMixCancelBtn) UI.nameMixCancelBtn.addEventListener('click', closeNameModal);
    if (UI.nameMixInput) {
        UI.nameMixInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); confirmSaveFromModal(); }
        });
    }
    if (UI.nameMixModal) {
        UI.nameMixModal.addEventListener('click', (e) => {
            if (e.target === UI.nameMixModal) closeNameModal();
        });
    }

    if (UI.mixInfoCloseBtn) UI.mixInfoCloseBtn.addEventListener('click', closeMixInfoModal);
    if (UI.mixInfoModal) {
        UI.mixInfoModal.addEventListener('click', (e) => {
            if (e.target === UI.mixInfoModal) closeMixInfoModal();
        });
    }

    if (UI.galleryNavBtn) UI.galleryNavBtn.addEventListener('click', openGalleryPage);
    if (UI.galleryPlayerBtn) UI.galleryPlayerBtn.addEventListener('click', openGalleryPage);
    if (UI.galleryOverlayBtn) UI.galleryOverlayBtn.addEventListener('click', openGalleryPage);
    if (UI.galleryBackBtn) UI.galleryBackBtn.addEventListener('click', closeGalleryPage);
    if (UI.galleryTabAll) UI.galleryTabAll.addEventListener('click', () => setGalleryFilter('all'));
    if (UI.galleryTabFavorites) UI.galleryTabFavorites.addEventListener('click', () => setGalleryFilter('favorites'));

    init();
})();
