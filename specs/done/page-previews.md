# Query and section aware public page previews

Implemented 2026-10-20. Deployment validation of the remote Browser Run binding remains part of rollout.

The previous dashboard metadata used one static mosaic for most routes. Station detail links had a live, separately drawn API image, but that renderer did not reflect page query parameters or the section being shared.

The public website Worker now rewrites HTML metadata for Home, stations, station details, timelapse, movie studio, pipeline and health. Its `/og.jpg?view=…` endpoint captures the actual application at 1200 × 630 with Browser Run. The existing React views, data hooks, maps and charts render the image; there is no parallel chart implementation. Movie studio retains its portrait preview inside the studio page, while timelapse captures the current animation frame. Explicit readiness markers and query/network settling prevent loading placeholders from becoming successful previews.

Home uses `useSectionSpy` from `use-prms/sections`, pinned to distribution commit `bb731063bf4202c0501e9c3d6fdada0817f699a4` (source `8f81b6e`). Its sections are `plot`, `map`, `states` and `pipeline`. The URL key is `section`, because `s` already controls chart stacking and station selection. Reader scrolling silently replaces the section parameter without resetting chart filters; legacy heading hashes become server-visible section parameters. Other route state continues using its existing query parameters.

Only allowlisted public routes and visual parameters participate in captures. Backend overrides, export jobs, login state and private admin routes are excluded. Every capture starts a fresh browser with no caller cookies and can reach only the application, public data hosts and map tile providers. URL length and parameter lengths are bounded. Failed captures return an uncached 503 rather than an unrelated generic image.

Cache keys contain canonical public URL state, deployed version and a server-derived time bucket. Ordinary page images refresh hourly; health images refresh every five minutes. Caller-supplied version/time arguments cannot force extra captures. Cache hits bypass rendering limits. Cold captures are deduplicated, limited to two concurrently per isolate, and subject to a native Cloudflare rate limiter of ten per minute per Cloudflare location. This limits bursts, not total account spending. Production and staging have distinct rate limit namespaces. External platforms maintain their own unfurl caches, so a newly shared link is not guaranteed to fetch a newly rendered image immediately.

The bindings attach Browser Run, Workers Assets and the native rate limiter to the existing HCCS website Worker; no VM, R2 bucket or additional database is required. `server/public-worker.ts` is the production entry. The separate hosted movie/auth Worker can delegate its final public fallback to `servePublicPage(request, env, ctx)` once mounted, retaining the same bindings and public route routing. Generated binding types are checked independently by `tsconfig.worker.json`.

URL state is the sharing contract. The embedded Home map currently keeps its camera and selection in local component state, so its section preview restores the default embedded map. Fullscreen `/stations` links encode camera and selection and preserve those views. Browser local storage, unsaved controls and private session state are never part of public captures.

Validation: 182 unit tests passed, including canonicalization, cookie isolation, real capture URL selection, cache reuse and cold capture rate limiting; two Playwright section-spy/readiness tests passed. Frontend and Worker TypeScript checks, Vite production build and Wrangler deployment dry-run passed. Local Worker HTML rewriting returned section/query-specific image metadata. Actual browser renders of Home, station map, ride animation and movie studio loaded their real plots without page errors, and their screenshots were visually reviewed. The Chrome MCP bootstrap was unavailable, so those render checks used installed Playwright Chromium; remote Cloudflare Browser Run rendering must be smoke tested after deployment.

Review URLs:

- `http://localhost:3462/?r=jh&s=b&section=states`
- `http://localhost:3462/stations?m=2609&sel=JC014&ll=40.724-74.04+14&__og=1`
- `http://localhost:3462/timelapse?anim&b=1m&d=260930-260930&t=260930T12&ll=40.735-73.975+12&__og=1`
- `http://localhost:3462/movie?anim&b=1m&clip=260929T04-260930T04&ll=40.735-73.975+12&__og=1`
- `http://localhost:3463/?r=jh&s=b&section=states` (built Worker metadata)
