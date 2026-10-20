# Query and section aware public page previews

Deployed 2026-10-20 at `1a7fdd1d`; remote Browser Run validation passed for five real public views.

The previous dashboard metadata used one static mosaic for most routes. Station detail links had a live, separately drawn API image, but that renderer did not reflect page query parameters or the section being shared.

The public website Worker now rewrites HTML metadata for Home, stations, station details, timelapse, movie studio, pipeline and health. Its `/og.jpg?view=…` endpoint captures the actual application at 1200 × 630 with Browser Run. The existing React views, data hooks, maps and charts render the image; there is no parallel chart implementation. Movie studio retains its portrait preview inside the studio page, while timelapse captures the current animation frame. Explicit readiness markers and query/network settling prevent loading placeholders from becoming successful previews.

Home uses `useSectionSpy` from `use-prms/sections`, pinned to distribution commit `bb731063bf4202c0501e9c3d6fdada0817f699a4` (source `8f81b6e`). Its sections are `plot`, `map`, `states` and `pipeline`. The URL key is `section`, because `s` already controls chart stacking and station selection. Reader scrolling silently replaces the section parameter without resetting chart filters; legacy heading hashes become server-visible section parameters. Other route state continues using its existing query parameters.

Only allowlisted public routes and visual parameters participate in captures. Backend overrides, export jobs, login state and private admin routes are excluded. Every capture starts a fresh browser with no caller cookies and can reach only the application, public data hosts and map tile providers. URL length and parameter lengths are bounded. Failed captures return an uncached 503 rather than an unrelated generic image.

Cache keys contain canonical public URL state, deployed version and a server-derived time bucket. Ordinary page images refresh hourly; health images refresh every five minutes. Caller-supplied version/time arguments cannot force extra captures. Cache hits bypass rendering limits. Cold captures are deduplicated, limited to two concurrently per isolate, and subject to a native Cloudflare rate limiter of ten per minute per Cloudflare location. This limits bursts, not total account spending. Production and staging have distinct rate limit namespaces. External platforms maintain their own unfurl caches, so a newly shared link is not guaranteed to fetch a newly rendered image immediately.

The bindings attach Browser Run, Workers Assets and the native rate limiter to the existing HCCS website Worker; no VM, R2 bucket or additional database is required. `server/public-worker.ts` is the production entry. The separate hosted movie/auth Worker can delegate its final public fallback to `servePublicPage(request, env, ctx)` once mounted, retaining the same bindings and public route routing. Generated binding types are checked independently by `tsconfig.worker.json`.

URL state is the sharing contract. The embedded Home map encodes selection and camera as `mapSel` and `mapLl`, preserving the host page's other filters. Its fullscreen/compare links carry selection, camera, month and theme into `/stations`. The reusable embed also supports local state when its host supplies no controlled values. Captures wait for both map data and GL readiness. Fullscreen `/stations` links retain their existing `ll`/`sel` contract. Browser local storage, unsaved controls and private session state are never part of public captures. Movie studio restores its URL's initial preview frame, rather than an unsaved playback position.

Known API selection is preserved: Home accepts only the fixed `prod`/`dev` enum and Stations its existing boolean flag; capture requests cannot provide an arbitrary backend URL. Capture-only CSS suppresses section focus outlines while normal keyboard focus remains visible.

Validation: the integrated 197-test frontend/server suite passed, alongside frontend and Worker TypeScript checks, production build, and focused browser checks. Chrome CUA verified section restoration, the selected two-source embedded map and its arcs, and the health card. Remote Browser Run produced five inspected 1200×630 JPEGs: Home states with filters, selected Columbus Drive station map/chart, noon ride animation, portrait movie studio and current health. Cold captures took 3.3–9.4 seconds; a repeat returned byte-identical JPEG bytes in 33 ms with a cache hit. Four private/external capture URLs returned 400. Evidence is saved under `wt/rides-tl/tmp/page-previews-remote-20261020/`.

Remote testing found a cached image response with max-age 14400 while its cold response specified 3600; repeated HTML metadata responses remained at 300. Account cache-setting/rule reads returned 403, so the exact account setting is unverified and unchanged. The source now reasserts the intended image TTL on cache hits for GET and HEAD, with a regression covering 14400→300 for health. That header correction and the embedded map followup still need their post-deploy repeat smoke. External platforms' own unfurl caching remains outside this control.

Review URLs:

- `http://localhost:3462/?r=jh&s=b&section=states`
- `http://localhost:3462/stations?m=2609&sel=JC014&ll=40.724-74.04+14&__og=1`
- `http://localhost:3462/timelapse?anim&b=1m&d=260930-260930&t=260930T12&ll=40.735-73.975+12&__og=1`
- `http://localhost:3462/movie?anim&b=1m&clip=260929T04-260930T04&ll=40.735-73.975+12&__og=1`
- `http://localhost:3463/?r=jh&s=b&section=states` (built Worker metadata)
