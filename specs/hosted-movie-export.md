# Hosted movie signup and export

User approved implementation and publication 2026-10-20. The existing `/movie` preview already renders, queues, cancels and downloads real MP4s through the localhost-only Vite middleware. The three original preview/clock/movie commits are independent of this hosted extension.

## Implementation

`www/src/query/movieExport.ts` validates one immutable export plan for local and hosted jobs. It rejects caller URLs, unsupported/duplicate settings, invalid dimensions/FPS/length, and oversized clips. The hosted `www/server/worker.ts` keeps maps public and uses pinned `oa/auth` for Google OIDC or emailed codes; any verified email can export, while admin identity is a separate configured list. `/signin` uses the shared `SignInPanel` and safely returns to the original movie URL.

`/__movies/jobs` enforces authenticated identity, same-origin mutations, server-derived ownership, and atomic D1 quotas: two active jobs/email, five submissions/email/UTC day, and 20 pending globally. Another person's job returns the same 404 as an absent job. Closed tabs do not hold quota forever: new submissions reconcile finished Batch jobs. Job status, cancellation and private download use the same API paths as localhost. The initial per-user limits are conservative operational defaults, not a paid product quota.

The HCCS Batch renderer uses the exact existing browser renderer and current public app assets, streams frames to ffmpeg, publishes private S3 artifacts, and writes progress at most every five seconds. A signed Lambda gateway accepts control-plane requests from the Worker. The HMAC covers timestamp, method, path, and body; old requests expire after 60 seconds. S3 submission records deduplicate a repeated job ID. Artifacts expire after 30 days and download URLs expire after five minutes. Cancellation remains terminal. The renderer uses no R2 write credentials.

The movie-only Pulumi project is `infra/movie-export`, separated from the old feed infrastructure because existing WT checkpoints are stale. Its wrapper always selects the stable common-repository backend at `infra/movie-state`, so independent worktrees do not fork production state. It declares a private S3 bucket, lifecycle/access policy, D1 database, ECR image repository, narrow IAM roles, Batch queue, dedicated idle-free Spot capacity capped at eight vCPUs (two concurrent four-vCPU jobs), and a two-hour job timeout. The image build runs in GitHub Actions; nothing builds a multi-GB container on the laptop.

Bootstrap applied 2026-10-20: 14 resources created, no existing feed/reproc resources changed. Account: HCCS AWS `688066488567`, Cloudflare `2363642879f18d37d52dca114059937e`. D1 is `ctbk-movies` (`87ed908c-33af-494e-980b-9b7f21a94696`); ECR is `688066488567.dkr.ecr.us-east-1.amazonaws.com/ctbk-movies`; private artifacts bucket is `ctbk-movies-688066488567`. Auth migrations and movie schema are initialized in the isolated D1. `www/wrangler.movies.jsonc` targets a separate `ctbk-movies-preview` Worker; promotion must merge bindings with the primary site's worker/OG changes so a later ordinary deployment cannot silently remove hosted export.

## Deployed preview

The isolated [hosted movie preview] is deployed with D1, gateway and session secrets. The Lambda gateway/job-definition phase added 11 resources (25 total), without changing existing feed/reproc infrastructure. Both remote image builds passed; the final deployed image is `ctbk-movies:1a7fdd1d`, built by [movie-image run 39424859446], with digest `sha256:15a99a7a0a59952650497b385edb1fe747999227c3b8a3d8e0c726f17c6ee21a`. Job definition revision 2 pins that image. New internal gateway/session keys were generated and propagated automatically; no existing credentials were rotated.

A real Batch smoke export was privately downloaded and verified: 480×600, 30fps, 1.000 seconds, 537,134 bytes. Its API job is `fcfdac7e-a0c5-4bad-a45c-216b67a759c8`, Batch job `3d1a58cc-95bc-481e-ae03-ca5f1c2bde5b`. A different signed principal received 404. A second real export (`4f1e4459-3cdc-484e-b356-74ca7584ead0`) was cancelled after rendering began and remained cancelled when reread. These probes used a synthetic signed test principal; they do not exercise Google or email-provider signup.

The requested 1080×1350, 30fps, 30-second portrait export (`285cbf54-c4ac-41a0-a9b4-b5b377611512`, Batch `bd3bb439-d1d4-4c2b-b5d2-9bbb08edb4f7`) stopped advancing at frame 610 after roughly ten minutes. It was cancelled to release the slot. Its target is the approved camera `40.745,-73.985,12.15`, September 29 04:00 through September 30 04:00, purple electric/blue classic rides, minimal header, clock and sparkline. The complete clip remains pending publication of the capture fix below and a successful retry.

The first smoke exposed an S3 permission edge: checking for a progress object before it exists returns AccessDenied without ListBucket. The gateway now has ListBucket only on its dedicated private movie bucket, plus its existing job-object permissions. The renderer's permissions remain limited to job objects. Resuming the already completed smoke job verified this change without submitting a duplicate render.

## Rollout remaining

1. Configure a verified signup provider. Actual checks found no CTBK OAuth/mailer environment variables, GitHub secrets, or HCCS Secrets Manager entries. HCCS SES has no verified sender and remains in sandbox (`ProductionAccessEnabled=false`), so it cannot send signup codes to arbitrary recipients. A Google web OAuth client, or a production transactional sender, is required before public signup can work. `/signin` currently reports provider setup pending.
2. Set provider secrets and explicit `ADMIN_EMAILS`, then verify actual signup and administrator access against the isolated preview. Owner isolation, queueing, progress, cancellation and private downloads already have real backend evidence above.
3. Adopt shared session logging from the auth-project extraction handoff. This implementation currently records job lifecycle through the existing auth audit hook; it does not yet provide the disky-style action recorder or `/admin/sessions`.
4. Promote to the primary site after signup validation, merging auth/movie bindings and routes with the production public-preview Worker. Keep localhost's loopback-only renderer path working for iteration.

For Google, create an external-audience Web application named `ctbk.dev movies` in the HCCS [Google Auth client console]. Register `/auth/google/callback` at each of `https://ctbk.dev`, `https://dev.ctbk.dev` and `https://ctbk-movies-preview.hccs-ctbk.workers.dev`; register those origins too. Use scopes `openid email profile` and publish for external users to admit any verified Google email. Store `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` as Worker secrets. This is the standard [Google web-server OAuth flow].

## Current verification

Complete frontend unit suite: 187 passed, including hosted ownership/quotas, signed gateway control, and unchanged localhost export. Typecheck passed. Three Chromium signup-flow tests passed against the exact pinned auth artifact (mocked provider endpoints); nine Python gateway tests passed with mocked AWS. Chrome extension verification is blocked by a missing browser plugin service module, so browser tests are supplemental evidence and not a claim of CIC. Live provider signup remains outstanding. Actual Batch rendering, running cancellation, private download and ffprobe validation are complete.

## Submission recovery

Every D1 reservation contains its deterministic renderer identity before any network call. A Worker crash before dispatch expires that reservation after two minutes; ambiguous submission failures stay pollable rather than losing the job identifier. The gateway records an S3 submission lease, and recovers an accepted Batch job by its exact deterministic `JOB_NAME` if it died before persisting the Batch ID. It never blindly resubmits an ambiguous call. Missing submissions expire, conditional S3 writes keep cancellation terminal, and the runner checks a cancellation tombstone before capturing and while reporting progress. A completion marker preserves private downloads for the full artifact-retention window after Batch removes old control-plane records.

`www/scripts/movie-smoke.mjs` accepts its endpoint and session secret on stdin, uses a synthetic signed principal to test backend authorization/ownership, privately downloads an MP4, and verifies dimensions/FPS/duration with ffprobe. Its default is a one-second 480×600/30fps smoke clip; `query` selects another immutable preview plan, `jobId` resumes an existing job, and `cancelAfterStart` tests cancellation after the renderer becomes active. Status-service errors retry with bounded backoff. The tool explicitly does not claim to test provider signup.

Public screenshot flags such as `__og` are stripped from preview/export snapshots. They cannot silently change the captured movie presentation, and the export request validator rejects unknown settings sent directly to the backend.

## Long-capture readiness

The frame-610 stall was inactive query-cache GC, rather than a missing ride-data bucket: quick seeks across 609–621 and to the final frame succeeded. Movie counts are prefetched with a ten-minute GC lifetime, but the sparkline originally reread those inactive chunks throughout capture. Once GC removed them, its totals became NaN and the frame readiness promise waited forever. A Chromium regression accelerates time eleven minutes: before the fix the sparkline becomes exactly empty; after retaining its fully loaded immutable totals vector it stays identical and seeks across frame 610 and midnight succeed. Only the small totals vector is retained, so station chunks can still be collected normally.

Every data/render/map stage now has a 120-second deadline reporting frame index, requested/rendered clock, data/totals readiness, deck redraw, map loaded and tile loaded state. Aborted waits remove listeners. Map idle listeners are installed before checking readiness. These changes ship in web assets; the already pinned Batch image loads those assets and needs no rebuild.

[hosted movie preview]: https://ctbk-movies-preview.hccs-ctbk.workers.dev/movie
[movie-image run 39424859446]: https://github.com/hudcostreets/ctbk.dev/actions/runs/39424859446
[Google Auth client console]: https://console.cloud.google.com/auth/clients/create
[Google web-server OAuth flow]: https://developers.google.com/identity/protocols/oauth2/web-server
