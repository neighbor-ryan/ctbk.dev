# Extract disky's session event logs into `oa/auth`

Requested 2026-10-20 while adding CTBK hosted movie signup. The user wants the same `/admin/sessions` functionality as disky, reused across apps. This is a handoff to the auth project; CTBK must consume the shared implementation instead of copying disky's logger into another app.

## Existing code to extract

Read the current disky implementation and tests from the local clone: `site/src/sessionLog.ts`, `sessionLogBoot.ts`, `sessionsModel.ts`, `SessionLogSwitch.tsx`, `SessionsPage.tsx`, and their tests; server modules `site/functions/_lib/sessionLog.ts`, `sessionLogShape.ts`, `site/functions/api/session-log/[[path]].ts`; schema and behavior in `specs/done/session-log.md`. Check all deployment worktrees (`wt/gcs`, `wt/cw`, `wt/hf`, `wt/r2`) for fixes before selecting the canonical version.

`oa/auth` already has `anyEmailPolicy`, Google OIDC, emailed-code verification, D1 session/auth audit storage, and React `SignInPanel`. Open signup therefore needs no new admission policy. The reusable gap is the optional, bounded user-action recorder, its storage/routes, and admin viewer.

## Proposed package surfaces

- `@open-athena/auth/session-log/client`: `startSessionLog`, a lazy boot helper, and an app-event sink. Export types separately so importing types does not install global listeners. The caller supplies endpoint, build identifier, enabled-until timestamp, and explicit application event configuration.
- `@open-athena/auth/session-log`: shared event/batch validation, limits, time-boxed switch helpers, and framework-neutral endpoint handlers. Supply `identify(request)` and `requireAdmin(request)` callbacks; do not import disky's auth or cloud environment types. Preserve server-derived identity and prevent a client `email` field from claiming another person's identity.
- `@open-athena/auth/session-log/d1`: switch, batch deduplication, per-session summaries, list/detail queries, and bounded retention purge. Ship additive migrations independently of the core auth schema. No dependence on disky's `admin_edits` table: an optional audit callback records switch changes.
- `@open-athena/auth/react/session-log`: switch and sessions viewer components with app-supplied navigation/link rendering. Put styling in an optional CSS subpath. Avoid any import of disky treemap, assignment, store, or path-index logic.

Keep APIs small and use disky as the first adopter. A staged extraction can move client/shared/server pieces first and the React viewer second; both apps must remain deployable throughout. Do not migrate live disky databases or change its production logging switch as part of this handoff.

## Behaviors that must survive

Preserve the default-off, admin-controlled expiration (default seven days, maximum 31), bounded batches, 20,000-event page-load cap, `(sid,pid,seq)` idempotency, secret-query redaction, no auth headers or raw request bodies, no arbitrary input contents, and no DOM snapshots or keystrokes. Retention defaults to 30 days; purge through bounded deletes so large tables cannot fail one D1 statement. Server enforcement must stop expired logging even if an old browser keeps sending. Logger failures must never block the app or recursively log themselves.

General input logging must remain opt-in per named field. Navigation URLs, structured clicks/picks, API timing/status, errors, viewport, and visibility are useful defaults. App-specific events must have an explicit schema and allowed fields; do not log arbitrary objects that can contain credentials.

## CTBK integration

CTBK's preview/export UI is in `www/src/pages/MovieStudio.tsx`. Its hosted Worker is `www/server/worker.ts`; it uses `oa/auth` with any verified email for `movie:export`, with admin status kept separate. Job lifecycle events (`movie.queued`, `movie.failed`, `movie.cancelled`, `movie.download`) already have a typed audit hook. Add authenticated session-log boot at the app root, map those lifecycle actions and movie control changes into typed events, and mount the shared viewer at `/admin/sessions`. Both session-list and session-detail APIs must enforce admin identity on the server.

Session events should retain the movie query and app build so an admin can reopen the same view. Redact auth codes/tokens and signed S3 URL query parameters. A movie export's ownership comes from the authenticated server identity, not the browser's event body. User action logs and movie artifacts have separate retention policies.

## Verification and completion

Port precise tests for switch expiry, body/event caps, duplicate batches, identity spoofing, URL redaction, purge limits, and admin authorization. Exercise the recorder and shared viewer in disky and CTBK with real browser rendering. Ensure the default-off import adds only a small lazy boot shim. The initial package release should include both apps' adoption notes, required migrations, and checked-in pinned package revisions. Move this spec under `specs/done/` in the auth project with the completed change.
