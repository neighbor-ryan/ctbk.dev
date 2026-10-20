# Immutable GBFS generation retention

Implemented locally on 2026-10-20; deployment and live verification belong to the integrating controller. This preserves raw source observations separately from the existing latest-LU-per-minute availability contract in [LU attribution].

## Evidence

At 2026-10-20 11:47 UTC / 07:47 EDT, 706 settled UTC minutes had 582 primary minute keys, 706 backup minute keys, and 706 cron heartbeats. All 124 empty primary bins had backup observations. The screenshot's twelve gaps all had backup snapshots; six pairs of primary neighbors were only 61–62 seconds apart, and six were 120 seconds apart. Empty calendar-minute keys alone therefore do not establish a missed source generation.

The durable health drift series also recorded 105 minute keys containing two different observed LUs, usually at `:00` and `:59`. For example, both 07:43:00 and 07:43:59 were captured, while the minute WAL retained only the latter. Per-minute deduplication was deliberate for normalized histograms; applying it to the only raw archive discarded the earlier captured payload. The observed US CloudFront POPs and complete heartbeat trace do not support blaming skipped cron delivery or overseas routing for these particular bins. Upstream/cache phase jitter is evident; a complete census of missed origin generations is unavailable.

Evidence is saved under the repository's ignored `tmp/feed-diagnosis-20261020/`: `audit.json`, `screenshot-gap-neighbors.json`, `observed-primary-key-collisions.json`, and representative ranged headers. Historical payloads already replaced cannot be recovered from the primary minute objects; a backup generation is a different publish phase and is not guaranteed to be identical.

## Storage and consumers

- Write `gbfs/status-generation/<date>/<epoch-ts>.json` first, using R2 conditional create. Retries and cold-start duplicates retain the original payload and first `polled_at`.
- Hash the source timestamp plus slim station contents, excluding request provenance. If the same LU has different contents, preserve a separate `<epoch-ts>-<SHA256>.json` variant and log a warning. Exact repeats are suppressed in the isolate; out-of-order observations are archived without regressing normalized aliases.
- Keep `gbfs/status/<date>/<HH-MM>.json` for existing serving, minute coverage, and queue contracts. Compare-and-swap plus timestamp checks preserve the latest LU even when isolates finish out of order. The alias includes `generation_key` pointing to its immutable source.
- Hourly compaction lists canonical generation objects for the target hour alongside minute aliases and backup fills. It retains every distinct LU once, preferring its earliest recorded observation. Daily `compact-r2.py download` fetches the generation prefix into a separate local `generations/` directory; `compact` likewise includes canonical LUs and deduplicates aliases. Same-LU content variants remain archived as JSON rather than entering standard parquet with ambiguous duplicate timestamps.
- Backup fills account for immutable generations even if an alias publish failed. Existing R2 queue rules and minutely health listings remain unchanged; no new Cloudflare resources or bindings are required.
- `DailyStatusSource.parse` already selects the latest `ts` per `(station, LU minute)` before creating histogram rows. Its normalized minute counts and weighting remain unchanged when daily raw parquet retains several LUs in one minute.

## Rollout and verification

Deploy the poller and hourly compactor together with the updated daily compaction script. Readers tolerate old days without a generation prefix. No historical rebuild is needed to start retaining new observations; re-running compaction for dates after deployment can include their generation objects. Never delete raw generation files or advance a historical completeness claim solely because minute aliases exist.

After deployment, verify that a first captured generation is readable under its exact timestamp; a later LU in the same minute creates another object while the alias advances; retries leave the first object's `polled_at` unchanged; hourly and daily parquet retain both LUs; normalized minute bins still use one latest snapshot. Track storage/write volume separately: preserving raw generations adds roughly one JSON object and conditional write per observed LU in addition to the existing alias, and hourly compaction performs an additional date-prefix metadata listing.

Validation covers immutable collision retention, cold-start idempotence, same-LU content variants, failed raw-write ordering, out-of-order source observations, hourly parquet round-trip retention, alias deduplication, hour boundaries, backup fill preference, and Python daily input selection. No bulk production WAL downloads were used.

The hourly reader limits concurrent snapshot decoding to four and skips full alias reads whose listing metadata points to an included immutable generation. It streams decoded snapshots into preallocated scalar columns, interns station IDs/reported timestamps, and sorts the columns in place, avoiding both retained station objects and a second full set of sorted column arrays. A synthetic 120-generation × 2,500-station hour (300,000 rows) completed with Node's old-space limit set to 64 MiB, producing a 1,193,533-byte parquet; the initial unbounded/copying implementation failed that constraint. Node heap limits are not a substitute for live Cloudflare isolate verification, which remains part of deployment acceptance.

[LU attribution]: ../lu-attribution.md
