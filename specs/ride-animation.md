# Ride animation: every ride, moving

Status: June 2025 data, interactive prototype, and movie capture implemented and verified; full-history backfill and routed paths pending (2026-10-11).
Author: Ryan + Claude
Builds on: [`timelapse-map.md`] (the `/timelapse` page, `GLMap`, movie mode `?mv=1` / `window.__tl.seek`), [`timelapse-fine-bins.md`] (the linked bin/fps/range/length controls). Its "v3 idea" in the v2-arcs sketch was per-trip animation; this spec is that idea.

## Goal

Draw every ride as a moving dot with a short trail, from its start station to its end station, over its real start time and duration. v1 moves riders in straight lines. v2 moves them along plausible bike routes. The same page and movie mode render it both interactively and as MP4s.

## Scale (measured)

Sources: `rides-tl/start/1d` shards (`count_n` summed per day, over all of history) and a local projection of `normalized/202506.parquet`. The projection has `Start/Stop Time`, `Start/End Station ID`, `Rideable Type` and `User Type`, 4,836,609 rides ending in June 2025, 51 MB read over HTTP from `data.ctbk.dev/.dvc/files/md5/7a/4dc92b…`. Scratch outputs are in `tmp/norm-202506-*.txt` and `tmp/fine/anim-pack.txt`.

| quantity | value | source |
|---|---|---|
| rides, 2013-06-01 → 2026-08-05 | 325.6 M (≈330 M through Aug 2026) | Σ `1d` shards |
| peak day | **208,126** (2026-06-03); 2025 max 206,229; 2025 mean 127.7 k/day | `1d` shards |
| duration, June 2025 | mean 12.9 min, median 9.2, p90 25.5, p99 58.8, p99.9 166 min; max 94 h | projection |
| long rides | 0.94% > 1 h; 0.09% (4,334) > 3 h; 21 > 1 d | projection |
| **concurrent rides** (per-minute max), June 2025 | mean **1,461**, p99 4,232, **peak 4,970** (2025-06-04 18:21, a 195 k-ride day); 18:00–19:00 hour mean 4,665 | ±1 event sweep |
| concurrent, at the 208 k peak day | ~5.3 k (scaling the above ×1.07) | extrapolated |
| round trips (start = end station) | 2.28% (June 2025); 2.06% (12 months) | projection; `se_c` |
| bike / user type | 70.7% e-bike; 20.0% `Customer` | projection |

The envelope estimate (150 k rides on a peak day, 15 min mean, so ≈1.5 k average and 3–4 k at the peak hour) got the average right. The peak is higher, ~5 k, because peak days now reach ~208 k rides. Five thousand moving dots is still trivial for deck.gl.

## Ride index

This is not a pyramid. Nothing here is summed; it is a **time-sorted leaf table**, chunked per day.

### Record

| field | type | notes |
|---|---|---|
| `Δstart` | LEB128 varint, seconds | delta from the previous ride's start, with the first ride relative to local midnight. Max gap on 2025-06-04 was 75 s (fits 1 byte); winter nights in 2013 may need 2 |
| `dur` | u16 seconds | capped at **3 h** (10,800). This keeps 99.91% of rides exact. Longer rides get `dur=cap` plus a `long` flag (drawn fading out, not arriving) |
| `ss`, `es` | u16 | indices into the month's station table |
| `flags` | u8 | bit 0 e-bike, bit 1 casual, bit 2 long (capped), bit 3 unknown bike type, bit 4 unknown user type |

The file is stored columnar (all `Δstart`, then all `dur`, …) and gzipped. **Measured on 2025-06-04**: 195,324 rides, 8.0 B/ride raw, **5.2 B/ride gzip-9**, **1.02 MB/day**. On 2025-06-15 (101 k rides) it was 5.3 B/ride. Brotli (R2/CF re-compress) should do slightly better; that is not measured. Full history is ~330 M × 5.3 B ≈ **1.75 GB**.

Tie order is `(start_ms, ss, es, dur)`, so a ride's **ordinal within its day file is a stable id**. `Ride ID` exists only from 2021-02, so it can't serve as the id.

Implemented wire format: `CTRA`, version `1`, reserved `0`, station count `u16`, record count `u32`, carry-in count `u32` (16-byte little-endian header). First start offset is signed zigzag LEB128; following offsets are unsigned deltas. Columns then contain durations, source indices, destination indices (`u16`) and flags (`u8`). Timestamps and durations are quantized to whole local-clock seconds; ties sort by `(start_seconds, ss, es, dur, flags)`. Both decoders reject invalid bounds, column lengths, carry-in, and noncanonical ordering. gzip has `mtime=0`.

### Chunking: per day, with carry-in

Each day file also carries the **carry-in**: rides that started in `[t0 − 3h, t0)` and are still in progress at `t0`, with negative start offsets. The cap bounds the look-back. On 2025-06-04 there were 576 carry-in rides (0.3%), and on 2025-06-15 there were 816. The alternative is hour chunks, whose carry-in would be the concurrency: 4,738 in progress at 18:00 against 19,528 starting in that hour, **+24%**. Hour files would also multiply request count by 24. So: day files, fetched whole (≤ ~1 MB), with the next day prefetched at 50% playback as `/timelapse` chunks are.

### Serving: static R2 files, no worker

- Keys: `rides-anim/v1/{YYYY}/{MM}/stations.json` (index → `{id, lat, lng}`, ~2.5 k entries) and `rides-anim/v1/{YYYY}/{MM}/{DD}.bin.gz`. These are DVX-tracked, so they are content-addressed under `data.ctbk.dev/.dvc/files/md5/…`. A small `rides-anim/v1/index.json` (day → md5, rides, bytes) is regenerated with each month and is the one mutable object (short `max-age`). Every day file is then immutable (`max-age=31536000, immutable`), CORS-open and edge-cached.
- There is no `/api/rides-anim` endpoint. A worker would only add a hop, since the files are already the exact unit the client wants and need no bbox filter. At ~5 k concurrent dots, viewport culling happens on the GPU for free.
- Canonical ids: the station table maps raw ids to canonical positions (`c:` clusters, from the same id-map as `/api/tl`), so merged stations draw at one point.

### Build

- **Monthly**: a new `ctbk update` stage, `ctbk rides-anim create -d <YM>` (a `MonthTable`, DVX-tracked, after `cons`). It reads `normalized/<YM>.parquet` plus `<YM+1>` when available, for rides that start in `YM` but end in the next month, and writes ~30 day files. This takes seconds, one month of rides being ≈ 5 M rows × 6 columns. When month `M+1` arrives, day files at `M`'s end are rewritten, because they gain the late-ending rides. DVX content hashes make this a normal re-run.
- **Backfill**: 160 months, run on `e` (not the laptop). It reads the six needed columns (~60–80 MB per recent month) and writes ~1.75 GB. Batch is optional; it is embarrassingly parallel over months.

## Rendering (v1)

Implemented candidate: `ctbk rides-anim create -d YYYYMM [-i LOCAL_NORMALIZED_DIR] [-u]`, `check -i BUILD_DIR`, and resumable `backfill -d YYYYMM-YYYYMM [-u]`. Projected R2 inputs are cached by ETag; checkpoints also compare the canonical map and station table hashes. Hashed day/station objects are published under `rides-anim-candidate/v1/objects/<md5>.*`; the mutable index uses 60-second caching. This candidate publication is content-addressed independently of DVX; formal `MonthTable`/DVX integration remains pending. It accounts separately for invalid-duration and unmapped rides.

June 2025 has 4,836,455 source and encoded rides, zero invalid-duration or unmapped rides, and 4,363 rides capped at three hours. Every one of its 30 daily counts equals the fine pyramid's `1d` start count exactly (`ctbk rides-anim check`); all daily files plus the station table total 25,713,970 bytes. June's local consolidated input plus projected July spillback are recorded in `tmp/animation-june/month.json` with the remote July ETag. Candidate index and files are published and publicly readable.

The browser mode is `/timelapse?anim=1&b=1m&d=250610-250610&t=250610T1800&len=60`. It shares linked controls and the movie clock, buffers on animation data, prefetches the next day after noon, and uses only the current day's padded carry-in (no adjacent-day double drawing). Geometry stays stable within 15-minute buckets; GPU trails use local seconds, with moving dot heads and round-trip loops. Long rides fade out; colors distinguish bike/user type and unknowns. This is explicitly labeled a June sample with inferred straight paths. Candidate mode still shares count-series queries for the scrubber; it does not require them to advance the clock.

Verification: 219 hermetic Python tests; 159 frontend unit tests and TypeScript; eight existing browser checks plus the animation browser check (exact concurrency 3,870 at 18:00 and 3,851 at 18:01; deterministic movie seek to 18:00). Desktop and 390×844 phone rendering/settings checked in HCCSx Chrome. Movie artifact `www/tmp/movies/ride-day-smoke.mp4`: 720 frames at 12 fps, 640×640, exactly 60.000000 seconds by ffprobe; the evening frame has visible trails and 3,870 rides. `e` was unreachable over SSH; `rides-anim submit` prepares a pinned HCCS Batch run using existing reproc capacity and R2 Secrets Manager bindings, with 4 vCPU/8 GiB and checkpoint resume after Spot interruption.

- deck.gl `TripsLayer` (`@deck.gl/geo-layers`, a new dependency beside `@deck.gl/layers` 9.4). Each ride is a path of `[start pos, end pos]` with timestamps `[start, start + dur]` in **seconds since the day's midnight**. float32 is exact to 1 s over a day, which is why the time base is per day and not epoch.
- **Data feed**: one layer per loaded day. The GPU filters by `currentTime`, so an entire day (≤ 210 k two-vertex paths, ~7 MB of attributes) is uploaded once, not per frame. Around midnight, both adjacent days' layers render; carry-in rides are dropped from day `d+1`'s layer when day `d` is loaded, which avoids double-drawing. `binary` data (typed arrays straight from the unpacked columns) avoids JS object churn.
- **Trails**: `trailLength` in sim-seconds (default 120 s), with `fadeTrail`. Color by `flags`: e-bike vs classic (default) or member vs casual; round trips (2%) could get a distinct hue, or a small arc (below).
- **Straight-line v1**: position(t) = lerp(start, end, (t − start)/dur). Round trips would sit still at their station. Instead, draw them as a small loop, a 3-vertex path out ~150 m toward the bearing of the next-most-common destination and back, so they are visible without faking a route.
- **Station layer underneath**: the `/timelapse` `flow` preset over a matching window (`w = trailLength`) shows departures and arrivals as the dots leave and land.
- **Controls**: the playhead is continuous time. The linked controls from [`timelapse-fine-bins.md`] apply with `bin` read as "sim-seconds per rendered frame": for example, 1 day at 30 fps in 60 s is 1,800 frames of 48 s each. `range = bin · fps · length` still holds.
- **Movie mode**: this reuses `?mv=1` and `window.__tl.seek(i)`, where `seek` sets `currentTime = rangeStart + i·bin` and awaits the day file(s) covering `[t − trailLength, t]`, deck's `onAfterRender`, and MapLibre `idle`. `TripsLayer` interpolates purely from `currentTime`, so frames are deterministic. The same black-canvas and `preserveDrawingBuffer` rules apply.

## Routes (v2)

Straight lines cut across buildings and rivers. The fix is to route every observed pair offline and animate along the route.

### Pair counts (measured)

These are from `aggregated/se_c_YYYYMM.parquet` (the monthly pair counts behind `spj`), 2025-09 → 2026-08, raw ids (`tmp/sec/pairs-stats.txt`):

- **1,640,937 distinct (start, end) pairs** in 12 months, over 2,515 stations (~26% of the 6.3 M possible). A single month has 310–786 k. 437 k pairs appear once in the year.
- **Concentration**: the top 60 k pairs carry 50% of rides, 420 k carry 90%, and 1.19 M carry 99%. Pairs with ≥ 10 rides/yr number **544 k and carry 93.2%** of rides; pairs with ≥ 2 number 1.20 M and carry 99.1%.
- Straight-line distance (from `gbfs/engine/station-geo.json`): the ride-weighted mean is **2.0 km**. The per-pair mean is 4.84 km (p99 13.6 km), because rare pairs are long.
- For all of history, raw ids churn across eras, and the geometric pairs (after canonicalization) are unmeasured. A guess is 2.5–3.5 M.

### Routing

- **Engine**: OSRM with a customized `bicycle.lua`, or Valhalla `bicycle`. Either takes an NYC-plus-Jersey-City OSM extract and requests `alternatives=3`. Valhalla's bike costing models bike lanes and hills better; OSRM is faster and simpler to batch. Try both on 1 k pairs and compare against a few hundred e-bike GPS tracks if any public ones exist; otherwise just eyeball them.
- **Where**: a Batch job (container: OSRM plus the extract, preprocessed once at ~1–2 GB RAM for NYC) runs over a pair list sharded by origin. As a guess, 1.6 M pairs at ~2–5 ms per route with alternatives on 16 vCPUs is **~5–10 min**. That is ungrounded, so time a 10 k sample first. The run repeats only for new pairs (a monthly delta) and on OSM refreshes (yearly).
- **Thinning**: route pairs with ≥ 2 rides/yr (1.2 M, 99.1% of rides). The rest draw straight-line.

### Storage

- Each route is Douglas-Peucker simplified at ~5 m and stored as a Google-encoded polyline (precision 5). Assumption, not measured: a ~6 km route (1.3× the 4.8 km pair mean) has ~20 vertices, since the grid means few turns, at ~4 B/vertex, so ~**80 B per route**.
- Totals: all 1.64 M pairs × 3 alternatives ≈ 390 MB raw; the ≥10 rides/yr set (544 k × 3) ≈ 130 MB raw. gzip makes both smaller.
- **Per-origin files**: `routes/v1/{origin}.json.gz`, each `{dest: [[len_m, polyline], …]}`. At ~52 KB average (≥10 set) they are lazy-loaded for the origins in the loaded day's rides. A whole-city day touches nearly every origin, so the first play of a day pulls most of the ~130 MB. After that everything is cached immutable. If that proves too heavy, the fallback is **per-day route bundles**: only the (pair, alt) used that day, deduplicated, written next to the day file. That costs ~100 k distinct pairs × 80 B ≈ 8 MB/day raw, a larger per-day cost but none up front. Decide from measurements.

### Per-ride route choice and timing

- Choice: `alt = weightedPick(hash32(day, ordinal), weights)` with `wᵢ ∝ (len_min / lenᵢ)^α`, α ≈ 4, so near-equal alternatives split and long detours are rare. The pick is deterministic, so every viewer and every movie render sees the same route.
- Timing: position(t) = the point at arc length `len · (t − start)/dur` along the polyline, which is constant speed. `TripsLayer` does this natively once per-vertex timestamps are set to `start + dur · cumlen/len`. Those are computed in the browser when a day is assembled (~210 k rides × ~20 vertices ≈ 4 M vertices, ~64 MB of attributes). That is heavy, so upload a sliding 2-hour window of rides instead of the whole day.
- A ride's duration is often longer than the route explains (stops, detours, an idle ride-share). In that case the dot moves slower, uniformly; there is no invented dwell.
- **Fallbacks**: pairs with no route (rare pairs, or new stations before the next routing delta) draw straight lines. Round trips keep the v1 loop, since there is no meaningful route.

## Streaming vs batch-rendered movies

- **Streaming (interactive)**: the default. Day files plus lazy routes, with deck.gl rendering live. It is cheap to host, since the files are static and immutable.
- **Batch-rendered**: movie mode through `scrns` on a headless browser, per [`timelapse-map.md`]. For long or high-fps movies (a year at 1 day/2 s), run it on `e` or Batch with the deterministic Docker setup. The outputs (MP4s on R2) are the showcase artifacts. A server-side renderer (headless deck.gl via `@luma.gl` + node GL, or a Python/Datashader path) isn't worth building while scrns determinism holds.

## Privacy

Citi Bike already publishes the source data per ride: start and end time, station and bike type, with no rider ids since 2021. Before 2021 there were also gender and birth year, which this index **drops**. Animating it reveals nothing new. The index keeps no `Ride ID`, and routes are inferred, not observed. One nuance: a rare pair at an odd hour is individually distinctive in the raw data too, so nothing changes. Still, don't add rider attributes (gender, age) as colors.

## Phases

- **A0: estimates** (this doc). Done: concurrency, packing, pair counts. Still open: the routing timing sample and the route vertex count, both measured on a 1 k-pair OSRM run on `e`.
- **A1: ride index**. The `rides-anim` stage and DVX outputs for one month, the day file format and a TS decoder (golden test: decode → re-encode is byte-identical, and count equals the `1d` shard's `count_n`), then the backfill on `e`.
- **A2: straight-line page**. `/timelapse?anim=1` (or `/rides`?), `TripsLayer`, the trail and color presets, the station layer underneath, and the round-trip loops.
- **A3: movie mode**. `seek` and scrns entries (a summer weekday evening rush in 60 s; one full day).
- **A4: routes**. The OSRM sample, then the full run on Batch (**needs approval**), per-origin files, deterministic choice, polyline timing, and the per-day-bundle decision.

## Open questions

- Page shape: a mode of `/timelapse` (sharing controls, range and movie mode) or its own route? The default is a mode (`anim=1`), because the linked controls and seek API are identical.
- Duration cap: 3 h (99.91%) or 2 h? Long rides are mostly docking failures and lost bikes. Should they draw at all?
- Station moves: does a per-month station table (above) handle relocations well enough?
- Pre-2021 data has no bike type. Color by user type there by default?
- Could routes also improve the v2 arcs in [`timelapse-map.md`] (arc = drawn route, width = count)?

[`timelapse-map.md`]: timelapse-map.md
[`timelapse-fine-bins.md`]: timelapse-fine-bins.md
