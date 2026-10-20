# Routed flow map: selection flows along streets, merging and splitting en route

Status: complete JC/Hoboken pair catalog, direction chevrons, opposing lanes, and routed animation implemented and verified (2026-10-12). Full NYC routing and the system layer remain pending. Depends on the route set from [`ride-animation.md`] §Routes (phase A4).

City rollout update (2026-10-20): the bounded city builder, exhaustive sparse-catalog audit, monthly network assignment, and optional network UI are implemented locally. Remote image, representative benchmark, full city publication, and real monthly network publication still need execution; no full-city coverage is claimed below.

## Goal

On `/stations`, the "where riders go" fan currently draws one straight arc per destination (merged per destination since `b9d45879`), with width set by trip count. Instead, draw the selection's flow **along plausible street routes**, so that:

- trips heading the same way share a street and its line thickens (merge);
- they peel off toward their destinations (split);
- rivers, the Hudson and the PATH tunnels are respected, since no line crosses water except on a bridge.

This is a classic traffic-assignment picture: each pair's trips are assigned to its route(s), and volumes are summed per street segment. One rendering serves a single station, a multi-station selection, and (later) the whole system ("busiest streets").

## Why routes and not edge bundling

Force-directed or hierarchical edge bundling of the straight arcs is FE-only and cheap, but it bundles by geometry, not by streets. Bundles cut across blocks and over the Hudson, and they shift whenever the selection changes. The routes already planned for ride animation (A4) give real, stable streets, and the per-segment sums fall out of data that is built anyway. Bundling stays the fallback, used if A4 slips: arcs today, routes once they exist.

## Data: routes as segment ids, not only polylines

A4 stores each route as a simplified polyline, which is enough to animate one dot but not to sum volumes. Flows need **shared segment identity**: two routes on the same block must reference the same segment.

- **Segment graph**: from the routing engine's node annotations (OSRM `annotations=nodes`, Valhalla `trace_attributes`), take each route's OSM node sequence. Over all routed pairs, contract the union to **segments**: maximal node chains with no branching *in the used set*. Each segment gets an id, a simplified polyline (~5 m DP) and a length. A guess: 30–60 k segments for NYC + JC, ~2–3 MB gzipped as one `segments.json.gz` (unmeasured).
- **Route → segment list**: each (pair, alt) becomes a varint delta-coded list of segment ids, with a direction bit per segment. A guess is ~25 segments per route at ~2 B each, so ~50 B per route; that is smaller than the polyline, which can then be *derived* from the segment list instead of stored separately. If that holds, A4 stores only segment lists, and animation and flows share one format.
- **Indexes**: flows need both directions. "out" for a set needs routes *from* each set station, and "in" needs routes *to* it. Store per-origin files (A4's `routes/v1/{origin}`) plus per-destination files with the same records keyed the other way. That roughly doubles route storage (a guess of ~2 × 80 MB for the ≥10 rides/yr set, gzipped smaller), with each file lazy-loaded per selected station.

Full-population sizes remain estimates; the measured sample is recorded below.

## Assignment (browser)

For a selection `S`, direction `d` and window (the same `pairCounts` input as `flowArcs` today):

1. For each pair `(s, o)` with count `c`, take its alternatives with weights `wᵢ ∝ (len_min / lenᵢ)^α` (A4's α ≈ 4), normalized.
2. Add `c · wᵢ` to every segment of alternative `i`. This uses the **expected** split, not a sampled one, so the map is deterministic and smooth. Animation samples per ride; flows do not.
3. The result is `vol[segment]` for the segments touched (a few thousand for one station).

Cost: one station with ~1,000 destinations × 3 alts × 25 segments = 75 k additions, well under a millisecond. A system-wide month (544 k pairs) is ~40 M additions, ~100 ms in a worker. That is fine for an occasional view, but **precompute** the system layer (below).

Pairs with no route fall back to a straight arc in the same style, slightly dashed so the reader can tell it apart.

## Rendering

- deck.gl `PathLayer` over the touched segments. Width `∝ vol` (the same `arcWidthPx` scale as today: max 12 px, floor 1 px), `widthUnits: 'pixels'`, round caps and joins so consecutive segments read as one line.
- Draw light to heavy, so heavy trunks sit on top. Same neutral hue as the arcs (white in dark mode, `#282830` in light), with alpha by sqrt share.
- **Direction**: for one direction (the default `out`), a segment's flow has a direction. Show it with subtle animated dashes (`PathStyleExtension` + a dash offset over time) or small chevrons every ~200 px; the dashes double as a cue that something is moving. For `both`, draw the two directions as offset lanes (`getOffset` ±½ width), like a two-way street.
- Destination circles keep the flow-encoding v2 sizing, since lines now end exactly at the stations.
- The cutoff becomes per segment: drop segments below 4% of the heaviest segment, and also drop whole pairs below the current pair floor, so the faint tails don't speckle the map.

## System layer: busiest streets

A per-month precomputed `vol[segment]` over all rides (both directions summed, plus a per-direction split) is a cheap static file (~30–60 k floats, ~100–200 KB) that answers "which streets carry Citi Bike". It can be:

- a `/stations` base layer, shown when nothing is selected;
- a time-mode layer per month (or per hour of day, as a stretch), which animates the network breathing over a year.

Build it as a stage alongside A4: (monthly `se_c` pair counts) × (route segment lists) → `flows/v1/{yyyymm}.bin`. It reruns monthly and on a routes refresh.

## Phases

- **R0 (with A4's sample)**: also request node annotations in the 1 k-pair OSRM run; measure segments per route and the size of the contracted graph. Decide whether routes are stored as segment lists only.
- **R1: segment graph + route lists** for the ≥10 rides/yr set, per-origin and per-destination files. The full-roadmap authorization covers the measured build; resource choices should follow R0's measurements.
- **R2: FE assignment + `PathLayer`** on `/stations` behind `?flow=routes`, with arcs as the fallback. Tests: exact `vol` for a fixture of three pairs with two alternatives each.
- **R3: system layer**: monthly `flows/v1` files, the empty-selection base layer, then the time-mode hookup.

## Open questions

- Engine choice (OSRM vs Valhalla) is A4's decision. One flows-specific point: Valhalla's bike costing prefers lanes and greenways, so its trunks look more like real riding.
- Bridges and the PATH: e-bikes on the Manhattan Bridge are real, but are bike-to-train trips (Hoboken ↔ Manhattan "rides" that are really PATH rides) worth special handling? Cross-river pairs are rare in this data, since JC/Hoboken and NYC are separate systems; check the counts.
- Should lines carry **time** as well (width = volume, color = typical hour of day or mean speed)? That would reintroduce color as a second channel, which flow encoding v2 deliberately moved away from. Default no.
- For multi-station selections, should flows *between* selected stations be shown, as internal flow? The arcs exclude them today.

[`ride-animation.md`]: ride-animation.md

R0 implementation: `ctbk flow-routes sample` reads the published June animation month, excludes midnight carry-in from pair counts, and selects 1,000 non-round-trip pairs stratified by source region (250 JC, 250 HB, 500 NYC, filling undersized strata from remaining ranked pairs). It calls only a self-hosted localhost bicycle router, requests up to three alternatives with full geometry and OSM node annotations, records snap distances, and keeps no-route/no-segment or >250 m snaps as explicit fallbacks. Weights use the specified alpha of four. Shared OSM chains are contracted at branches, route endpoints, and reversals; signed segment IDs retain direction. Response caching is scoped to the OSM MD5 and pinned engine. Tests verify exact shared chains/directions, alternative weights, carry exclusion, and the complete sample payload/summary.

Heavy work runs through `.github/workflows/routes-sample.yml` on a public hosted runner, with no local graph/image download. OSRM 26.10.0 amd64 image is pinned to digest `84074e1e66bd7eedc7fe52ece0f4deb2e6865454855ae77675c5f753d281fd58`; osmium is pinned to 4.3.1. The BBBike NewYork polygon covers longitude -74.36…-73.67 and latitude 40.48…40.96, including NYC and Jersey City. The workflow prepares the bicycle MLD graph, measures the sample, and publishes only hashed candidate geometry plus a short-lived index at `routes-sample/v1`. The extract MD5 and engine digest are part of the measurement provenance. Full OSM graph files remain ephemeral on the runner; only compressed geometry and summaries are retained.

Measured run `38197223911` succeeded: 1,000 routed pairs (250 JC, 250 HB, 500 NYC), 1,772 alternatives, 3,341 segments, 25,877 vertices, 863,061 compressed bytes, and no no-route/snap fallbacks. The download host was changed to `data.bbbike.org` with bounded retries after the original host timed out from GitHub's runner. Object `objects/0c3b0db6cf5179fe9fcc80d3c0124458.json.gz` records extract MD5 `b0e185c2d2c7564b8e186f2258b93880` and the pinned engine. Only the 863 KB candidate and small metadata were downloaded locally.

R2 sample preview: `/stations?fan=1&flow=routes` adds an “Inferred bike routes” toggle. Assignment uses each actual directed pair even when several sources are selected, excludes internal pairs, applies the existing destination cutoff, sums alternative-weighted volumes onto signed segment IDs, and drops segments below 4% of the heaviest. Street widths use the existing flow scale; labels round expected fractional volumes. Missing pairs remain arcs, and the legend reports routed trip share. This is partial sample coverage (54% of Columbus Drive's displayed June flow), not a full street network. Opposing directions are separate assignment volumes but currently overdraw on the same geometry; animated directional marks, offset lanes, dashed fallback arcs, full per-station route indexes, and the system layer remain pending.

Animation consumes the same candidate's full route geometry with deterministic FNV choice keyed by absolute ride start, source, destination, duration, and flags. Identical encoded ride tuples share a choice; carry-in preserves it across midnight. Added station connectors retain exact endpoints. Timestamps are proportional to approximate local geographic distance along the polyline, and binary-search head interpolation avoids scanning long paths every frame. Round trips and uncovered pairs keep existing inferred loops/straight paths. Routes are plausible bicycle paths, not observed rider tracks.

Population sizing: `ctbk flow-routes census -d YYYYMM-YYYYMM` counts each published ride once (excluding daily carry-in), verifies object hashes and indexed counts, and computes the directed non-round-trip pair population above the ≥10/year threshold. It reports selected trip coverage, per-region pair counts, round trips, and animation-source exclusions. The current command requires a shared station table across the selected months; mixed vocabularies raise explicitly. `.github/workflows/routes-census.yml` runs the yearly count remotely and publishes a compressed content-addressed pair-count candidate at `routes-census/v1`; only its small summary is retained as an artifact. This sizing precedes the full route build and per-origin/destination indexes.

Street trunks and uncovered fallback arcs share one width/opacity maximum, including multi-source selections, so an uncovered pair is not independently scaled up to look as heavy as a merged street trunk. The legend keys both line types on that common trip scale. HCCSx rendering verified on Columbus Drive's June preview.

Annual census run `38200639540` passed for 2024: 45,209,365 source rides, 45,209,026 encoded (339 invalid-duration exclusions, zero unmapped), 1,565,085 distinct non-round-trip pairs. The ≥10/year population is 516,902 pairs (HB 1,858; JC 3,363; NYC 511,681), carrying 41,136,758 of 44,163,355 non-round-trip rides (93.1%). Round trips total 1,045,671. The compressed count/coordinate population is 1,978,313 bytes at `routes-census/v1/objects/e62a5fcb6002caa3657daa3a6974ba7b.json.gz`. Full geometry and routing runtime will be substantially larger than the 1,000-pair preview; the full builder needs bounded storage, checkpoints, and per-origin/destination files before adoption.

## Complete JC/Hoboken catalog

`ctbk flow-routes nj` covers all 15,006 ordered nonroundtrip pairs among the 123 JC/HB IDs in September 2026's shared animation station table, including historical IDs and low-volume pairs. Canonical coordinates and the current bicycle graph produce inferred paths, not historical rider traces. New IDs or coordinate changes require a catalog rebuild. NYC retains the sample's NYC subset; round trips retain inferred loops.

HCCS Batch job `15b674a0-fe2e-4036-ad57-40a90133bdb8` succeeded using pinned image `688066488567.dkr.ecr.us-east-1.amazonaws.com/ctbk-reproc:routes-425034f3` (workflow `38265871693`), x86_64, 4 vCPUs and 16 GiB. It used the existing HCCS queue and Secrets Manager bindings; the RAC `e` VM was not used and no new VM was needed. Bulk extracts and routing files stayed in Batch. An immutable R2 OSM snapshot and one checkpoint per origin support Spot retries. Context hashes include station coordinates, OSM hash, engine, alternatives and snap threshold. Only exact-key reads are used.

All pairs routed, with zero fallbacks: 29,956 alternatives, 3,157 contracted segments, 15,839 vertices. The shared graph is 162,645 bytes compressed; graph plus origin and destination catalogs total 2,585,002 bytes. Public prefix `routes-nj/v1`; graph `objects/6ef5f13119b5a487845822fa4c2412f7.json.gz`; context `0961e50eec501c433219c8a356aed6d4`; OSM MD5 `b0e185c2d2c7564b8e186f2258b93880`. `ctbk flow-routes check-nj` verified every object's hash/size, exhaustive pair coverage, identical origin/destination records, normalized positive alternative weights, signed segment references, continuous geometry and indexed accounting. Only small catalogs/metadata were read locally.

The frontend lazy-loads selected origin files for outgoing flows and destination files for incoming flows. Animation loads origins relevant to its current 15-minute bucket plus trail, reconstructs geometry from signed segments and waits for the catalog before movie capture. NJ pairs override sample records in a separate graph namespace. Static forward chevrons use screen-pixel spacing; opposing flows use separate right-hand lanes. Width/opacity retain the common flow scale. HCCSx Chrome verified JC outgoing/incoming and 390×844 Hoboken views, all showing 100% routed. June 10 at 18:00 retains exactly 3,870 active rides, with 110 routed versus the sample's 84. The count-map link now animates the current day and instant. Unit fixtures and browser tests cover these behaviors and deterministic movie seeking.

Animation deliberately retains NJ origin catalogs plus the NYC sample and explicitly disables the city catalog, including its shared graph. Expanding `rideRouteOrigins` to all IDs without another delivery strategy would start up to 1,668 origin queries in September 30's peak 15-minute bucket plus 120-second trail (1,599 NYC, 69 NJ), and retain every destination route in those origin catalogs. Full NYC animation needs bounded pair/bucket bundles or another measured loading budget before this guard is removed. Selected-station city routing and preaggregated whole-network volumes remain available independently.

## City builder and whole-network rollout

`ctbk flow-routes city` builds the immutable census population into `routes-city/v1`, reusing the exact existing `routes-nj/v1/inputs/20261012/NewYork.osm.pbf` snapshot and pinned bicycle engine. It checkpoints each origin against station coordinates, OSM hash, engine/options, and the selected population. A representative 10,000-pair benchmark uses `-n 10000 -k routes-city-benchmark/v1`; it samples evenly over sorted source/destination pairs rather than just cheap short high-volume trips. A cap cannot overwrite the production prefix. `submit-city -N -k routes-city-benchmark/v1 -n 10000 -s IMAGE` prints the plan; without `-N` it verifies the HCCS account and submits to the existing queue with 4 vCPU, 16 GiB default, a six-hour hard timeout, and Spot retries. The image must contain the committed implementation; the older NJ image does not.

The city builder streams node annotations in two passes over per-origin compressed checkpoints, retaining shared OSM topology instead of all annotated routes. It spools the reverse index to SQLite and reads one destination at a time on the caller thread. This avoids the NJ prototype's full node-path retention and repeated whole-population scans. The immutable graph and both lazy catalog directions retain signed segment IDs. Measured timing and parent/child peak RSS are included in the summary; these RSS figures are separate process peaks, not a simultaneous combined memory measurement. `ctbk flow-routes check-city` verifies object hashes/sizes, exact sparse census pair coverage, both catalog directions, positive normalized alternatives, continuous signed segment chains, accounting, and exact census street-volume parity.

CA catalog publication uses at most eight concurrent uploads, consuming origin encoding and SQLite destination reads on the caller thread. Only eight catalog payloads can remain queued; the ordered result map and deterministic compressed objects match serial publication byte-for-byte. Routing/checkpoint computation remains serial. Separate topology/publication timings and stage logs expose the post-routing work.

Build the full rollout under `routes-city-candidate/v1`, then run `check-city -k routes-city-candidate/v1` and `promote-city` in the same remote task. The checker uses eight bounded catalog reads while preserving origin/destination iteration and floating-point aggregation order on the caller thread. Its small report binds acceptance to the exact candidate index MD5. Promotion refuses capped catalogs or a changed index, server-side copies only indexed CA objects with source size/hash checks and conditional copies, and publishes the production `index.json` last. It copies no checkpoints and downloads no catalog bodies to the laptop. Server-side copy and source ETag conditions are supported by R2's [S3 API compatibility table][r2-copy].

The existing 2024 census is a sizing baseline, not a current-population guarantee. Refresh `routes-census.yml` for `202510-202609` before the full build; September remains provisional. Catalogs retain the immutable census object key, so a later census refresh does not invalidate an older build's audit. New or rare pairs outside the chosen population continue to use explicit fallback arcs/straight animation paths. The full JC/Hoboken catalog still overrides the city/sample namespace for its covered pairs.

`ctbk flow-routes system -d YYYYMM` streams the month's immutable animation days, verifies hashes/counts, excludes carry-in and round trips, and assigns actual monthly directed-pair counts to route alternatives. It publishes `flows/v1` with exact encoded/routed/fallback/roundtrip accounting, source object hashes, provisional status, the route context and graph key, and expected signed street volumes. Missing routes are reported as uncovered volume rather than invented street traffic. `.github/workflows/routes-system.yml` runs this remotely for one month with a serialized writer; monthly automatic extension and time-mode interpolation remain follow-up work.

`/stations?m=2506&network=1` shows the opt-in whole-network layer with direction chevrons and opposing lanes when no station is pinned. It loads one monthly volume object and one shared graph, without downloading every origin catalog. The legend reports the actual non-roundtrip routed share and explicitly names an unbuilt month. Selections continue to use lazy origin/destination catalogs; a missing city index keeps today's NJ/sample behavior. Unit tests cover exact directional expected volumes and floors; a complete hermetic two-origin build/audit tests both indexes and checkpoint resume; the monthly fixture proves carry/roundtrip exclusion and uncovered-pair accounting. Browser checks cover network opt-in/reload/view preservation and an unbuilt month. Chrome CIC could not connect because the installed browser runtime references a missing service module; automated Chromium rendering was checked instead, without claiming HCCSx verification.

[r2-copy]: https://developers.cloudflare.com/r2/api/s3/api/
