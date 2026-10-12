# Routed flow map: selection flows along streets, merging and splitting en route

Status: real 1,000-pair sample and browser assignment implemented and verified; full route population, direction cues, and system layer pending (2026-10-11). Depends on the route set from [`ride-animation.md`] §Routes (phase A4).

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
