/**
 * GBFS hourly compactor — Cloudflare Worker with cron trigger.
 *
 * On each fire (cron `5 * * * *`), compacts the previous UTC hour's
 * raw generations and legacy minute JSONs into a single parquet shard:
 *
 *   gbfs/status/YYYY-MM-DD/HH-MM.json   (n0, ~580 KB / file × ≤60)
 *   gbfs/status-generation/YYYY-MM-DD/<LU>.json (all captured generations)
 *      ↓
 *   gbfs/avail/h1/YYYY-MM-DD/HH.parquet  (h1)
 *
 * Schema mirrors `compact-r2.py`'s daily compaction so downstream readers
 * (`gbfs/api`) see one consistent format across tiers:
 *   station_id   STRING
 *   ts           INT64    (GBFS last_updated)
 *   polled_at    INT64    (our poll time)
 *   num_*        INT32    (daily compaction uses INT16, but hyparquet-writer
 *   is_*         INT32     has no INT16; Snappy handles the high-zero bytes
 *                          and `parquetReadObjects` returns plain numbers
 *                          either way)
 *   last_reported INT64
 *
 * Idempotent: re-runs overwrite the same h1 key. Manual trigger (gated by
 * the `COMPACTOR_SECRET` env var, set via `wrangler secret put`):
 *
 *   curl -H 'x-compactor-secret: ...' \
 *     'https://.../compact?date=YYYY-MM-DD&hour=HH'
 *
 * Useful for backfill or after writer-config changes (e.g. rg-size bumps).
 */

interface StationRow {
	station_id: string;
	num_bikes_available: number;
	num_ebikes_available: number;
	num_docks_available: number;
	num_bikes_disabled: number;
	num_docks_disabled: number;
	is_installed: number;
	is_renting: number;
	is_returning: number;
	last_reported: number;
}

interface Snapshot {
	ts: number;
	polled_at: number;
	stations: StationRow[];
}

/** Alias and immutable inputs may describe the same LU. Keep its first
 * observation once; same-LU content variants remain in the immutable JSON WAL. */
export function distinctSnapshots(snapshots: readonly Snapshot[]): Snapshot[] {
	const byLu = new Map<number, Snapshot>();
	for (const snapshot of snapshots) {
		const prior = byLu.get(snapshot.ts);
		if (!prior || snapshot.polled_at < prior.polled_at) byLu.set(snapshot.ts, snapshot);
	}
	return [...byLu.values()].sort((a, b) => a.ts - b.ts);
}

export function generationKeysForHour(keys: readonly string[], date: string, hour: string): string[] {
	const start = Date.parse(`${date}T${hour}:00:00Z`) / 1000;
	return keys.filter((key) => {
		const match = key.match(/\/(\d+)\.json$/);
		const ts = match ? Number(match[1]) : NaN;
		return ts >= start && ts < start + 3600;
	});
}

interface Env {
	R2: R2Bucket;
	COMPACTOR_SECRET?: string;
}

const INT16_COLS = [
	'num_bikes_available',
	'num_ebikes_available',
	'num_docks_available',
	'num_bikes_disabled',
	'num_docks_disabled',
	'is_installed',
	'is_renting',
	'is_returning',
] as const;

const INT64_COLS = ['ts', 'polled_at', 'last_reported'] as const;

function pad2(n: number): string {
	return n.toString().padStart(2, '0');
}

/** UTC date+hour of the hour that *just ended* (1h before `now`). */
export function targetHour(now: Date): { date: string; hour: string } {
	const t = new Date(now.getTime() - 60 * 60 * 1000);
	const date = `${t.getUTCFullYear()}-${pad2(t.getUTCMonth() + 1)}-${pad2(t.getUTCDate())}`;
	const hour = pad2(t.getUTCHours());
	return { date, hour };
}

/** R2 prefix for the per-minute JSONs of a given (date, hour). */
function statusPrefix(date: string, hour: string): string {
	return `gbfs/status/${date}/${hour}-`;
}

/** Output key for the h1 parquet shard. */
function h1Key(date: string, hour: string): string {
	return `gbfs/avail/h1/${date}/${hour}.parquet`;
}

/**
 * List all minute-JSON keys under `gbfs/status/<date>/<hour>-*.json`. R2's
 * prefix listing is what we need; the trailing `MM.json` filter is implicit
 * in the prefix when we use `gbfs/status/<date>/<hour>-`.
 */
async function listMinuteKeys(r2: R2Bucket, prefix: string): Promise<string[]> {
	const keys: string[] = [];
	let cursor: string | undefined;
	while (true) {
		const result = await r2.list({ prefix, cursor, limit: 1000 });
		for (const obj of result.objects) {
			if (obj.key.endsWith('.json')) keys.push(obj.key);
		}
		if (!result.truncated) break;
		cursor = result.cursor;
	}
	return keys.sort();
}

/** The 2.3 WAL's minute keys plus, for each minute it lacks, the 1.1
 *  side-poller's record (`gbfs/probe/v11/<date>/<HH-MM>.json`: same LU-minute
 *  keying and slim station fields, minus `vehicle_types_available`; the same
 *  system on another publish phase). Which minutes were filled is exactly the
 *  v11 keys returned. Sorted by minute. */
export function withV11Fills(walKeys: readonly string[], v11Keys: readonly string[], generations: readonly string[] = []): string[] {
	const base = (k: string) => k.slice(k.lastIndexOf('/') + 1);
	const have = new Set(walKeys.map(base));
	for (const key of generations) {
		const ts = Number(base(key).replace('.json', ''));
		have.add(new Date(ts * 1000).toISOString().slice(11, 16).replace(':', '-') + '.json');
	}
	const fills = v11Keys.filter((k) => !have.has(base(k)));
	return [...walKeys, ...fills].sort((a, b) => (base(a) < base(b) ? -1 : base(a) > base(b) ? 1 : 0));
}

/** Fetch + parse one WAL JSON. Returns `null` if the object is missing/corrupt. */
async function readSnapshot(r2: R2Bucket, key: string): Promise<Snapshot | null> {
	const obj = await r2.get(key);
	if (!obj) return null;
	try {
		return JSON.parse(await obj.text()) as Snapshot;
	} catch (err) {
		console.warn(`Bad JSON at ${key}: ${err}`);
		return null;
	}
}

interface ColumnData {
	name: string;
	data: (string | number | bigint | null)[];
	type: 'STRING' | 'INT32' | 'INT64';
}

/**
 * Pivot a flat list of (snapshot × station) rows into columnar arrays
 * matching the daily-compaction parquet schema.
 *
 * Sorted by (station_id, ts) so per-station queries can use parquet's
 * row-group min/max stats on `station_id` to read just one row group
 * per shard (~60 rows for that station × that hour) instead of decoding
 * the whole 144k-row file. See `specs/gbfs-r2-only.md` Phase 2.5.
 */
export function buildColumnData(snapshots: Snapshot[]): ColumnData[] {
	const flatRows: (StationRow & { ts: number; polled_at: number })[] = [];
	for (const snap of snapshots) {
		for (const s of snap.stations) {
			flatRows.push({ ...s, ts: snap.ts, polled_at: snap.polled_at });
		}
	}
	flatRows.sort((a, b) =>
		a.station_id < b.station_id ? -1 : a.station_id > b.station_id ? 1 : a.ts - b.ts,
	);

	const n = flatRows.length;
	const stationIds = new Array<string>(n);
	const ts = new Array<bigint>(n);
	const polledAt = new Array<bigint>(n);
	const lastReported = new Array<bigint>(n);
	const int32: Record<string, number[]> = {};
	for (const c of INT16_COLS) int32[c] = new Array<number>(n);

	for (let i = 0; i < n; i++) {
		const r = flatRows[i];
		stationIds[i] = r.station_id;
		ts[i] = BigInt(r.ts);
		polledAt[i] = BigInt(r.polled_at);
		lastReported[i] = BigInt(r.last_reported ?? 0);
		for (const c of INT16_COLS) int32[c][i] = (r[c] ?? 0) | 0;
	}

	return [
		{ name: 'station_id', data: stationIds, type: 'STRING' },
		{ name: 'ts', data: ts, type: 'INT64' },
		{ name: 'polled_at', data: polledAt, type: 'INT64' },
		...INT16_COLS.map((c) => ({ name: c, data: int32[c], type: 'INT32' as const })),
		{ name: 'last_reported', data: lastReported, type: 'INT64' },
	];
}

/** Build a parquet buffer from snapshots. Returns `null` if there are no rows. */
async function snapshotsToParquet(snapshots: Snapshot[]): Promise<ArrayBuffer | null> {
	if (snapshots.length === 0) return null;
	const columnData = buildColumnData(snapshots);
	if (columnData[0].data.length === 0) return null;

	const { parquetWriteBuffer } = await import('hyparquet-writer');
	// codec defaults to 'SNAPPY'; statistics on by default → free index on station_id.
	// rowGroupSize ~600 ≈ 10 stations × 1 hour: keeps station_id min/max stats
	// useful for pruning, while bounding rg-count to ~250/shard. hyparquet
	// parses the full footer eagerly, so rg-count dominates worker parse cost
	// — tighter (e.g. 60 → 1 station/rg) hits the 128 MB memory limit when
	// many shards are read concurrently.
	return parquetWriteBuffer({ columnData, rowGroupSize: 600 });
}

/** Compact one (date, hour) window. Returns the count of minutes compacted. */
export async function compactHour(r2: R2Bucket, date: string, hour: string): Promise<{ minutes: number; rows: number; bytes: number }> {
	const wal = await listMinuteKeys(r2, statusPrefix(date, hour));
	const generations = generationKeysForHour(await listMinuteKeys(r2, `gbfs/status-generation/${date}/`), date, hour);
	const minuteKeys = withV11Fills(wal, await listMinuteKeys(r2, `gbfs/probe/v11/${date}/${hour}-`), generations);
	const keys = [...generations, ...minuteKeys];
	if (minuteKeys.length > wal.length) console.log(`${date} ${hour}h: filled ${minuteKeys.length - wal.length} minute(s) from the 1.1 side-poller`);
	if (keys.length === 0) {
		console.log(`No minute JSONs for ${date} hour ${hour}; skipping`);
		return { minutes: 0, rows: 0, bytes: 0 };
	}

	const results = await Promise.all(keys.map((k) => readSnapshot(r2, k)));
	const snapshots = distinctSnapshots(results.filter((s): s is Snapshot => s !== null));
	const buf = await snapshotsToParquet(snapshots);
	if (!buf) {
		console.warn(`No rows for ${date} hour ${hour}; skipping write`);
		return { minutes: new Set(snapshots.map((s) => Math.floor(s.ts / 60))).size, rows: 0, bytes: 0 };
	}

	const outKey = h1Key(date, hour);
	await r2.put(outKey, buf, { httpMetadata: { contentType: 'application/octet-stream' } });
	const rows = snapshots.reduce((acc, s) => acc + s.stations.length, 0);
	const minutes = new Set(snapshots.map((s) => Math.floor(s.ts / 60))).size;
	console.log(`Compacted ${snapshots.length} generations in ${minutes} minutes (${rows} rows, ${buf.byteLength} bytes) → ${outKey}`);
	return { minutes, rows, bytes: buf.byteLength };
}

export default {
	async scheduled(event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
		const { date, hour } = targetHour(new Date(event.scheduledTime));
		ctx.waitUntil(
			compactHour(env.R2, date, hour).catch((err) =>
				console.error(`Compaction failed for ${date}/${hour}:`, err),
			),
		);
	},

	async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);
		if (url.pathname === '/compact') {
			// Manual backfill is a privileged operation (R2 reads/writes, CPU);
			// the cron path skips this since it doesn't go through `fetch`.
			// Fail closed if no secret is configured — never expose to anonymous.
			if (!env.COMPACTOR_SECRET) {
				return new Response('compactor secret not configured\n', { status: 503 });
			}
			if (request.headers.get('x-compactor-secret') !== env.COMPACTOR_SECRET) {
				return new Response('unauthorized\n', { status: 401 });
			}
			const date = url.searchParams.get('date');
			const hour = url.searchParams.get('hour');
			if (!date || !hour || !/^\d{4}-\d{2}-\d{2}$/.test(date) || !/^\d{2}$/.test(hour)) {
				return new Response('usage: /compact?date=YYYY-MM-DD&hour=HH\n', { status: 400 });
			}
			const result = await compactHour(env.R2, date, hour);
			return new Response(JSON.stringify({ date, hour, ...result }, null, 2) + '\n', {
				headers: { 'content-type': 'application/json' },
			});
		}
		return new Response('GBFS hourly compactor.\n  GET /compact?date=YYYY-MM-DD&hour=HH (secret-gated)\n');
	},
} satisfies ExportedHandler<Env>;
