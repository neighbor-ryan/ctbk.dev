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
async function listMinuteKeys(r2: R2Bucket, prefix: string, references?: Map<string, string>): Promise<string[]> {
	const keys: string[] = [];
	let cursor: string | undefined;
	while (true) {
		const result = await r2.list({ prefix, cursor, limit: 1000, ...(references ? { include: ['customMetadata' as const] } : {}) });
		for (const obj of result.objects) {
			if (obj.key.endsWith('.json')) keys.push(obj.key);
			if (references && obj.customMetadata?.generation_key) references.set(obj.key, obj.customMetadata.generation_key);
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
class SnapshotColumns {
	private columns: ColumnData[] = [
		{ name: 'station_id', data: [], type: 'STRING' },
		{ name: 'ts', data: [], type: 'INT64' },
		{ name: 'polled_at', data: [], type: 'INT64' },
		...INT16_COLS.map((name) => ({ name, data: [], type: 'INT32' as const })),
		{ name: 'last_reported', data: [], type: 'INT64' },
	];

	private rows = 0;
	private initialized = false;
	private stationIds = new Map<string, string>();
	private reportedTimes = new Map<number, bigint>();

	private expectedSnapshots: number;
	constructor(expectedSnapshots = 0) { this.expectedSnapshots = expectedSnapshots; }

	add(snapshot: Snapshot): void {
		if (!this.initialized && snapshot.stations.length) {
			const expectedRows = this.expectedSnapshots * snapshot.stations.length;
			for (const col of this.columns) col.data = new Array(expectedRows);
			this.initialized = true;
		}
		const ts = BigInt(snapshot.ts), polled = BigInt(snapshot.polled_at);
		for (const station of snapshot.stations) {
			const row = this.rows++;
			let id = this.stationIds.get(station.station_id);
			if (id === undefined) { id = station.station_id; this.stationIds.set(id, id); }
			this.columns[0].data[row] = id;
			this.columns[1].data[row] = ts;
			this.columns[2].data[row] = polled;
			for (let i = 0; i < INT16_COLS.length; i++) this.columns[i + 3].data[row] = (station[INT16_COLS[i]] ?? 0) | 0;
			const time = station.last_reported ?? 0;
			let reported = this.reportedTimes.get(time);
			if (reported === undefined) { reported = BigInt(time); this.reportedTimes.set(time, reported); }
			this.columns[this.columns.length - 1].data[row] = reported;
		}
	}

	finish(): ColumnData[] {
		for (const col of this.columns) col.data.length = this.rows;
		const ids = this.columns[0].data as string[];
		const ts = this.columns[1].data as bigint[];
		const order = Array.from({ length: ids.length }, (_, i) => i);
		order.sort((a, b) => ids[a] < ids[b] ? -1 : ids[a] > ids[b] ? 1 : Number(ts[a] - ts[b]));
		// Apply the sorted-index permutation in place. Copying every column here
		// briefly doubles the whole hour's scalar storage before encoding it.
		const visited = new Uint8Array(order.length);
		for (const column of this.columns) {
			visited.fill(0);
			for (let start = 0; start < order.length; start++) {
				if (visited[start]) continue;
				let current = start;
				const saved = column.data[start];
				while (order[current] !== start) {
					column.data[current] = column.data[order[current]];
					visited[current] = 1;
					current = order[current];
				}
				column.data[current] = saved;
				visited[current] = 1;
			}
		}
		const sorted = this.columns;
		this.columns = [];
		return sorted;
	}
}

export function buildColumnData(snapshots: Snapshot[]): ColumnData[] {
	const builder = new SnapshotColumns(snapshots.length);
	for (const snapshot of snapshots) builder.add(snapshot);
	return builder.finish();
}

async function columnsToParquet(columnData: ColumnData[]): Promise<ArrayBuffer | null> {
	if (columnData[0].data.length === 0) return null;
	const { parquetWriteBuffer } = await import('hyparquet-writer');
	return parquetWriteBuffer({ columnData, rowGroupSize: 600 });
}

/** Compact one (date, hour) window. Returns the count of minutes compacted. */
export async function compactHour(r2: R2Bucket, date: string, hour: string): Promise<{ minutes: number; rows: number; bytes: number }> {
	const references = new Map<string, string>();
	const wal = await listMinuteKeys(r2, statusPrefix(date, hour), references);
	const generations = generationKeysForHour(await listMinuteKeys(r2, `gbfs/status-generation/${date}/`), date, hour);
	const minuteKeys = withV11Fills(wal, await listMinuteKeys(r2, `gbfs/probe/v11/${date}/${hour}-`), generations);
	const generationSet = new Set(generations);
	const keys = [...generations, ...minuteKeys.filter((key) => !generationSet.has(references.get(key) ?? ''))];
	if (minuteKeys.length > wal.length) console.log(`${date} ${hour}h: filled ${minuteKeys.length - wal.length} minute(s) from the 1.1 side-poller`);
	if (keys.length === 0) {
		console.log(`No minute JSONs for ${date} hour ${hour}; skipping`);
		return { minutes: 0, rows: 0, bytes: 0 };
	}

	// Bound decoded bodies rather than retaining alias + generation payloads for
	// an entire hour. Only scalar columns survive each batch; the large station
	// objects and JSON strings can be reclaimed before the next four reads.
	const builder = new SnapshotColumns(keys.length);
	const seen = new Set<number>();
	const minuteSet = new Set<number>();
	let rows = 0;
	for (let offset = 0; offset < keys.length; offset += 4) {
		const batch = await Promise.all(keys.slice(offset, offset + 4).map((key) => readSnapshot(r2, key)));
		for (const snapshot of batch) {
			if (!snapshot || seen.has(snapshot.ts)) continue;
			seen.add(snapshot.ts);
			minuteSet.add(Math.floor(snapshot.ts / 60));
			rows += snapshot.stations.length;
			builder.add(snapshot);
		}
	}
	const buf = await columnsToParquet(builder.finish());
	if (!buf) {
		console.warn(`No rows for ${date} hour ${hour}; skipping write`);
		return { minutes: minuteSet.size, rows: 0, bytes: 0 };
	}

	const outKey = h1Key(date, hour);
	await r2.put(outKey, buf, { httpMetadata: { contentType: 'application/octet-stream' } });
	const minutes = minuteSet.size;
	console.log(`Compacted ${seen.size} generations in ${minutes} minutes (${rows} rows, ${buf.byteLength} bytes) → ${outKey}`);
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
