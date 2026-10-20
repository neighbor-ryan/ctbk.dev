import { describe, expect, test } from 'vitest';
import { writeStatusSnapshot } from '../../lib/status-wal';
import type { MinuteRecord } from '../../lib/avail-monoid';
import worker from './index';

function memoryBucket() {
	const objects = new Map<string, { body: string; etag: string; customMetadata: Record<string, string> }>();
	const writes: string[] = [];
	let next = 0;
	const bucket = {
		head: async (key: string) => objects.get(key) ?? null,
		get: async (key: string) => {
			const obj = objects.get(key);
			return obj ? { ...obj, json: async () => JSON.parse(obj.body) } : null;
		},
		put: async (key: string, body: string, opts: R2PutOptions) => {
			writes.push(key);
			const prior = objects.get(key);
			const cond = opts.onlyIf as R2Conditional;
			if (cond.etagDoesNotMatch === '*' && prior) return null;
			if (cond.etagMatches && prior?.etag !== cond.etagMatches) return null;
			const obj = { body, etag: String(++next), customMetadata: opts.customMetadata ?? {} };
			objects.set(key, obj);
			return obj;
		},
	};
	return { bucket: bucket as unknown as R2Bucket, objects, writes };
}

const record = (iso: string, polled = 0): MinuteRecord => ({
	ts: Date.parse(iso) / 1000,
	polled_at: polled,
	stations: [],
});

describe('immutable status generations', () => {
	test('retains both LUs in one minute and advances only the minute alias', async () => {
		const { bucket, objects } = memoryBucket();
		const first = record('2026-10-20T07:43:00Z', 1);
		const second = record('2026-10-20T07:43:59Z', 2);
		await writeStatusSnapshot(bucket, first);
		await writeStatusSnapshot(bucket, second);
		expect([...objects.keys()]).toEqual([
			`gbfs/status-generation/2026-10-20/${first.ts}.json`,
			'gbfs/status/2026-10-20/07-43.json',
			`gbfs/status-generation/2026-10-20/${second.ts}.json`,
		]);
		expect(JSON.parse(objects.get(`gbfs/status-generation/2026-10-20/${first.ts}.json`)!.body)).toEqual(first);
		expect(JSON.parse(objects.get('gbfs/status/2026-10-20/07-43.json')!.body)).toEqual({
			...second, generation_key: `gbfs/status-generation/2026-10-20/${second.ts}.json`,
		});
	});

	test('cold-start duplicates preserve the first observation and do not regress aliases', async () => {
		const { bucket, objects } = memoryBucket();
		const first = record('2026-10-20T07:43:00Z', 1);
		const newer = record('2026-10-20T07:43:59Z', 2);
		await writeStatusSnapshot(bucket, first);
		await writeStatusSnapshot(bucket, newer);
		const before = [...objects];
		await writeStatusSnapshot(bucket, { ...first, polled_at: 3 });
		expect([...objects]).toEqual(before);
	});

	test('retains a changed payload for the same LU under a content variant', async () => {
		const { bucket, objects } = memoryBucket();
		const first = record('2026-10-20T07:43:00Z', 1);
		const changed = { ...first, stations: [{ station_id: 'A' }] } as MinuteRecord;
		await writeStatusSnapshot(bucket, first);
		await writeStatusSnapshot(bucket, changed);
		const generations = [...objects].filter(([key]) => key.startsWith('gbfs/status-generation/'));
		expect(generations.map(([, obj]) => JSON.parse(obj.body))).toEqual([first, changed]);
		expect(JSON.parse(objects.get(`gbfs/status-generation/2026-10-20/${first.ts}.json`)!.body)).toEqual(first);
	});

	test('a raw write failure prevents advancing the minute alias', async () => {
		const { bucket, objects } = memoryBucket();
		bucket.put = async () => { throw new Error('raw archive unavailable'); };
		await expect(writeStatusSnapshot(bucket, record('2026-10-20T07:43:00Z'))).rejects.toThrow('raw archive unavailable');
		expect([...objects]).toEqual([]);
	});

	test('poller retains boundary collisions, source regressions and suppresses exact repeats', async () => {
		const { bucket, objects, writes } = memoryBucket();
		const head = bucket.head.bind(bucket);
		bucket.head = async (key: string) => key.startsWith('gbfs/info/') ? { etag: 'info' } as R2Object : head(key);
		const timestamps = [1792482180, 1792482239, 1792482179, 1792482179];
		const fetcher = { fetch: async () => new Response(JSON.stringify({
			last_updated: timestamps.shift(), ttl: 60, data: { stations: [] },
		})) };
		for (let i = 0; i < 4; i++) {
			await worker.fetch(new Request('https://poller/poll'), { BUCKET: bucket, FETCH: fetcher } as never, {} as never);
		}
		expect([...objects.keys()].sort()).toEqual([
			'gbfs/status-generation/2026-10-20/1792482179.json',
			'gbfs/status-generation/2026-10-20/1792482180.json',
			'gbfs/status-generation/2026-10-20/1792482239.json',
			'gbfs/status/2026-10-20/07-42.json',
			'gbfs/status/2026-10-20/07-43.json',
		]);
		expect(JSON.parse(objects.get('gbfs/status/2026-10-20/07-43.json')!.body).ts).toBe(1792482239);
		expect(writes).toEqual([
			'gbfs/status-generation/2026-10-20/1792482180.json',
			'gbfs/status/2026-10-20/07-43.json',
			'gbfs/status-generation/2026-10-20/1792482239.json',
			'gbfs/status/2026-10-20/07-43.json',
			'gbfs/status-generation/2026-10-20/1792482179.json',
			'gbfs/status/2026-10-20/07-42.json',
		]);
	});
});
