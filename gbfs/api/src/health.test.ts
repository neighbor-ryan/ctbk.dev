import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
	classifyCoverage, classifyFeedGaps, getCompactionHealth, getFeedHealth, getHealthSnapshot,
	HEALTH_INVENTORY_KEY, HEALTH_SNAPSHOT_KEY, refreshHealthInventory,
	shouldRefreshHealthInventory, summarizeFeedDay, type HealthR2,
} from './health';

/** In-memory `HealthR2` over a fixed key set: prefix listing (no delimiter
 *  support needed beyond the daily-parquet scan), single page. */
function fakeR2(keys: string[]): HealthR2 {
	const uploaded = new Date('2026-09-25T23:59:30Z');
	return {
		async list({ prefix = '', delimiter }) {
			const matched = keys.filter((k) => k.startsWith(prefix));
			const objects = delimiter ? matched.filter((k) => !k.slice(prefix.length).includes(delimiter)) : matched;
			return { objects: objects.map((key) => ({ key, uploaded })), truncated: false, delimitedPrefixes: [] } as never;
		},
		async get() {
			return null;
		},
	};
}

describe('health LIST budget', () => {
	beforeEach(() => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date('2026-10-11T12:15:00Z'));
	});
	afterEach(() => vi.useRealTimers());

	it('refreshes live alert inputs without listing historical inventory, even with no cache', async () => {
		const prefixes: string[] = [];
		const r2 = fakeR2(['gbfs/status/2026-10-11/12-14.json']);
		const list = r2.list;
		r2.list = async (opts) => { prefixes.push(opts.prefix!); return list(opts); };
		const snapshot = await getHealthSnapshot(r2);
		expect(prefixes.sort()).toEqual([
			'gbfs/avail/h1/2026-10-10/', 'gbfs/avail/h1/2026-10-11/',
			'gbfs/heartbeat/2026-10-11/', 'gbfs/probe/v11/2026-10-10/', 'gbfs/probe/v11/2026-10-11/', 'gbfs/status/2026-10-10/', 'gbfs/status/2026-10-11/',
		]);
		expect(snapshot.feed.latestPoll?.time).toBe('12:14');
		expect(snapshot.inventoryGeneratedAt).toBe(null);
	});

	it('preserves pre-deploy inventory and its age through repeated minute refreshes', async () => {
		const r2 = fakeR2(['gbfs/status/2026-10-11/12-14.json']);
		const old = {
			generatedAt: 100, feed: { last7Days: [{ date: '2026-10-08', count: 123, expected: 1440 }] },
			compactions: { daily: { latestDate: '2026-10-09', count: 180 } },
			cascade: { cells: [], expectedCells: [] }, pyramids: [], builds: [],
		};
		const objects = new Map([[HEALTH_SNAPSHOT_KEY, JSON.stringify(old)]]);
		r2.get = async (key) => objects.has(key) ? { json: async <T>() => JSON.parse(objects.get(key)!) as T } : null;
		for (let i = 0; i < 2; i++) {
			const snapshot = await getHealthSnapshot(r2);
			expect([snapshot.inventoryGeneratedAt, snapshot.compactions.daily, snapshot.feed.last7Days[3]]).toEqual([
				100, old.compactions.daily, { date: '2026-10-08', count: 123, expected: 1440 },
			]);
			expect(snapshot.generatedAt).toBe(Date.now() / 1000);
			objects.set(HEALTH_SNAPSHOT_KEY, JSON.stringify(snapshot));
			vi.advanceTimersByTime(60_000);
		}
	});

	it('persists hourly inventory for another isolate; failed scans leave it intact', async () => {
		const objects = new Map<string, string>();
		const r2 = fakeR2(['gbfs/status/2026-10-10.parquet', 'gbfs/status/2026-10-08/01-00.json']);
		r2.get = async (key) => objects.has(key) ? { json: async <T>() => JSON.parse(objects.get(key)!) as T } : null;
		r2.put = async (key, value) => { objects.set(key, value); };
		const inventory = await refreshHealthInventory(r2);
		expect(JSON.parse(objects.get(HEALTH_INVENTORY_KEY)!)).toEqual(inventory);
		vi.advanceTimersByTime(60_000);
		const snapshot = await getHealthSnapshot({ ...r2 });
		expect([snapshot.inventoryGeneratedAt, snapshot.compactions.daily, snapshot.feed.last7Days[3]]).toEqual([
			inventory.generatedAt, { latestDate: '2026-10-10', count: 1 }, { date: '2026-10-08', count: 1, expected: 1440 },
		]);
		r2.list = async () => { throw new Error('inventory listing failed'); };
		await expect(refreshHealthInventory(r2)).rejects.toThrow('inventory listing failed');
		expect(JSON.parse(objects.get(HEALTH_INVENTORY_KEY)!)).toEqual(inventory);
	});

	it('refreshes inventory only for hourly scheduled slots, regardless of delivery delay', () => {
		expect(['2026-10-11T12:00:00Z', '2026-10-11T12:01:00Z', '2026-10-12T00:00:00Z']
			.map((t) => shouldRefreshHealthInventory(Date.parse(t)))).toEqual([true, false, true]);
	});
});

describe('health just after 00:00Z (today has no keys yet)', () => {
	beforeEach(() => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date('2026-09-26T00:00:30Z'));
	});
	afterEach(() => vi.useRealTimers());

	const r2 = fakeR2([
		'gbfs/status/2026-09-25/23-58.json',
		'gbfs/status/2026-09-25/23-59.json',
		'gbfs/avail/h1/2026-09-25/22.parquet',
		'gbfs/avail/h1/2026-09-25/23.parquet',
	]);

	it('feed: latest poll comes from yesterday', async () => {
		const f = await getFeedHealth(r2);
		expect(f.latestPoll).toEqual({
			key: 'gbfs/status/2026-09-25/23-59.json',
			date: '2026-09-25',
			time: '23:59',
			uploadedAt: '2026-09-25T23:59:30.000Z',
		});
		expect([f.todayCount, f.todayExpected]).toEqual([0, 1]);
		expect(f.last7Days.slice(-2)).toEqual([
			{ date: '2026-09-25', count: 2, expected: 1440 },
			{ date: '2026-09-26', count: 0, expected: 1 },
		]);
	});

	it('hourly: latest h1 comes from yesterday; todayCount stays 0', async () => {
		const c = await getCompactionHealth(r2);
		expect(c.hourly).toEqual({ latestKey: 'gbfs/avail/h1/2026-09-25/23.parquet', todayCount: 0 });
	});
});

describe('classifyFeedGaps', () => {
	const labels = (...ms: number[]) => new Set(ms.map((i) => `${String(Math.floor(i / 60)).padStart(2, '0')}-${String(i % 60).padStart(2, '0')}`));
	const all = (n: number, ...except: number[]) => labels(...Array.from({ length: n }, (_, i) => i).filter((i) => !except.includes(i)));

	it('holes with both ticks present are upstream skips; next to a skipped tick, unexplained', () => {
		// WAL holes at 00:03 (both ticks ran), 00:05 (its own tick skipped), 00:07 (next tick skipped).
		expect(classifyFeedGaps(all(10, 3, 5, 7), all(10, 5, 8), 9)).toEqual({
			settled: 9,
			missing: 3,
			unexplained: ['00:05', '00:07'],
			cronSkips: 2,
		});
	});

	it('only settled minutes count', () => {
		expect(classifyFeedGaps(all(5), all(10), 8)).toEqual({ settled: 8, missing: 3, unexplained: [], cronSkips: 0 });
	});

	it('a hole in the last settled minute waits for its next tick to settle', () => {
		// 2026-10-10 14:46: the hole was the newest settled minute and the 14:47
		// tick (outside `settled`) hadn't written its heartbeat yet → flagged,
		// with "0 skipped tick(s)"; once 14:47's landed, an upstream skip.
		expect(classifyFeedGaps(all(9, 7), all(8), 8)).toEqual({ settled: 8, missing: 1, unexplained: [], cronSkips: 0 });
		expect(classifyFeedGaps(all(10, 7), all(10, 8), 9)).toEqual({ settled: 9, missing: 1, unexplained: ['00:07'], cronSkips: 1 });
	});
});

describe('combined minute coverage', () => {
	it('credits the backup, retains real gaps, and limits skipped-tick loss to uncovered slots', () => {
		const labels = (minutes: number[]) => new Set(minutes.map((m) => `2026-10-20 00:0${m}`));
		expect(classifyCoverage(labels([0, 2]), labels([1, 2, 3]), '2026-10-20', 5,
			['2026-10-20 00:01', '2026-10-20 00:03', '2026-10-20 00:04'], ['00:01', '00:04'])).toEqual({
			complete: true,
			today: { date: '2026-10-20', settled: 5, primary: 2, backup: 3, combined: 4, recovered: 2, hours: [[5, 2, 3, 4], ...Array.from({ length: 23 }, () => [0, 0, 0, 0])] },
			missing: ['2026-10-20 00:04'], unexplained: ['2026-10-20 00:04'],
			recent: { primary: ['2026-10-20 00:01', '2026-10-20 00:03', '2026-10-20 00:04'], backup: ['2026-10-20 00:04'], combined: ['2026-10-20 00:04'] },
		});
	});

	it('compares full dates across midnight and never fills from a different day', () => {
		const result = classifyCoverage(new Set(['2026-10-19 23:58']), new Set(['2026-10-19 23:59']), '2026-10-20', 0,
			['2026-10-19 23:58', '2026-10-19 23:59', '2026-10-20 00:00']);
		expect(result.recent).toEqual({ primary: ['2026-10-19 23:59', '2026-10-20 00:00'], backup: ['2026-10-19 23:58', '2026-10-20 00:00'], combined: ['2026-10-20 00:00'] });
		expect([result.today.settled, result.today.combined]).toEqual([0, 0]);
	});

	it('paginates both feeds and reports unknown if a listing exceeds its bound', async () => {
		vi.useFakeTimers(); vi.setSystemTime(new Date('2026-10-20T00:05:00Z'));
		try {
			const r2 = fakeR2([]), calls: string[] = [];
			r2.list = async ({ prefix = '', cursor }) => {
				calls.push(`${prefix}:${cursor ?? ''}`);
				if (prefix === 'gbfs/probe/v11/2026-10-20/') return {
					objects: [{ key: `${prefix}00-01.json`, uploaded: new Date() }], truncated: true, cursor: 'next',
				};
				return { objects: [], truncated: false };
			};
			const feed = await getFeedHealth(r2, []);
			expect(calls.filter((p) => p.startsWith('gbfs/probe/v11/2026-10-20/'))).toEqual([
				'gbfs/probe/v11/2026-10-20/:', 'gbfs/probe/v11/2026-10-20/:next', 'gbfs/probe/v11/2026-10-20/:next',
			]);
			expect(feed.coverage!.complete).toBe(false);
			expect(feed.backup!.ts).toBe(null);
		} finally { vi.useRealTimers(); }
	});

	it('treats a truncated listing without a continuation cursor as incomplete', async () => {
		const r2 = fakeR2([]);
		r2.list = async () => ({ objects: [], truncated: true });
		expect((await getFeedHealth(r2, [])).coverage!.complete).toBe(false);
	});
});

describe('passive feed history', () => {
	it('summarizes only the requested UTC day, separating unknown POPs from known ones', () => {
		const t = Date.parse('2026-10-11T12:00:00Z') / 1000;
		expect(summarizeFeedDay('2026-10-11', [
			[t - 86400, 99, 'SIN1'], [t, 15, 'BCN50-P1'], [t + 60, 20, 'IAD12-P5'], [t + 120, 65],
		])).toEqual({
			source: 'health-refresh', date: '2026-10-11', samples: 3, p50S: 20, p90S: 65, overGenerationCount: 1,
			popSamples: 2, nonUsSamples: 1, pops: { 'BCN50-P1': 1, 'IAD12-P5': 1 },
		});
	});

	it('archives sampled days idempotently and preserves yesterday across midnight', async () => {
		vi.useFakeTimers();
		try {
			const objects = new Map<string, string>();
			const writes: string[] = [];
			const r2: HealthR2 = {
				...fakeR2([]),
				get: async (key) => objects.has(key) ? { json: async <T>() => JSON.parse(objects.get(key)!) as T } : null,
				put: async (key, value) => { objects.set(key, value); writes.push(key); },
			};
			const observe = async (iso: string, pop: string) => {
				vi.setSystemTime(new Date(iso));
				const t = Date.parse(iso) / 1000;
				const key = `gbfs/status/${iso.slice(0, 10)}/${iso.slice(11, 16).replace(':', '-')}.json`;
				objects.set(key, JSON.stringify({ ts: t - 15, polled_at: t, pop }));
				r2.list = async ({ prefix = '' }) => ({
					objects: [...objects.keys()].filter((k) => k.startsWith(prefix)).map((key) => ({ key, uploaded: new Date(iso) })),
					truncated: false,
				});
				return getFeedHealth(r2);
			};
			const first = await observe('2026-10-11T23:59:00Z', 'BCN50-P1');
			expect(first.history).toEqual([{
				source: 'health-refresh', date: '2026-10-11', samples: 1, p50S: 15, p90S: 15, overGenerationCount: 0,
				popSamples: 1, nonUsSamples: 1, pops: { 'BCN50-P1': 1 },
			}]);
			await getFeedHealth(r2);
			expect(writes.filter((key) => !key.startsWith('health/feed-coverage/'))).toEqual(['health/feed-drift.json', 'health/feed-history/2026-10-11.json']);
			const yesterday = objects.get('health/feed-history/2026-10-11.json');
			const next = await observe('2026-10-12T00:01:00Z', 'IAD12-P5');
			expect(objects.get('health/feed-history/2026-10-11.json')).toBe(yesterday);
			expect(next.history).toEqual([...first.history!, {
				source: 'health-refresh', date: '2026-10-12', samples: 1, p50S: 15, p90S: 15, overGenerationCount: 0,
				popSamples: 1, nonUsSamples: 0, pops: { 'IAD12-P5': 1 },
			}]);
			expect(next.gaps!.recentMissing).toEqual([
				'2026-10-11 23:50', '2026-10-11 23:51', '2026-10-11 23:52', '2026-10-11 23:53',
				'2026-10-11 23:54', '2026-10-11 23:55', '2026-10-11 23:56', '2026-10-11 23:57', '2026-10-11 23:58',
			]);
		} finally { vi.useRealTimers(); }
	});
});
