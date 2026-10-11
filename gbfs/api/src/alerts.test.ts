import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import type { HealthSnapshot } from './health';
import { d1SizeGB, DEFAULT_RULES, diffRules, feedLagP90Seconds, feedStaleMinutes, hourlyCompactionStaleMinutes, pyramidTipAgeHours, snapshotAgeMinutes, stationsStaleHours, type AlertState, type FiringEntry, type Rule, isUsPop, runAlerts } from './alerts';

const FIXED_NOW = new Date('2026-05-24T12:00:00Z');

beforeEach(() => {
	vi.useFakeTimers();
	vi.setSystemTime(FIXED_NOW);
});

afterEach(() => {
	vi.useRealTimers();
});

function snap(overrides: Partial<HealthSnapshot> = {}): HealthSnapshot {
	const base: HealthSnapshot = {
		generatedAt: FIXED_NOW.getTime() / 1000,
		feed: {
			latestPoll: {
				key: 'gbfs/status/2026-05-24/11-59.json',
				date: '2026-05-24',
				time: '11:59',
				uploadedAt: '2026-05-24T11:59:00.000Z',
			},
			drift: null,
			todayCount: 720,
			todayExpected: 721,
			last7Days: [],
		},
		compactions: {
			daily: { latestDate: '2026-05-23', count: 1 },
			hourly: { latestKey: 'gbfs/avail/h1/2026-05-24/11.parquet', todayCount: 12 },
		},
		cascade: { cells: [], expectedCells: [] },
		pyramids: [],
		tripdata: null,
	};
	return { ...base, ...overrides };
}

describe('feedStaleMinutes', () => {
	it('returns 0–1min when poll just landed', () => {
		expect(feedStaleMinutes(snap())).toBeCloseTo(1, 0);
	});
	it('returns Infinity when no poll', () => {
		const s = snap({ feed: { ...snap().feed, latestPoll: null } });
		expect(feedStaleMinutes(s)).toBe(Infinity);
	});
	it('scales linearly with poll age', () => {
		const s = snap({
			feed: { ...snap().feed, latestPoll: { ...snap().feed.latestPoll!, uploadedAt: '2026-05-24T11:50:00.000Z' } },
		});
		expect(feedStaleMinutes(s)).toBeCloseTo(10, 1);
	});
});

describe('feedLagP90Seconds', () => {
	const now = FIXED_NOW.getTime() / 1000;
	const withLags = (lags: number[], ageS = 60) =>
		snap({ feed: { ...snap().feed, drift: { latestS: lags[lags.length - 1], ts: now, polledAt: now, series: lags.map((l, i) => [now - ageS * (lags.length - i), l]) } } });
	it('p90 of the trailing hour', () => {
		expect(feedLagP90Seconds(withLags([4, 5, 6, 5, 4, 6, 5, 50, 55, 58]))).toBe(58);
		expect(feedLagP90Seconds(withLags([...Array(18).fill(5), 50, 55]))).toBe(50);
	});
	it('ignores points older than an hour; 0 below 10 points', () => {
		expect(feedLagP90Seconds(withLags(Array(20).fill(50), 400))).toBe(0);
		expect(feedLagP90Seconds(withLags([50, 50, 50]))).toBe(0);
	});
});

describe('hourlyCompactionStaleMinutes', () => {
	it('returns ~0 when hourly is current', () => {
		// Hour 11 compaction covers through end of hour 11 (12:00); at noon, age = 0.
		expect(hourlyCompactionStaleMinutes(snap())).toBeCloseTo(0, 0);
	});
	it('returns Infinity for missing key', () => {
		const s = snap({ compactions: { ...snap().compactions, hourly: { latestKey: null, todayCount: 0 } } });
		expect(hourlyCompactionStaleMinutes(s)).toBe(Infinity);
	});
	it('returns minutes since hour-end for stale hourly', () => {
		const s = snap({
			compactions: { ...snap().compactions, hourly: { latestKey: 'gbfs/avail/h1/2026-05-24/09.parquet', todayCount: 10 } },
		});
		// Hour 09 covers through 10:00; from 12:00 that's 2 hours = 120min.
		expect(hourlyCompactionStaleMinutes(s)).toBeCloseTo(120, 0);
	});
});

describe('diffRules', () => {
	const fireRule: Rule = {
		id: 'always-fire',
		description: 'always',
		check: () => true,
		firingText: () => 'firing!',
	};
	const restRule: Rule = {
		id: 'never-fire',
		description: 'never',
		check: () => false,
		firingText: () => 'should not appear',
	};

	function entry(firingSince: string): FiringEntry {
		return { firingSince, threadTs: `ts-${firingSince}`, firingText: 'prior firing text' };
	}

	it('emits firing transition for newly-firing rule', () => {
		const transitions = diffRules([fireRule], { firing: {} }, snap());
		expect(transitions).toEqual([{ rule: fireRule, kind: 'firing', firingText: 'firing!' }]);
	});

	it('emits resolved transition when rule clears', () => {
		const resolvedRule: Rule = { id: 'always-fire', description: 'desc', check: () => false, firingText: () => '' };
		const prior = entry('2026-05-24T11:00:00Z');
		const transitions = diffRules([resolvedRule], { firing: { 'always-fire': prior } }, snap());
		expect(transitions).toEqual([
			{ rule: resolvedRule, kind: 'resolved', priorEntry: prior },
		]);
	});

	it('resolves a firing rule that was removed/renamed (else it sticks in state forever)', () => {
		const prior = entry('2026-05-24T11:00:00Z');
		const [t, ...rest] = diffRules([restRule], { firing: { 'always-fire': prior } }, snap());
		expect(rest).toEqual([]);
		expect({ kind: t.kind, id: t.rule.id, description: t.rule.description, check: t.rule.check(snap()), priorEntry: t.priorEntry }).toEqual({
			kind: 'resolved',
			id: 'always-fire',
			description: '`always-fire` rule retired',
			check: false,
			priorEntry: prior,
		});
	});

	it('no transition when rule keeps firing (deduped)', () => {
		const transitions = diffRules([fireRule], { firing: { 'always-fire': entry('2026-05-24T11:00:00Z') } }, snap());
		expect(transitions).toEqual([]);
	});

	it('no transition when rule keeps not-firing', () => {
		const transitions = diffRules([restRule], { firing: {} }, snap());
		expect(transitions).toEqual([]);
	});

	it('handles mixed firing/resolved/steady-state in one pass', () => {
		const ruleA: Rule = { id: 'a', description: 'A', check: () => true, firingText: () => 'A fires' };
		const ruleB: Rule = { id: 'b', description: 'B', check: () => false, firingText: () => 'B fires' };
		const ruleC: Rule = { id: 'c', description: 'C', check: () => true, firingText: () => 'C fires' };
		const prev: AlertState = {
			firing: {
				b: entry('2026-05-24T11:00:00Z'),
				c: entry('2026-05-24T11:00:00Z'),
			},
		};
		const transitions = diffRules([ruleA, ruleB, ruleC], prev, snap());
		expect(transitions.map((t) => [t.rule.id, t.kind])).toEqual([
			['a', 'firing'],   // new
			['b', 'resolved'], // was firing, now not
			// c stays firing — no transition
		]);
	});
});

/** `pyramidTips` with each watched pyramid's newest shard ending `h` hours ago. */
const tipsAt = (ages: Record<string, number>) => Object.fromEntries(Object.entries(ages).map(([name, h]) =>
	[name, FIXED_NOW.getTime() - h * 3_600_000]));
const HEALTHY_TIPS = { 'avail-v5': 0.2, 'avail-v6': 0.2, 'smg-v1': 12, 'rides-start': 24 * 24, 'rides-end': 24 * 24 };

const firingIds = (s: HealthSnapshot) => DEFAULT_RULES.filter((r) => r.check(s)).map((r) => r.id);

describe('pyramidTipAgeHours', () => {
	it('ages the newest registered period_end', () => {
		expect(pyramidTipAgeHours(snap({ pyramidTips: tipsAt({ 'avail-v6': 3 }) }), 'avail-v6')).toBe(3);
	});
	it('Infinity for a pyramid with no shards; 0 for a pre-feature snapshot', () => {
		expect(pyramidTipAgeHours(snap({ pyramidTips: { 'avail-v6': null } }), 'avail-v6')).toBe(Infinity);
		expect(pyramidTipAgeHours(snap(), 'avail-v6')).toBe(0);
	});
});

describe('stationsStaleHours / snapshotAgeMinutes', () => {
	it('hours since the last stations upsert; Infinity when unknown', () => {
		expect(stationsStaleHours(snap({ stations: { lastUpdatedAt: FIXED_NOW.getTime() / 1000 - 7200 } }))).toBe(2);
		expect(stationsStaleHours(snap({ stations: null }))).toBe(Infinity);
		expect(stationsStaleHours(snap())).toBe(0);  // pre-feature cached snapshot
	});
	it('minutes since the snapshot was generated', () => {
		expect(snapshotAgeMinutes(snap({ generatedAt: FIXED_NOW.getTime() / 1000 - 600 }))).toBe(10);
	});
});

describe('DEFAULT_RULES on a full snapshot', () => {
	const fresh = (overrides: Partial<HealthSnapshot> = {}) => snap({
		pyramidTips: tipsAt(HEALTHY_TIPS),
		stations: { lastUpdatedAt: FIXED_NOW.getTime() / 1000 - 12 * 3600 },
		...overrides,
	});

	it('healthy → nothing fires', () => {
		expect(firingIds(fresh())).toEqual([]);
	});

	it('a dead cascade (avail tips 1.5h behind) fires both avail tip rules only', () => {
		expect(firingIds(fresh({ pyramidTips: tipsAt({ ...HEALTHY_TIPS, 'avail-v5': 1.5, 'avail-v6': 1.5 }) }))).toEqual([
			'pyramid-tip-stale:avail-v5',
			'pyramid-tip-stale:avail-v6',
		]);
	});

	it('a missed daily smg fill fires at >36h, a late monthly ingest at >50d', () => {
		expect(firingIds(fresh({ pyramidTips: tipsAt({ ...HEALTHY_TIPS, 'smg-v1': 37, 'rides-end': 51 * 24 }) }))).toEqual([
			'pyramid-tip-stale:smg-v1',
			'pyramid-tip-stale:rides-end',
		]);
	});

	it('stale stations (the 2026-06 loader failure) and a dead snapshot cron each fire', () => {
		expect(firingIds(fresh({
			stations: { lastUpdatedAt: FIXED_NOW.getTime() / 1000 - 90 * 24 * 3600 },
			generatedAt: FIXED_NOW.getTime() / 1000 - 20 * 60,
		}))).toEqual(['stations-stale', 'health-snapshot-stale']);
	});

	it('upstream-skipped generations alone fire nothing; a hole next to a skipped tick does', () => {
		const gaps = { settled: 718, missing: 30, unexplained: [], cronSkips: 0 };
		expect(firingIds(fresh({ feed: { ...snap().feed, gaps } }))).toEqual([]);
		const lost = fresh({ feed: { ...snap().feed, gaps: { ...gaps, missing: 31, unexplained: ['10:20'], cronSkips: 2 } } });
		expect(firingIds(lost)).toEqual(['feed-missed-lus']);
		expect(DEFAULT_RULES.find((r) => r.id === 'feed-missed-lus')!.firingText(lost)).toBe(
			':rotating_light: *GBFS minutes possibly lost* — 1 WAL hole(s) today next to a skipped poller tick (10:20); 2 skipped tick(s), 30 other unobserved generation(s) (upstream omission or cache)',
		);
	});

	const withRecentLags = (lags: number[], pop = 'IAD12-P5') => {
		const now = FIXED_NOW.getTime() / 1000;
		return fresh({ feed: { ...snap().feed, drift: {
			latestS: lags[lags.length - 1], ts: now, polledAt: now,
			series: lags.map((lag, i) => [now - 60 * (lags.length - 1 - i), lag]), pop,
		} } });
	};

	it('fresh non-US routing and borderline lag do not page', () => {
		expect(firingIds(withRecentLags(Array(10).fill(37), 'BCN50-P1'))).toEqual([]);
		expect(firingIds(withRecentLags([15], 'BCN50-P1'))).toEqual([]);
		expect([isUsPop('IAD12-P5'), isUsPop('jfk50-c1'), isUsPop('NRT12-P9')]).toEqual([true, true, false]);
	});

	it('three generation-late observations warn on any POP; isolated slow samples do not', () => {
		expect(firingIds(withRecentLags([20, 65, 20, 65, 20]))).toEqual([]);
		const risk = withRecentLags([60, 61, 62], 'TPE53-P1');
		expect(firingIds(risk)).toEqual(['feed-lag']);
		expect(DEFAULT_RULES.find((r) => r.id === 'feed-lag')!.firingText(risk)).toBe(
			':warning: *GBFS stale-cache loss risk* — 3 distinct observations first seen ≥ 60s late in the last 10 minutes; a feed generation lasts about a minute, so updates may be superseded before we see them (latest serving POP: TPE53-P1)',
		);
		expect(firingIds(fresh({ d1: { sizeBytes: 8.5e9 } }))).toEqual(['d1-size']);
		expect(d1SizeGB(fresh({ d1: { sizeBytes: 4.01e9 } }))).toBe(4.01);
	});

	it('lag recovery uses 45s hysteresis and does not mistake missing samples for recovery', () => {
		const rule = DEFAULT_RULES.find((r) => r.id === 'feed-lag')!;
		const prior: FiringEntry = { firingSince: FIXED_NOW.toISOString(), threadTs: 'ts1', firingText: 'risk' };
		const state = { firing: { 'feed-lag': prior } };
		expect(diffRules([rule], state, withRecentLags([45, 45, 45]))).toEqual([]);
		expect(diffRules([rule], state, withRecentLags([]))).toEqual([]);
		expect(diffRules([rule], state, withRecentLags([44, 44, 44]))).toEqual([
			{ rule, kind: 'resolved', priorEntry: prior },
		]);
	});

	it('old, future, and duplicate observations cannot manufacture a lag incident', () => {
		const s = withRecentLags([65]);
		const now = FIXED_NOW.getTime() / 1000;
		s.feed.drift!.series = [[now - 601, 65], [now + 1, 65], [now, 65], [now, 65], [now, 65]];
		expect(firingIds(s)).toEqual([]);
	});

	it('coverage bursts page even with healthy ticks, and recover at at most one recent hole', () => {
		const rule = DEFAULT_RULES.find((r) => r.id === 'feed-generation-gaps')!;
		const gaps = { settled: 718, missing: 3, unexplained: [], cronSkips: 0, recentMissing: ['11:50', '11:51', '11:52'] };
		const s = fresh({ feed: { ...snap().feed, gaps } });
		expect(firingIds(s)).toEqual(['feed-generation-gaps']);
		expect(rule.firingText(s)).toBe(':rotating_light: *GBFS generation coverage loss* — 3 missing WAL generations in the last 10 settled minutes (11:50, 11:51, 11:52); upstream omission, stale cache, or missed polls may be responsible. Running cron ticks alone do not prove coverage');
		gaps.recentMissing = ['11:51', '11:52'];
		expect([rule.check(s), rule.check(s, true)]).toEqual([false, true]);
		gaps.recentMissing = ['11:52'];
		expect(rule.check(s, true)).toBe(false);
	});

});

/** In-memory R2 with etags and `onlyIf.etagMatches` (a failed precondition
 *  returns `null`, as R2's `put` does). */
class FakeR2 {
	objects = new Map<string, { body: string; etag: string }>();
	private n = 0;
	async list() { return { objects: [], truncated: false, delimitedPrefixes: [] } as never; }
	async get(key: string) {
		const o = this.objects.get(key);
		if (!o) return null;
		return { etag: o.etag, json: async <T>() => JSON.parse(o.body) as T };
	}
	async put(key: string, body: string, opts?: { onlyIf?: { etagMatches?: string } }) {
		const want = opts?.onlyIf?.etagMatches;
		if (want !== undefined && this.objects.get(key)?.etag !== want) return null;
		const o = { body, etag: `e${++this.n}` };
		this.objects.set(key, o);
		return { etag: o.etag };
	}
	state(): AlertState { return JSON.parse(this.objects.get('gbfs/alerts/state.json')!.body); }
}

describe('runAlerts', () => {
	const rule: Rule = { id: 'always', description: 'never fires', check: () => true, firingText: () => 'boom' };

	it('overlapping cron runs post a firing transition once', async () => {
		vi.useRealTimers();
		const r2 = new FakeR2();
		await r2.put('health/snapshot.json', JSON.stringify(snap()));
		const posts: string[][] = [];
		const slack = {
			sync: async (thread: { messages: string[] }) => {
				posts.push(thread.messages);
				// Slack's round-trip: the other run reads state meanwhile.
				await new Promise((r) => setTimeout(r, 20));
				return { threadId: `ts${posts.length}` };
			},
		};
		const [a, b] = await Promise.all([
			runAlerts(r2, 'tok', [rule], undefined, slack as never),
			runAlerts(r2, 'tok', [rule], undefined, slack as never),
		]);
		expect(posts).toEqual([['boom']]);
		expect([a.length, b.length].sort()).toEqual([0, 1]);
		expect(Object.keys(r2.state().firing)).toEqual(['always']);
		expect(r2.state().firing.always.threadTs).toBe('ts1');
	});

	it('a live claim defers to its holder; an expired one (a run that died mid-post) is taken over', async () => {
		vi.useRealTimers();
		const r2 = new FakeR2();
		await r2.put('health/snapshot.json', JSON.stringify(snap()));
		const posts: string[][] = [];
		const slack = { sync: async (thread: { messages: string[] }) => { posts.push(thread.messages); return { threadId: 'ts1' }; } };
		const claimed = (ageMs: number) => JSON.stringify({ firing: {}, claim: { at: new Date(Date.now() - ageMs).toISOString() } });
		await r2.put('gbfs/alerts/state.json', claimed(60_000));
		expect(await runAlerts(r2, 'tok', [rule], undefined, slack as never)).toEqual([]);
		expect(posts).toEqual([]);
		await r2.put('gbfs/alerts/state.json', claimed(6 * 60_000));
		expect((await runAlerts(r2, 'tok', [rule], undefined, slack as never)).map((t) => t.rule.id)).toEqual(['always']);
		expect(posts).toEqual([['boom']]);
		expect(r2.state()).toEqual({ firing: { always: { firingSince: expect.any(String), threadTs: 'ts1', firingText: 'boom' } } });
	});
});
