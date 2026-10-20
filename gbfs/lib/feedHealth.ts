/** Pure feed health model shared by Worker diagnostics and browser rendering. */
export type FeedObservation = [polledAt: number, lagS: number, pop?: string | null];

/** Sampled by health refreshes, not a census of every WAL generation. */
export interface FeedDaySummary {
	source: 'health-refresh';
	date: string;
	samples: number;
	p50S: number;
	p90S: number;
	overGenerationCount: number;
	popSamples: number;
	nonUsSamples: number;
	pops: Record<string, number>;
}
export interface FeedDrift {
	/** `polled_at - ts` of the latest WAL record, seconds. */
	latestS: number;
	/** Feed `last_updated` (epoch s) of the latest WAL record. */
	ts: number;
	/** Poll wall-clock (epoch s) of the latest WAL record. */
	polledAt: number;
	/** `[polled_at epoch s, drift s, optional POP]` health samples, ≤24h old, oldest first. */
	series: FeedObservation[];
	/** CloudFront POP that served the latest record (`x-amz-cf-pop`), when recorded. */
	pop?: string | null;
}

export interface FeedHealth {
	latestPoll: { key: string; date: string; time: string; uploadedAt: string } | null;
	drift: FeedDrift | null;
	todayCount: number;
	todayExpected: number;
	last7Days: Array<{ date: string; count: number; expected: number }>;
	gaps?: FeedGaps;
	/** Last seven UTC days of durable sampled lag/POP summaries. */
	history?: FeedDaySummary[];
	/** Minute slots observed by either poller; not a census of upstream generations. */
	coverage?: FeedCoverage;
	backup?: { latestPoll: FeedHealth['latestPoll']; ts: number | null; polledAt: number | null };
	coverageHistory?: FeedCoverageDay[];
}

export interface FeedCoverageDay {
	date: string;
	settled: number;
	primary: number;
	backup: number;
	combined: number;
	recovered: number;
	/** Per UTC hour: settled slots, primary, backup, combined. */
	hours: Array<[number, number, number, number]>;
}

export interface FeedCoverage {
	complete: boolean;
	today: FeedCoverageDay;
	/** Full UTC date + HH:MM labels. */
	missing: string[];
	unexplained: string[];
	recent: { primary: string[]; backup: string[]; combined: string[] };
}
export interface FeedGaps {
	/** Settled minutes today (excludes the current + previous, in flight). */
	settled: number;
	missing: number;
	/** `HH:MM` of holes adjacent to a missing heartbeat. */
	unexplained: string[];
	/** Settled minutes with no heartbeat (CF skipped the cron). */
	cronSkips: number;
	/** Missing WAL minute slots in the last ten settled minutes, across UTC midnight.
	 *  Tick presence cannot distinguish upstream omission from stale-cache loss. */
	recentMissing?: string[];
}
