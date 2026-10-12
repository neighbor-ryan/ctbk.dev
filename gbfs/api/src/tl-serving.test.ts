import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';
import { memStorage } from 'pyrmts';
import { chunkSize, frameOf } from '../../../www/src/lib/tlGrid';
import { serveTl } from './tl';

const day = Date.UTC(2025, 5, 10);
const iso = (ms: number) => new Date(ms).toISOString().slice(0, 19);
const enc = (s: string) => new TextEncoder().encode(s);
const records = [1, 15].map((minutes) => {
	const bytes = new Uint8Array(readFileSync(new URL(`./fixtures/tl-${minutes}m.parquet`, import.meta.url)));
	const key = `fixture/${minutes}m.parquet`;
	return { key, bytes, manifest: { tier: `${minutes}m`, shard_dur: '1d', period_start: day, period_end: day + 86400000, key, bytes: bytes.length, written_at: 1 } };
});

describe('real parquet → derived CSR responses', () => {
	it.each([
		{ bin: '20m', chunk: 4392, frames: 72, tier: '1m', covered: [[0, 72]], ptr: [...new Array(41).fill(0), 1, ...new Array(31).fill(3)], idx: [0, 0, 1], val: [3, 8, 4], partial: false, from: day, gaps: [] },
		{ bin: '45m', chunk: 2196, frames: 64, tier: '15m', covered: [[32, 64]], ptr: [...new Array(51).fill(0), ...new Array(14).fill(2)], idx: [0, 1], val: [11, 4], partial: true, from: day - 86400000, gaps: [[iso(day - 86400000), iso(day)]] },
	])('reads the coarsest nesting tier for $bin and preserves partial coverage', async (test) => {
		const storage = memStorage(new Map([
			...records.map((r): [string, Uint8Array] => [r.key, r.bytes]),
			['fixture/start/manifest.jsonl', enc(records.map((r) => JSON.stringify(r.manifest)).join('\n'))],
			['fixture/start/final.json', enc(JSON.stringify({ finalThrough: '2025-07-01T00:00:00Z' }))],
		]));
		expect(chunkSize(test.bin)).toBe(test.frames);
		expect(Math.floor(frameOf(test.bin, day) / test.frames)).toBe(test.chunk);
		const response = await serveTl(storage, new Request(`https://x/api/tl?gv=1&raw=1&bin=${test.bin}&chunk=${test.chunk}`), '', { prefix: 'fixture', canonicalized: false });
		expect(await response.json()).toEqual({
			anchor: 'start', bin: test.bin, chunk: test.chunk, k: test.frames,
			t0: iso(test.from), t1: iso(day + 86400000), gv: 1, ids: ['a', 'b'],
			fmt: 'csr', ptr: test.ptr, idx: test.idx, val: test.val, unmapped: new Array(test.frames).fill(0),
			partial: test.partial, covered: test.covered,
			plan: { reads: [{ tier: test.tier, shard: '1d', key: `fixture/${test.tier}.parquet`, from: iso(day), to: iso(day + 86400000) }], gaps: test.gaps },
		});
		expect(response.headers.get('Cache-Control')).toBe(test.partial ? 'public, max-age=3600' : 'public, max-age=86400, immutable');
	});
});
