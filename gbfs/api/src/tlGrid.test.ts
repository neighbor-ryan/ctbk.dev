import { describe, expect, it } from 'vitest';
import { canonBin, chunkRange, chunkSize, frameOf, frameStartMs, originMs, parseBin, TL_GENESIS_MS } from '../../../www/src/lib/tlGrid';

describe('shared timelapse grid v1', () => {
	it('canonicalizes aliases and rejects bins that straddle daily/calendar boundaries', () => {
		expect(['60min', '24h', '90min', '12mo', '4mo'].map(canonBin)).toEqual(['1h', '1d', '90m', '1y', '4mo']);
		for (const bin of ['0m', '7m', '5h', '5mo', '2y', '9007199254740992m']) {
			expect(() => parseBin(bin)).toThrow();
		}
	});

	it('preserves all deployed chunk sizes and adds bounded subhour/derived chunks', () => {
		expect(['1h', '3h', '6h', '12h', '1d', '3d', '7d', '14d', '1mo', '1m', '5m', '15m', '30m', '2h', '4mo', '1y']
			.map(chunkSize)).toEqual([48, 32, 32, 32, 32, 32, 32, 32, 24, 720, 144, 96, 48, 48, 6, 2]);
	});

	it('keeps old immutable URLs on the same ranges and handles calendar/leap-day steps', () => {
		expect(chunkRange('1h', 2196)).toEqual([Date.UTC(2025, 5, 9), Date.UTC(2025, 5, 11)]);
		expect(chunkRange('1d', 137)).toEqual([Date.UTC(2025, 4, 16), Date.UTC(2025, 5, 17)]);
		expect(chunkRange('1mo', 6)).toEqual([Date.UTC(2024, 0, 1), Date.UTC(2026, 0, 1)]);
		expect(originMs('1m')).toBe(TL_GENESIS_MS);
		expect(frameStartMs('4mo', frameOf('4mo', Date.UTC(2024, 1, 29)))).toBe(Date.UTC(2024, 0, 1));
		expect(frameStartMs('1m', frameOf('1m', Date.UTC(2024, 1, 29, 23, 59, 59)))).toBe(Date.UTC(2024, 1, 29, 23, 59));
	});
});
