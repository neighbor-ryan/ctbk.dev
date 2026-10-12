/** Shared, versioned timelapse grid. Local wall-clock timestamps are UTC
 *  values here; calendar bins advance calendar months, never nominal days. */
export const TL_GRID_VERSION = 1;
export const TL_GENESIS_MS = Date.UTC(2013, 5, 1);
export const DAY_MS = 86_400_000;
export type TlBin = string;

export type ParsedBin =
	| { code: TlBin; kind: 'fixed'; minutes: number }
	| { code: TlBin; kind: 'calendar'; months: number };

export function parseBin(raw: string): ParsedBin {
	const match = /^(\d+)(min|mo|m|h|d|y)$/.exec(raw);
	if (!match) throw new Error(`invalid bin '${raw}'`);
	const count = Number(match[1]);
	if (!Number.isSafeInteger(count) || count < 1 || count > 1_000_000) throw new Error(`invalid bin '${raw}'`);
	const unit = match[2];
	if (unit === 'mo' || unit === 'y') {
		const months = count * (unit === 'y' ? 12 : 1);
		if (months > 12 || 12 % months !== 0) throw new Error(`calendar bin '${raw}' must divide one year`);
		return { code: months === 12 ? '1y' : `${months}mo`, kind: 'calendar', months };
	}
	const minutes = count * (unit === 'd' ? 1440 : unit === 'h' ? 60 : 1);
	if (minutes < 1440 ? 1440 % minutes !== 0 : minutes % 1440 !== 0) {
		throw new Error(`fixed bin '${raw}' must divide a day or span whole days`);
	}
	const code = minutes % 1440 === 0 ? `${minutes / 1440}d`
		: minutes % 60 === 0 ? `${minutes / 60}h` : `${minutes}m`;
	return { code, kind: 'fixed', minutes };
}

export const canonBin = (raw: string): TlBin => parseBin(raw).code;

/** Measured June 2025 station-bin rows per ride; interpolation only sets
 *  chunk size. Once version 1 ships, changing this table requires a new
 *  grid version because chunk URLs must retain their meaning. */
const DENSITY: readonly (readonly [number, number])[] = [
	[1, .873], [2, .796], [5, .649], [10, .511], [15, .428],
	[30, .297], [60, .193], [120, .118], [240, .070], [1440, .014],
];

function rowsPerDay(minutes: number): number {
	let ratio = DENSITY[DENSITY.length - 1][1];
	for (let i = 1; i < DENSITY.length; i++) {
		const [a, ra] = DENSITY[i - 1];
		const [b, rb] = DENSITY[i];
		if (minutes > b) continue;
		const t = Math.log(minutes / a) / Math.log(b / a);
		ratio = Math.exp(Math.log(ra) + t * Math.log(rb / ra));
		break;
	}
	return Math.min(2500 * 1440 / minutes, 200_000 * ratio);
}

export function chunkSize(bin: TlBin): number {
	const spec = parseBin(bin);
	if (spec.kind === 'calendar') return Math.max(2, 24 / spec.months);
	if (spec.minutes >= 1440) return 32;
	const rows = rowsPerDay(spec.minutes);
	const spans = [360, 720, 1440, 2880, 5760, 11520, 23040, 46080]
		.filter((span) => span % spec.minutes === 0 && rows * span / 1440 <= 100_000);
	if (!spans.length) throw new Error(`no bounded chunk span for '${bin}'`);
	return spans[spans.length - 1] / spec.minutes;
}

const monthIdx = (ms: number): number => {
	const d = new Date(ms);
	return d.getUTCFullYear() * 12 + d.getUTCMonth();
};
const monthMs = (i: number): number => Date.UTC(Math.floor(i / 12), ((i % 12) + 12) % 12);

export function originMs(bin: TlBin): number {
	const spec = parseBin(bin);
	const k = chunkSize(bin);
	if (spec.kind === 'calendar') {
		const span = spec.months * k;
		return monthMs(Math.floor(monthIdx(TL_GENESIS_MS) / span) * span);
	}
	const span = spec.minutes * 60_000 * k;
	return Math.floor(TL_GENESIS_MS / span) * span;
}

export function frameOf(bin: TlBin, ms: number): number {
	const spec = parseBin(bin);
	return Math.floor(spec.kind === 'calendar'
		? (monthIdx(ms) - monthIdx(originMs(bin))) / spec.months
		: (ms - originMs(bin)) / (spec.minutes * 60_000));
}

export function frameStartMs(bin: TlBin, i: number): number {
	const spec = parseBin(bin);
	return spec.kind === 'calendar'
		? monthMs(monthIdx(originMs(bin)) + i * spec.months)
		: originMs(bin) + i * spec.minutes * 60_000;
}

export function chunkRange(bin: TlBin, k: number): [number, number] {
	const size = chunkSize(bin);
	return [frameStartMs(bin, k * size), frameStartMs(bin, (k + 1) * size)];
}

export function nominalBinMs(bin: TlBin): number {
	const spec = parseBin(bin);
	return spec.kind === 'calendar' ? spec.months * 30.436875 * DAY_MS : spec.minutes * 60_000;
}
