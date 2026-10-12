/**
 * Timelapse control-bar logic (`/timelapse`, `specs/timelapse-map.md` "UX"):
 * the pure half of the range picker (presets, clamping, the frame-count
 * guard), per-bin labels (clock, hover tip, speed, step units). No React;
 * unit-tested in `timelapseControls.test.ts`.
 *
 * Ranges are inclusive local days `[a, b]` as local-as-UTC midnights (the
 * `d=` param); a range covers the frames containing its first and last
 * instants, so a coarse bin's first/last frame may overhang the range.
 */
import { BIN_OPTIONS, binMs, DAY_MS, frameIndex, type Bin } from './timelapseFrames'
import { parseBin } from '../lib/tlGrid'

export type Range = [number, number]

/** `YYYYMM` (`station-urls.json` `latestMonth`) → its last day. */
export function lastDataDay(latestMonth: string): number | null {
  const m = /^(\d{4})(\d{2})$/.exec(latestMonth)
  return m ? Date.UTC(Number(m[1]), Number(m[2]), 0) : null
}

/** Inclusive frame range `[iStart, iEnd]` covering the day range. */
export function rangeFrames(bin: Bin, [a, b]: Range): [number, number] {
  return [frameIndex(bin, a), frameIndex(bin, b + DAY_MS - 1)]
}

export function frameCount(bin: Bin, range: Range): number {
  const [i0, i1] = rangeFrames(bin, range)
  return i1 - i0 + 1
}

// ---------------------------------------------------------------------------
// Spans + presets.
// ---------------------------------------------------------------------------

/** A range length: `n` days or calendar months, or the whole history. */
export type Span = { n: number; u: 'd' | 'mo' } | 'all'
export interface RangePreset {
  label: string
  span: Span
}

const d = (n: number): Span => ({ n, u: 'd' })
const mo = (n: number): Span => ({ n, u: 'mo' })
const P = (label: string, span: Span): RangePreset => ({ label, span })

/** Quick range presets per bin (each well under `SOFT_FRAMES`). */
export const RANGE_PRESETS: Record<Bin, RangePreset[]> = {
  '1h': [P('1d', d(1)), P('3d', d(3)), P('1w', d(7)), P('2w', d(14))],
  '3h': [P('3d', d(3)), P('1w', d(7)), P('2w', d(14)), P('1mo', mo(1))],
  '6h': [P('1w', d(7)), P('2w', d(14)), P('1mo', mo(1)), P('3mo', mo(3))],
  '12h': [P('2w', d(14)), P('1mo', mo(1)), P('3mo', mo(3)), P('6mo', mo(6))],
  '1d': [P('1mo', mo(1)), P('3mo', mo(3)), P('1y', mo(12)), P('all', 'all')],
  '3d': [P('3mo', mo(3)), P('1y', mo(12)), P('3y', mo(36)), P('all', 'all')],
  '7d': [P('1y', mo(12)), P('3y', mo(36)), P('all', 'all')],
  '14d': [P('1y', mo(12)), P('3y', mo(36)), P('all', 'all')],
  '1mo': [P('2y', mo(24)), P('5y', mo(60)), P('all', 'all')],
}

/** The span a bin falls back to (the default `d`, and the window a bin
 *  switch re-fits to when the old range makes no sense at the new bin). */
export const DEFAULT_SPAN: Record<Bin, Span> = {
  '1h': d(7), '3h': d(14), '6h': mo(1), '12h': mo(3), '1d': mo(12), '3d': mo(12), '7d': mo(36), '14d': mo(60), '1mo': 'all',
}

function addMonths(ms: number, n: number): number {
  const t = new Date(ms)
  return Date.UTC(t.getUTCFullYear(), t.getUTCMonth() + n, t.getUTCDate())
}

/** The `span`-long window ending on day `end` (clamped at `lo`). */
export function spanEnding(span: Exclude<Span, 'all'>, end: number, lo: number): Range {
  const a = span.u === 'd' ? end - (span.n - 1) * DAY_MS : addMonths(end + DAY_MS, -span.n)
  return [Math.max(lo, a), end]
}

/** The `span`-long window starting on day `start` (clamped at `hi`). */
export function spanStarting(span: Exclude<Span, 'all'>, start: number, hi: number): Range {
  const b = span.u === 'd' ? start + (span.n - 1) * DAY_MS : addMonths(start, span.n) - DAY_MS
  return [start, Math.min(hi, b)]
}

/**
 * Apply a `span` to the current range, keeping the playhead's day `tDay`
 * inside it: the window ending on the current end, if that contains `tDay`
 * (so widening or narrowing at the end "zooms" in place); else the window
 * starting at `tDay` (playback runs forward from the playhead), shifted back
 * to end at `hi` if it would run past the data. All within `[lo, hi]`.
 */
export function spanRange(span: Span, [, b]: Range, tDay: number, lo: number, hi: number): Range {
  if (span === 'all') return [lo, hi]
  const t = Math.min(hi, Math.max(lo, tDay))
  const back = spanEnding(span, Math.min(hi, Math.max(lo, b)), lo)
  if (t >= back[0] && t <= back[1]) return back
  const fwd = spanStarting(span, t, hi)
  const full = spanStarting(span, t, Infinity)
  return fwd[1] === full[1] ? fwd : spanEnding(span, hi, lo)
}

/** Clamp a day to `[lo, hi]`, then clamp the range's other end so `a ≤ b`,
 *  moving the side that wasn't edited. */
export function editRange([a, b]: Range, side: 'start' | 'end', day: number, lo: number, hi: number): Range {
  const v = Math.min(hi, Math.max(lo, day))
  if (side === 'start') return [v, Math.max(v, b)]
  return [Math.min(a, v), v]
}

// ---------------------------------------------------------------------------
// Frame-count guard.
// ---------------------------------------------------------------------------

/** Above this many frames the bar suggests a coarser bin. */
export const SOFT_FRAMES = 1500
/** Hard cap: a range edit past it is trimmed (keeps `1d` over all of
 *  history, ~4,850 frames today, while `1h` stops at 250 days). */
export const HARD_FRAMES = 6000
/** A bin switch to a coarser bin re-fits ranges with fewer frames than this. */
export const MIN_FRAMES = 4

/** Longest range (days) allowed at `bin` by `HARD_FRAMES`. */
export function maxDays(bin: Bin): number {
  return Math.max(1, Math.floor((HARD_FRAMES * binMs(bin)) / DAY_MS))
}

/** Trim a range over `HARD_FRAMES` to `maxDays(bin)`, keeping the side
 *  the user just set (`keep`). */
export function capRange(bin: Bin, range: Range, keep: 'start' | 'end'): { range: Range; capped: boolean } {
  if (frameCount(bin, range) <= HARD_FRAMES) return { range, capped: false }
  const n = maxDays(bin)
  const [a, b] = range
  return { range: keep === 'start' ? [a, a + (n - 1) * DAY_MS] : [b - (n - 1) * DAY_MS, b], capped: true }
}

/** The finest bin that plays `range` in ≤ `SOFT_FRAMES` frames. */
export function suggestBin(range: Range): Bin {
  return BIN_OPTIONS.find((b) => frameCount(b, range) <= SOFT_FRAMES) ?? BIN_OPTIONS[BIN_OPTIONS.length - 1]
}

/** Range to use after switching `from` → `to`: unchanged unless it's too
 *  many frames at a finer bin (> `SOFT_FRAMES`) or too few at a coarser one
 *  (< `MIN_FRAMES`), in which case the new bin's `DEFAULT_SPAN` around the
 *  playhead (`spanRange`). */
export function fitRangeToBin(from: Bin, to: Bin, range: Range, tDay: number, lo: number, hi: number): Range {
  const n = frameCount(to, range)
  const finer = binMs(to) < binMs(from)
  const coarser = binMs(to) > binMs(from)
  if ((finer && n > SOFT_FRAMES) || (coarser && n < MIN_FRAMES)) return spanRange(defaultSpan(to), range, tDay, lo, hi)
  return range
}

// ---------------------------------------------------------------------------
// Labels.
// ---------------------------------------------------------------------------

const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const MONTH = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
const pad2 = (n: number) => String(n).padStart(2, '0')


/** `18:00` (`1h`) or `18:00–21:00` (coarser sub-day bins). */
function hoursLabel(bin: Bin, ms: number): string {
  const t = new Date(ms)
  const minutes = t.getUTCHours() * 60 + t.getUTCMinutes()
  const spec = parseBin(bin)
  const width = spec.kind === 'fixed' ? spec.minutes : 60
  const fmt = (m: number) => `${pad2(Math.floor(m / 60))}:${pad2(m % 60)}`
  return width <= 60 ? fmt(minutes) : `${fmt(minutes)}–${fmt(minutes + width)}`
}

/** `Jun 5–11, 2025` / `Jun 28 – Jul 4, 2025` / `Dec 29, 2025 – Jan 4, 2026`. */
function daysLabel(a: number, b: number): string {
  const x = new Date(a)
  const y = new Date(b)
  const [xm, xd, xy] = [x.getUTCMonth(), x.getUTCDate(), x.getUTCFullYear()]
  const [ym, yd, yy] = [y.getUTCMonth(), y.getUTCDate(), y.getUTCFullYear()]
  if (xy !== yy) return `${MON[xm]} ${xd}, ${xy} – ${MON[ym]} ${yd}, ${yy}`
  if (xm !== ym) return `${MON[xm]} ${xd} – ${MON[ym]} ${yd}, ${yy}`
  return `${MON[xm]} ${xd}–${yd}, ${yy}`
}

/** The big clock label for the frame starting at `ms`: `Tue, Jun 10, 2025 ·
 *  18:00` (sub-day; `18:00–21:00` past `1h`), `Tue, Jun 10, 2025` (`1d`), a
 *  day span (`3d`/`7d`/`14d`), `June 2025` (`1mo`). */
export function clockLabel(bin: Bin, ms: number): string {
  const t = new Date(ms)
  const day = `${DOW[t.getUTCDay()]}, ${MON[t.getUTCMonth()]} ${t.getUTCDate()}, ${t.getUTCFullYear()}`
  if (binMs(bin) < DAY_MS) return `${day} · ${hoursLabel(bin, ms)}`
  if (bin === '1d') return day
  const parsed = parseBin(bin)
  const n = parsed.kind === 'fixed' ? parsed.minutes / 1440 : undefined
  if (n) return daysLabel(ms, ms + (n - 1) * DAY_MS)
  const spec = parseBin(bin)
  if (spec.kind === 'calendar' && spec.months === 12) return String(t.getUTCFullYear())
  if (spec.kind === 'calendar' && spec.months > 1) return `${MONTH[t.getUTCMonth()]}–${MONTH[t.getUTCMonth() + spec.months - 1]} ${t.getUTCFullYear()}`
  return `${MONTH[t.getUTCMonth()]} ${t.getUTCFullYear()}`
}

/** Compact label (the scrubber's hover tip): `Tue Jun 10 · 18:00`
 *  (sub-day, no year), `Tue Jun 10, 2025` (`1d`), the day span, `Jun 2025`. */
export function shortLabel(bin: Bin, ms: number): string {
  const t = new Date(ms)
  if (binMs(bin) < DAY_MS) return `${DOW[t.getUTCDay()]} ${MON[t.getUTCMonth()]} ${t.getUTCDate()} · ${hoursLabel(bin, ms)}`
  if (bin === '1d') return `${DOW[t.getUTCDay()]} ${MON[t.getUTCMonth()]} ${t.getUTCDate()}, ${t.getUTCFullYear()}`
  const parsed = parseBin(bin)
  const n = parsed.kind === 'fixed' ? parsed.minutes / 1440 : undefined
  if (n) return daysLabel(ms, ms + (n - 1) * DAY_MS)
  return `${MON[t.getUTCMonth()]} ${t.getUTCFullYear()}`
}

/** Selector label per bin. */
export const BIN_LABEL: Record<Bin, string> = {
  '1h': '1 hour', '3h': '3 hours', '6h': '6 hours', '12h': '12 hours',
  '1d': '1 day', '3d': '3 days', '7d': '1 week', '14d': '2 weeks', '1mo': '1 month',
}

/** "Rides per {unit}", "Previous {unit}". */
export const UNIT: Record<Bin, string> = {
  '1h': 'hour', '3h': '3 hours', '6h': '6 hours', '12h': '12 hours',
  '1d': 'day', '3d': '3 days', '7d': 'week', '14d': '2 weeks', '1mo': 'month',
}

/** shift+←/→: frames per big step, and its label. */
export const BIG_STEP: Record<Bin, { n: number; label: string }> = {
  '1h': { n: 24, label: 'day' },
  '3h': { n: 8, label: 'day' },
  '6h': { n: 28, label: 'week' },
  '12h': { n: 14, label: 'week' },
  '1d': { n: 7, label: 'week' },
  '3d': { n: 10, label: '30 days' },
  '7d': { n: 4, label: '4 weeks' },
  '14d': { n: 2, label: '4 weeks' },
  '1mo': { n: 12, label: 'year' },
}

/** Playback speeds, in frames per second. */
export const SPEEDS = [1, 2, 4, 8, 16, 30]

/** `sp` frames/s in the bin's units: `8 h/s` (`1h`), `24 h/s` (`3h` × 8),
 *  `8 d/s` (`1d`), `2 wk/s` (`14d` × 1), `8 mo/s` (`1mo`). */
export function speedLabel(bin: Bin, sp: number): string {
  const spec = parseBin(bin)
  if (spec.kind === 'fixed' && spec.minutes < 60) return `${Number((sp * spec.minutes).toFixed(3))} min/s`
  const h = spec.kind === 'fixed' && spec.minutes < 1440 ? spec.minutes / 60 : null
  if (h) return `${Number((sp * h).toFixed(3))} h/s`
  if (bin === '1d' || bin === '3d') return `${sp * (bin === '3d' ? 3 : 1)} d/s`
  if (bin === '7d' || bin === '14d') return `${sp * (bin === '14d' ? 2 : 1)} wk/s`
  return spec.kind === 'calendar' ? `${sp * spec.months} mo/s` : `${sp * spec.minutes / 1440} d/s`
}

/** Next slower / faster speed from `sp` (which needn't be in `SPEEDS`). */
export function stepSpeed(sp: number, dir: -1 | 1): number {
  if (dir > 0) return SPEEDS.find((s) => s > sp) ?? SPEEDS[SPEEDS.length - 1]
  return [...SPEEDS].reverse().find((s) => s < sp) ?? SPEEDS[0]
}

// ---------------------------------------------------------------------------
// `<input type="date">` values.
// ---------------------------------------------------------------------------

/** Local-as-UTC midnight → `YYYY-MM-DD`. */
export function isoDay(ms: number): string {
  const t = new Date(ms)
  return `${t.getUTCFullYear()}-${pad2(t.getUTCMonth() + 1)}-${pad2(t.getUTCDate())}`
}

/** `YYYY-MM-DD` → local-as-UTC midnight, or null (empty / malformed). */
export function parseIsoDay(s: string): number | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s)
  if (!m) return null
  const ms = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]))
  return isoDay(ms) === s ? ms : null
}

/** Midnight of the day containing `ms`. */
export function dayOf(ms: number): number {
  return Math.floor(ms / DAY_MS) * DAY_MS
}

/** Derived-bin controls use the same calendar/fixed semantics as the grid. */
export function binLabel(bin: Bin): string {
  if (BIN_LABEL[bin]) return BIN_LABEL[bin]
  const spec = parseBin(bin)
  const n = spec.kind === 'calendar' ? (spec.months === 12 ? 1 : spec.months)
    : spec.minutes >= 1440 ? spec.minutes / 1440 : spec.minutes >= 60 ? spec.minutes / 60 : spec.minutes
  const unit = spec.kind === 'calendar' ? (spec.months === 12 ? 'year' : 'month')
    : spec.minutes >= 1440 ? 'day' : spec.minutes >= 60 ? 'hour' : 'minute'
  return `${n} ${unit}${n === 1 ? '' : 's'}`
}
export const binUnit = (bin: Bin): string => UNIT[bin] ?? binLabel(bin).replace(/^1 /, '')
export const defaultSpan = (bin: Bin): Span => DEFAULT_SPAN[bin] ?? (binMs(bin) < 3_600_000 ? d(1) : binMs(bin) < DAY_MS ? d(7) : 'all')
export const rangePresets = (bin: Bin): RangePreset[] => RANGE_PRESETS[bin] ?? (binMs(bin) < 3_600_000
  ? [P('1d', d(1)), P('3d', d(3))] : RANGE_PRESETS[binMs(bin) < DAY_MS ? '1h' : '1mo'])
export function bigStep(bin: Bin): { n: number; label: string } {
  return BIG_STEP[bin] ?? (binMs(bin) < DAY_MS ? { n: Math.round(DAY_MS / binMs(bin)), label: 'day' } : { n: 1, label: binUnit(bin) })
}

export function stepBin(bin: Bin, direction: -1 | 1): Bin {
  return direction === 1 ? BIN_OPTIONS.find((b) => binMs(b) > binMs(bin)) ?? BIN_OPTIONS[BIN_OPTIONS.length - 1]
    : [...BIN_OPTIONS].reverse().find((b) => binMs(b) < binMs(bin)) ?? BIN_OPTIONS[0]
}
