import { frameIndex, frameStartMs, parseT, type Bin } from './timelapseFrames'

/** Movie-only subrange: inclusive start, exclusive end, aligned to bin edges. */
export function movieRange(bin: Bin, start: number, end: number, clip?: string): [number, number] {
  if (!clip) return [start, end + 1]
  const parts = clip.split('-')
  const a = parts.length === 2 ? parseT(parts[0]) : null
  const b = parts.length === 2 ? parseT(parts[1]) : null
  if (a === null || b === null || a >= b) throw new Error('Movie clip needs a start before its exclusive end')
  const first = frameIndex(bin, a), after = frameIndex(bin, b)
  if (frameStartMs(bin, first) !== a || frameStartMs(bin, after) !== b) throw new Error('Movie clip must align to bin edges')
  if (first < start || after > end + 1) throw new Error('Movie clip is outside the date range')
  return [first, after]
}

/** Capture samples the same bins/second clock as interactive playback. */
export function movieGrid(bins: number, speed: number, fps: number, framesPerBin?: number) {
  if (!Number.isInteger(bins) || bins < 1) throw new Error('Movie needs a positive whole number of bins')
  if (![speed, fps].every((v) => Number.isFinite(v) && v > 0)) throw new Error('Movie rates must be positive')
  if (framesPerBin !== undefined && (!Number.isInteger(framesPerBin) || framesPerBin < 1)) throw new Error('Frames per bin must be a positive integer')
  const step = framesPerBin === undefined ? speed / fps : 1 / framesPerBin
  const frames = Math.ceil(Number((bins / step).toPrecision(12)))
  return {
    frames, fps, seconds: frames / fps, step,
    offset(i: number): number {
      if (!Number.isInteger(i) || i < 0 || i >= frames) throw new Error('Movie frame is out of range')
      return i * step
    },
  }
}
