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
