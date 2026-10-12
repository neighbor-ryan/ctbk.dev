import { chunkSize, parseBin } from '../lib/tlGrid'

export type Kernel = 'box' | 'tri'
export type SparseFrame = readonly (readonly [number, number])[]
export interface WindowSample { values: Float64Array; missing: boolean }

export function windowFrames(bin: string, window: string): number {
  const b = parseBin(bin)
  const w = parseBin(window)
  if (b.kind !== w.kind) throw new Error('Window and bin must use the same time units')
  const step = b.kind === 'fixed' ? b.minutes : b.months
  const span = w.kind === 'fixed' ? w.minutes : w.months
  if (span % step !== 0 || span < step) throw new Error('Window must be a whole multiple of the bin')
  const n = span / step
  if (n > chunkSize(bin)) throw new Error('Window cannot exceed one chunk')
  return n
}

/** Box windows update only incoming/outgoing nonzero entries. A seek
 *  rebuilds the window. Triangle weights preserve the total window mass. */
export class WindowAccumulator {
  private frame: number | null = null
  private history: (SparseFrame | null)[] = []
  private sum: Float64Array

  constructor(private readonly size: number, private readonly width: number, private readonly kernel: Kernel) {
    this.sum = new Float64Array(size)
  }

  sample(i: number, read: (i: number) => SparseFrame | null | undefined): WindowSample | undefined {
    if (this.frame === i) return this.result()
    const add = (entries: SparseFrame | null, weight: number) => {
      for (const [index, value] of entries ?? []) this.sum[index] += weight * value
    }
    if (this.frame === i - 1) {
      const incoming = read(i)
      if (incoming === undefined) return undefined
      add(this.history.shift()!, -1)
      this.history.push(incoming)
      add(incoming, 1)
    } else {
      const history: (SparseFrame | null)[] = []
      for (let f = i - this.width + 1; f <= i; f++) {
        const entries = read(f)
        if (entries === undefined) return undefined
        history.push(entries)
      }
      this.history = history
      this.sum = new Float64Array(this.size)
      for (const entries of history) add(entries, 1)
    }
    this.frame = i
    return this.result()
  }

  private result(): WindowSample {
    if (this.kernel === 'box') return { values: this.sum.slice(), missing: this.history.some((f) => f === null) }
    const values = new Float64Array(this.size)
    this.history.forEach((entries, f) => {
      const weight = 2 * (f + 1) / (this.width + 1)
      for (const [index, value] of entries ?? []) values[index] += weight * value
    })
    return { values, missing: this.history.some((f) => f === null) }
  }
}
