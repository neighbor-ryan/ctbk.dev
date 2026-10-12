import { describe, expect, it } from 'vitest'
import { WindowAccumulator, windowFrames, type SparseFrame } from './timelapseWindow'

describe('trailing station windows', () => {
  const rows: SparseFrame[] = [[[0, 2]], [[1, 3]], [[0, 4]], [[1, 5]]]
  const read = (i: number) => i < 0 ? [] : rows[i]

  it('preserves exact sums on steps, reverse seeks, and genesis pre-roll', () => {
    const box = new WindowAccumulator(2, 3, 'box')
    expect([0, 2, 3, 1].map((i) => box.sample(i, read))).toEqual([
      { values: Float64Array.of(2, 0), missing: false },
      { values: Float64Array.of(6, 3), missing: false },
      { values: Float64Array.of(4, 8), missing: false },
      { values: Float64Array.of(2, 3), missing: false },
    ])
  })

  it('waits for absent chunks and keeps known gaps distinct from zero', () => {
    const box = new WindowAccumulator(2, 2, 'box')
    expect(box.sample(4, read)).toBeUndefined()
    expect(box.sample(2, (i) => i === 1 ? null : read(i))).toEqual({ values: Float64Array.of(4, 0), missing: true })
  })

  it('applies triangular weights with the same total mass as a box window', () => {
    const tri = new WindowAccumulator(2, 3, 'tri')
    expect(tri.sample(2, read)).toEqual({ values: Float64Array.of(7, 3), missing: false })
    expect(tri.sample(3, read)).toEqual({ values: Float64Array.of(4, 9), missing: false })
  })

  it('validates nesting and the two-chunk window limit', () => {
    expect([['1m', '30m'], ['5m', '1h'], ['2mo', '6mo']].map(([b, w]) => windowFrames(b, w))).toEqual([30, 12, 3])
    expect(() => windowFrames('15m', '20m')).toThrow(new Error('Window must be a whole multiple of the bin'))
    expect(() => windowFrames('1m', '1d')).toThrow(new Error('Window cannot exceed one chunk'))
    expect(() => windowFrames('1mo', '1d')).toThrow(new Error('Window and bin must use the same time units'))
  })
})
