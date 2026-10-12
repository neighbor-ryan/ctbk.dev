import { describe, expect, it } from 'vitest'
import { decodeOrder, encodeOrder, parseLength, rangeForFrames } from './timelapseLinked'

describe('linked control URL and date edges', () => {
  it('round-trips recency and rejects malformed orders', () => {
    expect(decodeOrder('Lfdb')).toEqual(['L', 'f', 'R', 'b'])
    expect(encodeOrder(['f', 'R', 'b', 'L'])).toBe('fdbL')
    expect(encodeOrder(['L', 'f', 'R', 'b'])).toBeUndefined()
    expect(decodeOrder('ffff')).toEqual(['L', 'f', 'R', 'b'])
    expect(['60', '1m30s', '0', 'bad'].map(parseLength)).toEqual([60, 90, null, null])
  })
  it('moves the start at the data edge while preserving duration, including calendar frames', () => {
    expect(rangeForFrames('1h', Date.UTC(2025, 5, 28), 7 * 24, Date.UTC(2025, 5, 30))).toEqual([Date.UTC(2025, 5, 24), Date.UTC(2025, 5, 30)])
    expect(rangeForFrames('1mo', Date.UTC(2024, 0, 1), 2, Date.UTC(2024, 2, 31))).toEqual([Date.UTC(2024, 0, 1), Date.UTC(2024, 1, 29)])
  })
})
