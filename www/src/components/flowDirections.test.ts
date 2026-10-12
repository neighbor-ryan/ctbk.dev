import { describe, expect, it } from 'vitest'
import { directedStreets } from './flowDirections'

const projection = { project: (p: [number, number]) => p, unproject: (p: [number, number]) => p }
describe('street direction marks', () => {
  it('separates opposing lanes and points each chevron toward its destination', () => {
    expect(directedStreets([
      { code: 1, count: 100, path: [[0, 0], [100, 0]] },
      { code: -1, count: 100, path: [[100, 0], [0, 0]] },
    ], projection, () => 4)).toEqual({
      streets: [
        { code: 1, count: 100, path: [[0, 4], [100, 4]] },
        { code: -1, count: 100, path: [[100, -4], [0, -4]] },
      ], arrows: [
        { count: 100, path: [[31, 6.4], [35, 4], [31, 1.6]] },
        { count: 100, path: [[69, -6.4], [65, -4], [69, -1.6]] },
      ],
    })
  })
  it('leaves single-direction lanes centered and suppresses arrows on short edges', () => {
    expect(directedStreets([{ code: 1, count: 1, path: [[0, 0], [20, 0]] }], projection, () => 1)).toEqual({
      streets: [{ code: 1, count: 1, path: [[0, 0], [20, 0]] }], arrows: [],
    })
  })
})
