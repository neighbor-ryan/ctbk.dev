import { describe, expect, it } from 'vitest'
import { WebMercatorViewport } from '@deck.gl/core'
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
  it('keeps geographic lane and chevron positions independent of camera translation', () => {
    const paths = [
      { code: 1, count: 100, path: [[-74.03, 40.72], [-74.02, 40.73]] as [number, number][] },
      { code: -1, count: 20, path: [[-74.02, 40.73], [-74.03, 40.72]] as [number, number][] },
    ]
    const render = (longitude: number, latitude: number) => {
      const result = directedStreets(paths, new WebMercatorViewport({ longitude, latitude, zoom: 14 }), () => 4)
      return JSON.parse(JSON.stringify(result, (_key, value) => typeof value === 'number' ? Math.round(value * 1e8) / 1e8 : value))
    }
    expect(render(-74.03, 40.72)).toEqual(render(0, 0))
    expect(render(-73.9, 40.8)).toEqual(render(0, 0))
  })
})
