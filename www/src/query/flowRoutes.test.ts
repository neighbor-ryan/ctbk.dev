import { describe, expect, it } from 'vitest'
import { assignRoutes, mergeRoutes, routeGeometry, systemFlowPaths, type RouteSample } from './flowRoutes'

const stations = Object.fromEntries(['A', 'B', 'C', 'D'].map((name, i) => [name, { name, lng: i, lat: i, ends: 0 }]))
const sample: RouteSample = {
  version: 1,
  segments: [{ path: [[0, 0], [1, 1]] }, { path: [[1, 1], [2, 2]] }, { path: [[2, 2], [3, 3]] }],
  pairs: [
    { source: 'A', destination: 'B', routes: [{ weight: 0.75, segments: [1] }, { weight: 0.25, segments: [2] }] },
    { source: 'A', destination: 'C', routes: [{ weight: 0.5, segments: [1, 3] }, { weight: 0.5, segments: [2, 3] }] },
    { source: 'B', destination: 'A', routes: [{ weight: 0.25, segments: [-1] }, { weight: 0.75, segments: [-2] }] },
  ],
}
describe('street assignment', () => {
  it('renders every positive precomputed network volume by default, retaining explicit floors', () => {
    expect(systemFlowPaths(sample, [[1, 100], [-2, 30], [3, 3]])).toEqual([
      { code: 3, count: 3, path: [[2, 2], [3, 3]] },
      { code: -2, count: 30, path: [[2, 2], [1, 1]] },
      { code: 1, count: 100, path: [[0, 0], [1, 1]] },
    ])
    expect(systemFlowPaths(sample, [[1, 100], [-2, 30], [3, 3]], .04)).toEqual([
      { code: -2, count: 30, path: [[2, 2], [1, 1]] },
      { code: 1, count: 100, path: [[0, 0], [1, 1]] },
    ])
    expect(() => systemFlowPaths(sample, [[0, 100]])).toThrow('Invalid system street volume')
    expect(() => systemFlowPaths(sample, [[4, 100]])).toThrow('Invalid system street volume')
  })
  it('merges namespaces without losing direction and honors explicit NJ fallbacks', () => {
    const merged = mergeRoutes(sample, [{ path: [[4, 4], [5, 5]] }], [{ pairs: [
      { source: 'A', destination: 'B', routes: [{ weight: 1, segments: [-1] }] },
      { source: 'A', destination: 'C', routes: [] },
    ] }])
    expect(merged.pairs).toEqual([
      { source: 'A', destination: 'B', routes: [{ weight: 1, segments: [-4] }] },
      { source: 'A', destination: 'C', routes: [] },
      sample.pairs[2],
    ])
    expect(routeGeometry(merged, merged.pairs[0].routes[0])).toEqual([[5, 5], [4, 4]])
    expect(routeGeometry(sample, { weight: 1, segments: [1, 2] })).toEqual([[0, 0], [1, 1], [2, 2]])
  })
  it('adds expected alternative volume to shared directional segments, retaining uncovered pairs as arcs', () => {
    expect(assignRoutes(sample, stations, { A: { B: 100, C: 60, D: 20, A: 8 } }, ['A'], 'out')).toEqual({
      paths: [
        { code: 2, path: [[1, 1], [2, 2]], count: 55 },
        { code: 3, path: [[2, 2], [3, 3]], count: 60 },
        { code: 1, path: [[0, 0], [1, 1]], count: 105 },
      ],
      arcs: [{ from: 'A', to: 'D', source: [0, 0], target: [3, 3], count: 20 }], routed: 160, total: 180,
    })
    expect(assignRoutes(sample, stations, { B: { A: 80 }, A: { B: 100 } }, ['A'], 'in')).toEqual({
      paths: [{ code: -1, path: [[1, 1], [0, 0]], count: 20 }, { code: -2, path: [[2, 2], [1, 1]], count: 60 }],
      arcs: [], routed: 80, total: 80,
    })
  })
  it('uses each actual pair for multiple selected sources and excludes internal pairs', () => {
    expect(assignRoutes(sample, stations, { A: { B: 100, C: 60 }, B: { A: 80, D: 1 } }, ['A', 'B'], 'out')).toEqual({
      paths: [
        { code: 1, path: [[0, 0], [1, 1]], count: 30 },
        { code: 2, path: [[1, 1], [2, 2]], count: 30 },
        { code: 3, path: [[2, 2], [3, 3]], count: 60 },
      ], arcs: [], routed: 60, total: 60,
    })
  })
})
