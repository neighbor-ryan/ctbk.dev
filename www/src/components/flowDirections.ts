import type { FlowPath } from '../query/flowRoutes'

type Point = [number, number]
interface Projection { project: (p: Point) => number[]; unproject: (p: Point) => number[] }

/** Screen-space right-hand lanes and regularly spaced forward chevrons. */
export function directedStreets(paths: readonly FlowPath[], projection: Projection, width: (count: number) => number) {
  const codes = new Set(paths.map((p) => p.code))
  const arrows: { path: Point[]; count: number }[] = []
  const streets = paths.map((flow) => {
    const points = flow.path.map((p) => projection.project(p) as Point)
    const offset = codes.has(-flow.code) ? width(flow.count) / 2 + 2 : 0
    const shifted = points.map((p, i): Point => {
      const a = points[Math.max(0, i - 1)], b = points[Math.min(points.length - 1, i + 1)]
      const dx = b[0] - a[0], dy = b[1] - a[1], length = Math.hypot(dx, dy)
      return length ? [p[0] - dy / length * offset, p[1] + dx / length * offset] : p
    })
    let next = 35, distance = 0
    for (let i = 1; i < shifted.length; i++) {
      const a = shifted[i - 1], b = shifted[i]
      const length = Math.hypot(b[0] - a[0], b[1] - a[1])
      if (!length) continue
      const dx = (b[0] - a[0]) / length, dy = (b[1] - a[1]) / length
      while (next <= distance + length) {
        const f = (next - distance) / length, x = a[0] + f * (b[0] - a[0]), y = a[1] + f * (b[1] - a[1])
        const size = Math.max(4, Math.min(7, width(flow.count) / 2 + 2))
        const chevron: Point[] = [[x - dx * size - dy * size * .6, y - dy * size + dx * size * .6], [x, y], [x - dx * size + dy * size * .6, y - dy * size - dx * size * .6]]
        arrows.push({ path: chevron.map((p) => projection.unproject(p) as Point), count: flow.count })
        next += 90
      }
      distance += length
    }
    return { ...flow, path: shifted.map((p) => projection.unproject(p) as Point) }
  })
  return { streets, arrows }
}
