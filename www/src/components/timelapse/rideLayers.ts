import { TripsLayer } from '@deck.gl/geo-layers'
import { ScatterplotLayer } from '@deck.gl/layers'
import { routeGeometry, type RouteSample } from '../../query/flowRoutes'
import type { LoadedRideDay } from '../../query/useRideAnimation'

export interface MovingRide {
  path: [number, number][]
  times: number[]
  flags: number
  routed?: boolean
}

export const BIKE_COLORS = { electric: [202, 170, 255], classic: [80, 160, 255], unknown: [160, 160, 160] } as const

export function rideRgb(flags: number, colorBy: 'bike' | 'user'): readonly [number, number, number] {
  return colorBy === 'bike'
    ? BIKE_COLORS[flags & 8 ? 'unknown' : flags & 1 ? 'electric' : 'classic']
    : flags & 16 ? [160, 160, 160] : flags & 2 ? [255, 100, 190] : [100, 220, 110]
}

export function rideRouteOrigins(day: LoadedRideDay | undefined, bucket: number, trail: number): string[] {
  if (!day) return []
  const ids = new Set<string>()
  const { rides, stations } = day
  for (let i = 0; i < rides.starts.length; i++) {
    if (rides.starts[i] > bucket + 900 + trail || rides.starts[i] + rides.durations[i] < bucket - trail) continue
    const id = stations[rides.sources[i]].id
    if (id.startsWith('JC') || id.startsWith('HB')) ids.add(id)
  }
  return [...ids].sort()
}

/** Stable across day boundaries: carry-in retains the same absolute start. */
export function routeFraction(key: string): number {
  let hash = 2166136261
  for (let i = 0; i < key.length; i++) hash = Math.imul(hash ^ key.charCodeAt(i), 16777619)
  return (hash >>> 0) / 4294967296
}

export function pathTimes(path: [number, number][], start: number, end: number): number[] {
  const lengths = [0]
  for (let i = 1; i < path.length; i++) {
    const a = path[i - 1], b = path[i]
    const dx = (b[0] - a[0]) * Math.cos((a[1] + b[1]) / 2 * Math.PI / 180)
    lengths.push(lengths[i - 1] + Math.hypot(dx, b[1] - a[1]))
  }
  const total = lengths[lengths.length - 1]
  if (!(total > 0)) throw new Error('Route geometry has no length')
  return lengths.map((length) => start + (end - start) * length / total)
}

/** Geometry is held for a 15-minute clock bucket; currentTime moves on the GPU. */
export function ridePaths(day: LoadedRideDay, bucket: number, trail: number, routes?: RouteSample, midnight = 0): MovingRide[] {
  const { rides: r, stations } = day
  const paths: MovingRide[] = []
  const lookup = new Map(routes?.pairs.map((p) => [JSON.stringify([p.source, p.destination]), p.routes]))
  for (let i = 0; i < r.starts.length; i++) {
    const start = r.starts[i]
    const end = start + r.durations[i]
    if (start > bucket + 900 + trail || end < bucket - trail) continue
    const source = stations[r.sources[i]]
    const destination = stations[r.destinations[i]]
    const a: [number, number] = [source.lng, source.lat]
    const b: [number, number] = [destination.lng, destination.lat]
    const roundTrip = r.sources[i] === r.destinations[i]
    const alternatives = lookup.get(JSON.stringify([source.id, destination.id]))
    if (!roundTrip && alternatives?.length) {
      const choice = routeFraction(JSON.stringify([midnight + start, source.id, destination.id, r.durations[i], r.flags[i]]))
      let cumulative = 0
      const route = alternatives.find((route) => { cumulative += route.weight; return choice < cumulative }) ?? alternatives[alternatives.length - 1]
      const geometry = routeGeometry(routes!, route)
      if (geometry.length) {
        const path = [a, ...geometry, b].filter((p, i, points) => !i || p[0] !== points[i - 1][0] || p[1] !== points[i - 1][1])
        paths.push({ path, times: pathTimes(path, start, end), flags: r.flags[i], routed: true })
        continue
      }
    }
    paths.push({
      path: roundTrip ? [a, [a[0] + .001, a[1] + .001], b] : [a, b],
      times: roundTrip ? [start, start + r.durations[i] / 2, end] : [start, end],
      flags: r.flags[i],
    })
  }
  return paths
}

export function rideHead(ride: MovingRide, seconds: number): [number, number] | null {
  if (seconds < ride.times[0] || seconds >= ride.times[ride.times.length - 1]) return null
  let lo = 1, hi = ride.times.length - 1
  while (lo < hi) {
    const mid = (lo + hi) >>> 1
    if (ride.times[mid] <= seconds) lo = mid + 1
    else hi = mid
  }
  const segment = lo - 1
  const f = (seconds - ride.times[segment]) / (ride.times[segment + 1] - ride.times[segment])
  const a = ride.path[segment]
  const b = ride.path[segment + 1]
  return [a[0] + f * (b[0] - a[0]), a[1] + f * (b[1] - a[1])]
}

export function movingRideLayers(paths: MovingRide[], seconds: number, trail: number, colorBy: 'bike' | 'user') {
  const color = (r: MovingRide): [number, number, number, number] => {
    const rgb = rideRgb(r.flags, colorBy)
    const duration = r.times[r.times.length - 1] - r.times[0]
    const opacity = r.flags & 4 ? Math.max(0, 1 - (seconds - r.times[0]) / duration) : 1
    return [...rgb, Math.round(230 * opacity)] as [number, number, number, number]
  }
  const active = paths.flatMap((r) => {
    const position = rideHead(r, seconds)
    return position ? [{ ride: r, position }] : []
  })
  return {
    active: active.length,
    routed: active.filter((r) => r.ride.routed).length,
    layers: [
      new TripsLayer<MovingRide>({
        id: 'moving-ride-trails', data: paths, getPath: (r) => r.path, getTimestamps: (r) => r.times,
        getColor: color, currentTime: seconds, trailLength: trail, fadeTrail: true,
        widthMinPixels: 1.5, opacity: .8, updateTriggers: { getColor: [colorBy, seconds] },
      }),
      new ScatterplotLayer({
        id: 'moving-ride-heads', data: active, getPosition: (r) => r.position, getFillColor: (r) => color(r.ride),
        getRadius: 4, radiusMinPixels: 2, radiusMaxPixels: 5,
      }),
    ],
  }
}
