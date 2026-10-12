import { TripsLayer } from '@deck.gl/geo-layers'
import { ScatterplotLayer } from '@deck.gl/layers'
import type { LoadedRideDay } from '../../query/useRideAnimation'

export interface MovingRide {
  path: [number, number][]
  times: number[]
  flags: number
}

/** Geometry is held for a 15-minute clock bucket; currentTime moves on the GPU. */
export function ridePaths(day: LoadedRideDay, bucket: number, trail: number): MovingRide[] {
  const { rides: r, stations } = day
  const paths: MovingRide[] = []
  for (let i = 0; i < r.starts.length; i++) {
    const start = r.starts[i]
    const end = start + r.durations[i]
    if (start > bucket + 900 + trail || end < bucket - trail) continue
    const source = stations[r.sources[i]]
    const destination = stations[r.destinations[i]]
    const a: [number, number] = [source.lng, source.lat]
    const b: [number, number] = [destination.lng, destination.lat]
    const roundTrip = r.sources[i] === r.destinations[i]
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
  const segment = ride.times.findIndex((t, i) => i > 0 && seconds < t) - 1
  const f = (seconds - ride.times[segment]) / (ride.times[segment + 1] - ride.times[segment])
  const a = ride.path[segment]
  const b = ride.path[segment + 1]
  return [a[0] + f * (b[0] - a[0]), a[1] + f * (b[1] - a[1])]
}

export function movingRideLayers(paths: MovingRide[], seconds: number, trail: number, colorBy: 'bike' | 'user') {
  const color = (r: MovingRide): [number, number, number, number] => {
    const rgb = colorBy === 'bike'
      ? r.flags & 8 ? [160, 160, 160] : r.flags & 1 ? [35, 210, 255] : [255, 154, 65]
      : r.flags & 16 ? [160, 160, 160] : r.flags & 2 ? [255, 100, 190] : [100, 220, 110]
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
