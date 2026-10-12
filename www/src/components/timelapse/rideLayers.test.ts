import { describe, expect, it } from 'vitest'
import { pathTimes, rideHead, ridePaths, routeFraction } from './rideLayers'

describe('inferred ride paths', () => {
  const day = {
    stations: [{ id: 'A', lng: -74, lat: 40 }, { id: 'B', lng: -73, lat: 41 }],
    rides: {
      stationCount: 2, carry: 1, starts: new Int32Array([-10, 0, 5]),
      durations: new Uint16Array([20, 30, 10800]), sources: new Uint16Array([0, 1, 0]),
      destinations: new Uint16Array([1, 1, 1]), flags: new Uint8Array([0, 1, 5]),
    },
  }
  it('renders carry-in once, with local seconds and a visible round-trip loop', () => {
    const paths = ridePaths(day, 0, 120)
    expect(paths).toEqual([
      { path: [[-74, 40], [-73, 41]], times: [-10, 10], flags: 0 },
      { path: [[-73, 41], [-72.999, 41.001], [-73, 41]], times: [0, 15, 30], flags: 1 },
      { path: [[-74, 40], [-73, 41]], times: [5, 10805], flags: 5 },
    ])
    expect(rideHead(paths[0], 0)).toEqual([-73.5, 40.5])
    expect(rideHead(paths[0], -11)).toEqual(null)
    expect(rideHead(paths[0], 10)).toEqual(null)
    expect(rideHead(paths[1], 15)).toEqual([-72.999, 41.001])
    expect(ridePaths(day, 11700, 120)).toEqual([])
  })
  it('paces routes by distance and keeps inferred alternative choice stable across midnight carry-in', () => {
    expect(pathTimes([[0, 0], [1, 0], [3, 0]], 10, 70)).toEqual([10, 30, 70])
    expect(rideHead({ path: [[0, 0], [1, 0], [3, 0]], times: [10, 30, 70], flags: 0 }, 30)).toEqual([1, 0])
    expect(routeFraction('hello')).toEqual(0x4f9f2cab / 4294967296)
    const routes = { version: 1, segments: [], pairs: [{ source: 'A', destination: 'B', routes: [
      { weight: 1, segments: [], geometry: [[-74, 40], [-74, 41], [-73, 41]] as [number, number][] },
    ] }] }
    const current = ridePaths(day, 0, 120, routes, 86400)[0]
    const previousDay = { ...day, rides: { ...day.rides, starts: new Int32Array([86390, 86400, 86405]) } }
    const previous = ridePaths(previousDay, 85500, 120, routes, 0)[0]
    expect(current.path).toEqual([[-74, 40], [-74, 41], [-73, 41]])
    expect(current.routed).toEqual(true)
    expect(previous.path).toEqual(current.path)
    const milliseconds = (t: number) => Math.round(t * 1000)
    expect(previous.times.map((t) => milliseconds(t - 86400))).toEqual(current.times.map(milliseconds))
  })
})
