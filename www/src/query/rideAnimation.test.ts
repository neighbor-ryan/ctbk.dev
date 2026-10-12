import { describe, expect, it } from 'vitest'
import { decodeRideDay, encodeRideDay, unpackRideDay } from './rideAnimation'

// Shared literal golden with Python's test_ride_animation.py.
const bytes = Uint8Array.from('43545241010003000300000001000000130a0514001e00302a000001000200010002000000000105'.match(/../g)!, (h) => parseInt(h, 16))
const golden = {
  stationCount: 3, carry: 1,
  starts: new Int32Array([-10, 0, 5]), durations: new Uint16Array([20, 30, 10800]),
  sources: new Uint16Array([0, 1, 2]), destinations: new Uint16Array([1, 2, 0]), flags: new Uint8Array([0, 1, 5]),
}

describe('individual ride decoder', () => {
  it('decodes the Python golden, including signed carry-in', () => {
    expect(decodeRideDay(bytes)).toEqual(golden)
    expect(encodeRideDay(decodeRideDay(bytes))).toEqual(bytes)
  })
  it('reads gzip and already-decompressed HTTP responses', async () => {
    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream('gzip'))
    const compressed = new Uint8Array(await new Response(stream).arrayBuffer())
    expect(await unpackRideDay(compressed)).toEqual(golden)
    expect(await unpackRideDay(bytes)).toEqual(golden)
  })
  it.each([bytes.slice(0, 15), bytes.slice(0, -1), new Uint8Array([...bytes, 0])])('rejects corrupt column lengths', (data) => {
    expect(() => decodeRideDay(data)).toThrow('Invalid CTRA ride file')
  })
})
