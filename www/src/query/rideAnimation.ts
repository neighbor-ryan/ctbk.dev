/** CTRA v1: columnar per-day rides, with capped midnight carry-in. */
export interface RideDay {
  stationCount: number
  carry: number
  starts: Int32Array
  durations: Uint16Array
  sources: Uint16Array
  destinations: Uint16Array
  flags: Uint8Array
}

export function encodeRideDay(day: RideDay): Uint8Array {
  const n = day.starts.length
  if (!Number.isInteger(day.stationCount) || day.stationCount < 0 || day.stationCount > 65535 || !Number.isInteger(day.carry) || day.carry < 0 || day.carry > n) throw new Error('Invalid CTRA header')
  if ([day.durations, day.sources, day.destinations, day.flags].some((a) => a.length !== n)) throw new Error('Invalid CTRA column lengths')
  const starts: number[] = []
  for (let i = 0; i < n; i++) {
    const at = day.starts[i]
    let value = i === 0 ? at < 0 ? -2 * at - 1 : 2 * at : at - day.starts[i - 1]
    if (value < 0) throw new Error('Invalid CTRA start order')
    do {
      const byte = value % 128
      value = Math.floor(value / 128)
      starts.push(byte + (value > 0 ? 128 : 0))
    } while (value > 0)
  }
  const bytes = new Uint8Array(16 + starts.length + n * 7)
  bytes.set(new TextEncoder().encode('CTRA'))
  bytes[4] = 1
  const view = new DataView(bytes.buffer)
  view.setUint16(6, day.stationCount, true)
  view.setUint32(8, n, true)
  view.setUint32(12, day.carry, true)
  bytes.set(starts, 16)
  const offset = 16 + starts.length
  ;[day.durations, day.sources, day.destinations].forEach((column, c) => column.forEach((v, i) => view.setUint16(offset + c * n * 2 + i * 2, v, true)))
  bytes.set(day.flags, offset + n * 6)
  decodeRideDay(bytes)
  return bytes
}

export function decodeRideDay(bytes: Uint8Array): RideDay {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const fail = (): never => { throw new Error('Invalid CTRA ride file') }
  if (bytes.length < 16 || new TextDecoder().decode(bytes.subarray(0, 4)) !== 'CTRA' || bytes[4] !== 1 || bytes[5] !== 0) fail()
  const stationCount = view.getUint16(6, true)
  const n = view.getUint32(8, true)
  const carry = view.getUint32(12, true)
  if (carry > n || n > (bytes.length - 16) / 8) fail()
  const starts = new Int32Array(n)
  let offset = 16
  for (let i = 0; i < n; i++) {
    let value = 0
    let done = false
    for (let shift = 0; shift < 35; shift += 7) {
      if (offset >= bytes.length) fail()
      const byte = bytes[offset++]
      value += (byte & 127) * 2 ** shift
      if (byte < 128) {
        if (shift > 0 && byte === 0) fail()
        done = true
        break
      }
    }
    if (!done || value > 0xffffffff) fail()
    const start = i === 0 ? Math.floor(value / 2) * (value % 2 ? -1 : 1) - (value % 2) : starts[i - 1] + value
    if (start < -10800 || start >= 86400 || (i > 0 && start < starts[i - 1])) fail()
    starts[i] = start
  }
  if (bytes.length !== offset + n * 7) fail()
  const readColumn = (at: number) => Uint16Array.from({ length: n }, (_, i) => view.getUint16(at + i * 2, true))
  const durations = readColumn(offset)
  const sources = readColumn(offset + n * 2)
  const destinations = readColumn(offset + n * 4)
  const flags = bytes.slice(offset + n * 6)
  let actualCarry = 0
  for (let i = 0; i < n; i++) {
    if (durations[i] < 1 || durations[i] > 10800 || sources[i] >= stationCount || destinations[i] >= stationCount || flags[i] > 31) fail()
    if (starts[i] < 0) {
      actualCarry++
      if (starts[i] + durations[i] <= 0) fail()
    }
    if (i > 0 && starts[i] === starts[i - 1]) {
      const current = [sources[i], destinations[i], durations[i], flags[i]]
      const previous = [sources[i - 1], destinations[i - 1], durations[i - 1], flags[i - 1]]
      const first = current.findIndex((v, j) => v !== previous[j])
      if (first >= 0 && current[first] < previous[first]) fail()
    }
  }
  if (actualCarry !== carry) fail()
  return { stationCount, carry, starts, durations, sources, destinations, flags }
}

export async function unpackRideDay(bytes: Uint8Array): Promise<RideDay> {
  if (bytes[0] !== 0x1f || bytes[1] !== 0x8b) return decodeRideDay(bytes)
  const stream = new Blob([new Uint8Array(bytes)]).stream().pipeThrough(new DecompressionStream('gzip'))
  return decodeRideDay(new Uint8Array(await new Response(stream).arrayBuffer()))
}
