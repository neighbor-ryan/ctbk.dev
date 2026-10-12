import { useQuery } from '@tanstack/react-query'
import { directedPairs, type FlowArc, type FlowDirection } from '../components/flowLens'
import type { Stations, StationPairCounts } from '../components/stationMapCommon'

type Position = [number, number]
export interface RouteSample {
  version: number
  segments: { path: Position[] }[]
  pairs: { source: string; destination: string; routes: { weight: number; segments: number[]; geometry?: Position[] }[] }[]
}
export interface FlowPath { code: number; path: Position[]; count: number }
const BASE = 'https://data.ctbk.dev/routes-sample/v1/'

export function assignRoutes(
  sample: RouteSample,
  stations: Stations,
  counts: StationPairCounts | null,
  selected: readonly string[],
  direction: FlowDirection,
) {
  const routes = new Map(sample.pairs.map((p) => [JSON.stringify([p.source, p.destination]), p.routes]))
  const pairs = directedPairs(stations, counts, selected, direction)
  const byOther = new Map<string, number>()
  for (const p of pairs) byOther.set(p.other, (byOther.get(p.other) ?? 0) + p.count)
  const cutoff = Math.max(0, ...byOther.values()) * 0.04
  const volumes = new Map<number, number>()
  const arcs: FlowArc[] = []
  let routed = 0, total = 0
  for (const p of pairs) {
    if (byOther.get(p.other)! < cutoff) continue
    total += p.count
    const alternatives = routes.get(JSON.stringify([p.from, p.to]))
    if (!alternatives?.length) {
      const a = stations[p.from], b = stations[p.to]
      arcs.push({ from: p.from, to: p.to, count: p.count, source: [a.lng, a.lat], target: [b.lng, b.lat] })
      continue
    }
    routed += p.count
    for (const route of alternatives) {
      for (const code of route.segments) volumes.set(code, (volumes.get(code) ?? 0) + p.count * route.weight)
    }
  }
  const paths: FlowPath[] = [...volumes].map(([code, count]) => {
    const segment = sample.segments[Math.abs(code) - 1]
    if (!segment || code === 0) throw new Error('Invalid route segment')
    return { code, count, path: code < 0 ? [...segment.path].reverse() : segment.path }
  }).sort((a, b) => a.count - b.count || a.code - b.code)
  const segmentFloor = (paths[paths.length - 1]?.count ?? 0) * 0.04
  return { paths: paths.filter((p) => p.count >= segmentFloor), arcs: arcs.sort((a, b) => a.count - b.count), routed, total }
}

async function get(path: string): Promise<Response> {
  const response = await fetch(BASE + path)
  if (!response.ok) throw new Error(`Route data: HTTP ${response.status}`)
  return response
}

export function useFlowRoutes(enabled: boolean) {
  const index = useQuery({
    queryKey: ['flow-routes-index', BASE], enabled, staleTime: 60000,
    queryFn: () => get('index.json').then((r) => r.json() as Promise<{ version: number; key: string }>),
  })
  const sample = useQuery({
    queryKey: ['flow-routes-sample', BASE, index.data?.key], enabled: enabled && !!index.data, staleTime: Infinity,
    queryFn: async () => {
      if (index.data!.version !== 1) throw new Error('Unsupported route index')
      const buffer = await (await get(index.data!.key)).arrayBuffer()
      const bytes = new Uint8Array(buffer)
      const response = bytes[0] === 0x1f && bytes[1] === 0x8b
        ? new Response(new Blob([buffer]).stream().pipeThrough(new DecompressionStream('gzip')))
        : new Response(buffer)
      const data = await response.json() as RouteSample
      if (data.version !== 1) throw new Error('Unsupported route sample')
      return data
    },
  })
  return { data: sample.data, error: index.error ?? sample.error }
}
