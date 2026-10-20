import { useQueries, useQuery } from '@tanstack/react-query'
import { useMemo } from 'react'
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
const NJ_BASE = 'https://data.ctbk.dev/routes-nj/v1/'
const CITY_BASE = 'https://data.ctbk.dev/routes-city/v1/'
interface ObjectMeta { key: string; bytes: number }
interface NjIndex { version: number; graph: ObjectMeta; origins: Record<string, ObjectMeta>; destinations: Record<string, ObjectMeta> }

/** Separate graph namespaces; NJ records override sample pairs, including explicit fallbacks. */
export function mergeRoutes(sample: RouteSample, graph: RouteSample['segments'], catalogs: Pick<RouteSample, 'pairs'>[]): RouteSample {
  const offset = sample.segments.length
  const pairs = new Map(sample.pairs.map((p) => [JSON.stringify([p.source, p.destination]), p]))
  for (const catalog of catalogs) for (const pair of catalog.pairs) {
    pairs.set(JSON.stringify([pair.source, pair.destination]), {
      ...pair, routes: pair.routes.map((r) => ({ ...r, segments: r.segments.map((code) => Math.sign(code) * (Math.abs(code) + offset)) })),
    })
  }
  return { version: 1, segments: [...sample.segments, ...graph], pairs: [...pairs.values()] }
}

export function routeGeometry(sample: RouteSample, route: RouteSample['pairs'][number]['routes'][number]): Position[] {
  if (route.geometry) return route.geometry
  const path: Position[] = []
  for (const code of route.segments) {
    const segment = sample.segments[Math.abs(code) - 1]
    if (!segment || !code) throw new Error('Invalid route segment')
    const points = code < 0 ? [...segment.path].reverse() : segment.path
    for (const p of points) if (!path.length || p[0] !== path[path.length - 1][0] || p[1] !== path[path.length - 1][1]) path.push(p)
  }
  return path
}

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

async function get(path: string, base = BASE): Promise<Response> {
  const response = await fetch(base + path)
  if (!response.ok) throw new Error(`Route data: HTTP ${response.status}`)
  return response
}

async function packed<T>(base: string, key: string): Promise<T> {
  const buffer = await (await get(key, base)).arrayBuffer()
  const bytes = new Uint8Array(buffer)
  const response = bytes[0] === 0x1f && bytes[1] === 0x8b
    ? new Response(new Blob([buffer]).stream().pipeThrough(new DecompressionStream('gzip')))
    : new Response(buffer)
  return response.json() as Promise<T>
}

function useRouteCatalog(base: string, enabled: boolean, selected: readonly string[] | undefined, direction: FlowDirection, optional = false) {
  const index = useQuery({
    queryKey: ['flow-routes-index', base], enabled, staleTime: 60000,
    queryFn: async () => {
      const response = await fetch(base + 'index.json')
      if (optional && response.status === 404) return null
      if (!response.ok) throw new Error(`Route catalog: HTTP ${response.status}`)
      const data = await response.json() as NjIndex
      if (data.version !== 1) throw new Error('Unsupported route catalog')
      return data
    },
  })
  const keys = enabled ? [...new Set((selected ?? Object.keys(index.data?.origins ?? {}))
    .map((id) => (direction === 'in' ? index.data?.destinations : index.data?.origins)?.[id]?.key)
    .filter((key): key is string => !!key))].sort() : []
  const graph = useQuery({
    queryKey: ['flow-routes-graph', base, index.data?.graph.key], enabled: enabled && !!keys.length, staleTime: Infinity,
    queryFn: () => packed<Pick<RouteSample, 'segments'>>(base, index.data!.graph.key),
  })
  const catalogs = useQueries({ queries: keys.map((key) => ({
    queryKey: ['flow-routes-pairs', base, key], enabled, staleTime: Infinity,
    queryFn: () => packed<Pick<RouteSample, 'pairs'>>(base, key),
  })) })
  const error = index.error ?? graph.error ?? catalogs.find((q) => q.error)?.error
  const complete = !enabled || !!error || index.isSuccess && (!index.data || !keys.length || !!graph.data && catalogs.every((q) => !!q.data))
  return { index: enabled ? index.data : undefined, graph: enabled ? graph.data : undefined, catalogs, keys, error: enabled ? error : null, complete }
}

export function useFlowRoutes(enabled: boolean, selected?: readonly string[], direction: FlowDirection = 'out', { city: includeCity = true }: { city?: boolean } = {}) {
  const index = useQuery({
    queryKey: ['flow-routes-index', BASE], enabled, staleTime: 60000,
    queryFn: () => get('index.json').then((r) => r.json() as Promise<{ version: number; key: string }>),
  })
  const sample = useQuery({
    queryKey: ['flow-routes-sample', BASE, index.data?.key], enabled: enabled && !!index.data, staleTime: Infinity,
    queryFn: async () => {
      if (index.data!.version !== 1) throw new Error('Unsupported route index')
      const data = await packed<RouteSample>(BASE, index.data!.key)
      if (data.version !== 1) throw new Error('Unsupported route sample')
      return data
    },
  })
  const city = useRouteCatalog(CITY_BASE, enabled && includeCity, selected, direction, true)
  const nj = useRouteCatalog(NJ_BASE, enabled, selected, direction)
  const error = index.error ?? sample.error ?? city.error ?? nj.error
  const complete = !!sample.data && city.complete && nj.complete
  const data = useMemo(() => {
    if (!sample.data || !complete && !error) return undefined
    let merged = sample.data
    for (const catalog of [city, nj]) if (catalog.graph && catalog.catalogs.every((q) => !!q.data)) {
      merged = mergeRoutes(merged, catalog.graph.segments, catalog.catalogs.map((q) => q.data!))
    }
    return merged
    // Immutable query objects identify the merged payload; selection changes keys.
  }, [sample.data, nj.graph, city.graph, complete, error, [...city.catalogs, ...nj.catalogs].map((q) => q.dataUpdatedAt).join(','), [...city.keys, ...nj.keys].join(',')])
  return { data, error, nj: !!nj.index && !nj.error, city: !!city.index && !city.error }
}

export interface SystemFlow {
  version: number
  month: string
  context: string
  graph: ObjectMeta
  routesPrefix: string
  volumes: [number, number][]
  encodedRides: number
  routedRides: number
  fallbackRides: number
  roundTrips: number
  provisional: boolean
}

export function systemFlowPaths(graph: Pick<RouteSample, 'segments'>, volumes: [number, number][], floor = .04): FlowPath[] {
  const maximum = volumes.reduce((maximum, [, count]) => Math.max(maximum, count), 0)
  return volumes.map(([code, count]) => {
    const segment = graph.segments[Math.abs(code) - 1]
    if (!Number.isSafeInteger(code) || !code || !segment || !Number.isFinite(count) || count <= 0) throw new Error('Invalid system street volume')
    return { code, count, path: code > 0 ? segment.path : [...segment.path].reverse() }
  }).filter((p) => p.count >= maximum * floor).sort((a, b) => a.count - b.count || a.code - b.code)
}

export function useSystemFlows(enabled: boolean, month: string | undefined) {
  const base = 'https://data.ctbk.dev/flows/v1/'
  const index = useQuery({
    queryKey: ['system-flow-index', base], enabled, staleTime: 60000,
    queryFn: async () => {
      const response = await fetch(base + 'index.json')
      if (response.status === 404) return null
      if (!response.ok) throw new Error(`System flows: HTTP ${response.status}`)
      const data = await response.json() as { version: number; months: Record<string, ObjectMeta> }
      if (data.version !== 1) throw new Error('Unsupported system-flow index')
      return data
    },
  })
  const meta = month ? index.data?.months[month] : undefined
  const data = useQuery({
    queryKey: ['system-flow-month', base, meta?.key], enabled: enabled && !!meta, staleTime: Infinity,
    queryFn: async () => {
      const data = await packed<SystemFlow>(base, meta!.key)
      if (data.version !== 1 || data.month !== month || !/^routes-(city|nj)\/v1$/.test(data.routesPrefix)) throw new Error('Unsupported system-flow month')
      return data
    },
  })
  const graph = useQuery({
    queryKey: ['system-flow-graph', data.data?.routesPrefix, data.data?.graph.key], enabled: enabled && !!data.data, staleTime: Infinity,
    queryFn: () => packed<Pick<RouteSample, 'segments'>>(`https://data.ctbk.dev/${data.data!.routesPrefix}/`, data.data!.graph.key),
  })
  const paths = useMemo(() => data.data && graph.data ? systemFlowPaths(graph.data, data.data.volumes) : undefined, [data.data, graph.data])
  return { data: data.data, paths, missing: index.isSuccess && !meta, error: index.error ?? data.error ?? graph.error }
}
