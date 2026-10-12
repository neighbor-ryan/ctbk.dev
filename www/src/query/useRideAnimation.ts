import { useEffect } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { unpackRideDay, type RideDay } from './rideAnimation'

const BASE = 'https://data.ctbk.dev/rides-anim-candidate/v1/'
export interface AnimationStation { id: string; lat: number; lng: number }
interface DayMeta { key: string; stations: string; rides: number; carry: number }
interface AnimationIndex { version: number; days: Record<string, DayMeta> }
export interface LoadedRideDay { rides: RideDay; stations: AnimationStation[] }

async function get(path: string): Promise<Response> {
  const response = await fetch(BASE + path)
  if (!response.ok) throw new Error(`Animation data: HTTP ${response.status}`)
  return response
}

export function useRideAnimation(ms: number, enabled: boolean) {
  const qc = useQueryClient()
  const date = new Date(ms).toISOString().slice(0, 10)
  const index = useQuery({
    queryKey: ['ride-animation-index', BASE], enabled,
    queryFn: async () => {
      const data = await (await get('index.json')).json() as AnimationIndex
      if (data.version !== 1) throw new Error('Unsupported animation index')
      return data
    }, staleTime: 60000,
  })
  const meta = index.data?.days[date]
  const options = (m: DayMeta) => ({
    queryKey: ['ride-animation-day', BASE, m.key, m.stations], staleTime: Infinity,
    queryFn: async (): Promise<LoadedRideDay> => {
      const [rides, stations] = await Promise.all([
        get(m.key).then((r) => r.arrayBuffer()).then((b) => unpackRideDay(new Uint8Array(b))),
        qc.fetchQuery({
          queryKey: ['ride-animation-stations', BASE, m.stations], staleTime: Infinity,
          queryFn: () => get(m.stations).then((r) => r.json() as Promise<AnimationStation[]>),
        }),
      ])
      if (stations.length !== rides.stationCount) throw new Error('Animation station table disagrees with rides')
      return { rides, stations }
    },
  })
  const day = useQuery({
    ...options(meta ?? { key: '', stations: '', rides: 0, carry: 0 }), enabled: enabled && !!meta,
  })
  const seconds = (ms % 86400000) / 1000
  useEffect(() => {
    if (!enabled || seconds < 43200) return
    const nextDate = new Date(ms + 86400000).toISOString().slice(0, 10)
    const next = index.data?.days[nextDate]
    if (next) void qc.prefetchQuery(options(next))
    // The immutable day key is the prefetch identity, not fractional time.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [enabled, date, seconds >= 43200, index.data, qc])
  const missing = enabled && !!index.data && !meta
  return { day: day.data, ready: !enabled || missing || !!day.data, missing, error: index.error ?? day.error, seconds, date }
}
