import { llzParam, type LLZ } from 'use-prms'
import { selParam } from '../lib/mapSelection'

export const EMBED_DEFAULT_VIEW: LLZ = { lat: 40.758, lng: -73.965, zoom: 12 }
export const embedViewParam = llzParam({ default: EMBED_DEFAULT_VIEW, latLngDecimals: 3 })
const outgoingViewParam = llzParam({ default: { lat: 0, lng: 0, zoom: 0 }, latLngDecimals: 3 })

export function stationsMapHref(view: LLZ, selection: string[], theme: string, month?: string): string {
  const params = new URLSearchParams({ theme })
  const ids = selParam.encode(selection)
  if (ids) params.set('sel', ids)
  params.set('ll', outgoingViewParam.encode(view)!)
  if (month) params.set('m', month.slice(2))
  return `/stations?${params}`
}
