/** Public URL state shared by metadata generation and the capture renderer. */
import { pageSectionLabel, pageSections } from './pageSections'
const PUBLIC_PATH = /^(?:\/(?:v2|stations|timelapse|movie|health(?:\/feed)?|pipeline)?|\/(?:s|stations)\/[a-zA-Z0-9+._-]+)$/
const VISUAL_KEYS = new Set(('section theme y s pct r pyramid u g rt d avg cc ar m sel ll mapSel mapLl gl flow network fan dir c side lens mark pies pr t tt tm b w wk st sz src sp len lru lp clr trail clip cfps mw mh mlen anim cap fpb tileBase tr tbin ts ty tpct tu tg trt tavg td tcc rollup rt_pair span wide from to').split(' '))
export const PREVIEW_WIDTH = 1200
export const PREVIEW_HEIGHT = 630
export const PREVIEW_TTL = 3600
export const previewTtl = (view: URL) => view.pathname.startsWith('/health') ? 300 : PREVIEW_TTL

export function publicView(input: URL): URL | null {
  if (!PUBLIC_PATH.test(input.pathname.replace(/%2b/gi, '+')) || input.search.length > 4096) return null
  const url = new URL(input.pathname, input.origin)
  const sections = pageSections(input.pathname)
  for (const [key, value] of input.searchParams) {
    if (key === 'section' && !sections.some(section => section.id === value)) continue
    if (VISUAL_KEYS.has(key) && value.length <= 1024) url.searchParams.append(key, value)
  }
  // Backend selection is a known public enum/flag, never an arbitrary URL.
  const api = input.searchParams.get('api')
  if ((input.pathname === '/' || input.pathname === '/v2') && (api === 'prod' || api === 'dev')) url.searchParams.set('api', api)
  if (input.pathname === '/stations' && api !== null) url.searchParams.set('api', '')
  url.searchParams.sort()
  return url
}

export function previewTitle(view: URL): string {
  if (view.pathname === '/' || view.pathname === '/v2') {
    const section = view.searchParams.get('section')
    return ({ map: 'Citi Bike stations and destinations', states: 'Citi Bike station states', pipeline: 'Citi Bike data pipeline' } as Record<string, string>)[section ?? ''] ?? 'Citi Bike rides'
  }
  if (view.pathname === '/stations') return 'Citi Bike station map'
  if (view.pathname === '/timelapse' || view.pathname === '/movie') return view.searchParams.has('anim') ? 'Citi Bike ride animation' : 'Citi Bike over time'
  const label = pageSectionLabel(view)
  if (view.pathname.startsWith('/health')) return label ? `Citi Bike health · ${label}` : 'Citi Bike feed and pipeline health'
  if (view.pathname === '/pipeline') return label ? `Citi Bike data pipeline · ${label}` : 'Citi Bike data pipeline'
  return label ? `Citi Bike station details · ${label}` : 'Citi Bike station details'
}

export function imageUrl(view: URL, version: string, now = Date.now()): URL {
  const url = new URL('/og.jpg', view.origin)
  url.searchParams.set('view', view.pathname + view.search)
  url.searchParams.set('v', version)
  url.searchParams.set('at', String(Math.floor(now / (previewTtl(view) * 1000))))
  return url
}

export function captureView(view: URL): URL {
  const capture = new URL(view)
  capture.searchParams.set('__og', '1')
  if (capture.pathname === '/' || capture.pathname === '/v2') capture.searchParams.set('screenshot', '')
  return capture
}
