/** Public URL state shared by metadata generation and the capture renderer. */
const PUBLIC_PATH = /^(?:\/(?:v2|stations|timelapse|movie|health(?:\/feed)?|pipeline)?|\/s\/[a-zA-Z0-9+._-]+)$/
const VISUAL_KEYS = new Set(('section theme y s pct r pyramid u g rt d avg cc ar m sel ll gl flow fan dir c side lens mark pies pr t tt tm b w wk st sz src sp len lru lp clr trail clip cfps mw mh mlen anim cap fpb tileBase tr tbin ts ty tpct tu tg trt tavg td tcc rollup rt_pair').split(' '))
export const PREVIEW_WIDTH = 1200
export const PREVIEW_HEIGHT = 630
export const PREVIEW_TTL = 3600
export const previewTtl = (view: URL) => view.pathname.startsWith('/health') ? 300 : PREVIEW_TTL

export function publicView(input: URL): URL | null {
  if (!PUBLIC_PATH.test(input.pathname) || input.search.length > 4096) return null
  const url = new URL(input.pathname, input.origin)
  for (const [key, value] of input.searchParams) {
    if (VISUAL_KEYS.has(key) && value.length <= 1024) url.searchParams.append(key, value)
  }
  // All screenshots use production public data, never arbitrary backend URLs.
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
  if (view.pathname.startsWith('/health')) return 'Citi Bike feed and pipeline health'
  if (view.pathname === '/pipeline') return 'Citi Bike data pipeline'
  return 'Citi Bike station details'
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
