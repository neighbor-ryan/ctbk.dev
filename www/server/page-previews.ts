import puppeteer, { type BrowserWorker } from '@cloudflare/puppeteer'
import { captureView, imageUrl, PREVIEW_HEIGHT, PREVIEW_WIDTH, previewTitle, previewTtl, publicView } from '../src/lib/pagePreview'

export interface PreviewEnv {
  ASSETS: { fetch(request: Request): Promise<Response> }
  BROWSER?: BrowserWorker
  OG_RENDER_LIMIT: { limit(options: { key: string }): Promise<{ success: boolean }> }
}
interface Context { waitUntil(promise: Promise<unknown>): void }
declare const caches: { default: Cache }
declare class HTMLRewriter {
  on(selector: string, handler: { element(element: { setAttribute(name: string, value: string): void; setInnerContent(value: string): void }): void }): this
  transform(response: Response): Response
}

const inFlight = new Map<string, Promise<Response>>()
const PUBLIC_HOSTS = new Set(['ctbk.dev', 'dev.ctbk.dev'])
async function version(env: PreviewEnv, origin: string): Promise<string> {
  const response = await env.ASSETS.fetch(new Request(`${origin}/version.json`))
  return response.ok && response.headers.get('content-type')?.includes('application/json')
    ? (await response.json() as { sha?: string }).sha ?? 'unversioned' : 'unversioned'
}

async function render(view: URL, env: PreviewEnv): Promise<Response> {
  if (!env.BROWSER) return new Response('Page preview renderer unavailable\n', { status: 503 })
  const browser = await puppeteer.launch(env.BROWSER, { guardrails: { allowedDomains: [
    'ctbk.dev', 'dev.ctbk.dev', 'data.ctbk.dev', 'ctbk-gbfs-api.hccs-ctbk.workers.dev', 'ctbk-gbfs-api-dev.hccs-ctbk.workers.dev',
    '*.basemaps.cartocdn.com', 'basemaps.cartocdn.com', 'tiles.stadiamaps.com', '*.tile.openstreetmap.org',
    'ctbk.s3.amazonaws.com',
  ] } })
  try {
    const page = await browser.newPage()
    await page.setViewport({ width: PREVIEW_WIDTH, height: PREVIEW_HEIGHT, deviceScaleFactor: 1 })
    await page.setJavaScriptEnabled(true)
    await page.goto(captureView(view).href, { waitUntil: 'networkidle2', timeout: 25000 })
    const section = view.searchParams.get('section')
    if (section && /^[a-zA-Z0-9_-]{1,64}$/.test(section)) {
      await page.waitForSelector(`[id="${section}"]`, { timeout: 15000 })
      await page.addStyleTag({ content: `html[data-og-capture] body { padding-bottom: ${PREVIEW_HEIGHT}px !important; } html[data-og-capture] :focus { outline: none !important; }` })
      await page.$eval(`[id="${section}"]`, (el) => el.scrollIntoView({ block: 'start' }))
    }
    await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true', { timeout: 25000 })
    await page.waitForNetworkIdle({ idleTime: 500, timeout: 15000 })
    await page.evaluate(async () => { await document.fonts.ready })
    await page.evaluate(() => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))))
    const bytes = await page.screenshot({ type: 'jpeg', quality: 85, captureBeyondViewport: false })
    return new Response(bytes as unknown as BodyInit, { headers: { 'content-type': 'image/jpeg', 'cache-control': `public, max-age=${previewTtl(view)}`, 'x-preview-view': view.pathname + view.search } })
  } finally { await browser.close() }
}

export async function servePublicPage(request: Request, env: PreviewEnv, ctx: Context): Promise<Response> {
  const requested = new URL(request.url)
  if (!['GET', 'HEAD'].includes(request.method)) return env.ASSETS.fetch(request)
  const sha = await version(env, requested.origin)
  if (requested.pathname === '/og.jpg') {
    const relative = requested.searchParams.get('view') ?? '/'
    if (!relative.startsWith('/') || relative.startsWith('//')) return new Response('Invalid public view\n', { status: 400 })
    const view = publicView(new URL(relative, requested.origin))
    if (!view || view.origin !== requested.origin || !PUBLIC_HOSTS.has(view.hostname)) return new Response('Invalid public view\n', { status: 400 })
    const key = imageUrl(view, sha)
    const cacheRequest = new Request(key.href)
    const hit = await caches.default.match(cacheRequest)
    if (hit) {
      // Cache API responses may inherit a larger zone Browser Cache TTL.
      const headers = new Headers(hit.headers)
      headers.set('cache-control', `public, max-age=${previewTtl(view)}`)
      return new Response(request.method === 'HEAD' ? null : hit.body, { status: hit.status, headers })
    }
    let pending = inFlight.get(key.href)
    if (!pending) {
      // Bound concurrent cold screenshots per isolate; cache hits remain cheap.
      if (inFlight.size >= 2) return new Response('Preview renderer busy\n', { status: 503, headers: { 'retry-after': '10' } })
      pending = (async () => {
        const allowance = await env.OG_RENDER_LIMIT.limit({ key: 'public-capture' })
        if (!allowance.success) return new Response('Preview capture rate exceeded\n', { status: 429, headers: { 'retry-after': '60' } })
        return render(view, env)
      })().catch((error: unknown) => {
        console.error('Public page preview failed', { view: view.pathname + view.search, error: String(error) })
        return new Response('Page preview temporarily unavailable\n', { status: 503, headers: { 'retry-after': '30' } })
      }).finally(() => { inFlight.delete(key.href) })
      inFlight.set(key.href, pending)
    }
    const response = (await pending).clone()
    if (response.ok) ctx.waitUntil(caches.default.put(cacheRequest, response.clone()))
    return request.method === 'HEAD' ? new Response(null, response) : response
  }
  const view = publicView(requested)
  const response = await env.ASSETS.fetch(request)
  if (!view || requested.searchParams.has('__og') || !response.headers.get('content-type')?.includes('text/html')) return response
  const title = `${previewTitle(view)} | ctbk.dev`
  const description = 'Current Citi Bike data, with the map, chart, filters and section from this link.'
  const meta = new Map([
    ['og:url', view.href], ['og:image', imageUrl(view, sha).href],
    ['og:image:width', String(PREVIEW_WIDTH)], ['og:image:height', String(PREVIEW_HEIGHT)],
  ])
  const rewrite = new HTMLRewriter()
  // Build-time station stubs already contain the station's name and description.
  if (!view.pathname.startsWith('/s/')) {
    meta.set('og:title', title)
    meta.set('og:description', description)
    rewrite.on('title', { element(el) { el.setInnerContent(title) } })
  }
  for (const [name, content] of meta) rewrite.on(`meta[property="${name}"]`, { element(el) { el.setAttribute('content', content) } })
  const rewritten = rewrite.transform(response)
  const headers = new Headers(rewritten.headers)
  headers.set('cache-control', 'public, max-age=300')
  return new Response(rewritten.body, { status: rewritten.status, headers })
}
