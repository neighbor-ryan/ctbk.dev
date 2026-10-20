import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
const capture = vi.hoisted(() => ({ goto: vi.fn(), screenshot: vi.fn(), close: vi.fn(), launch: vi.fn() }))
vi.mock('@cloudflare/puppeteer', () => ({ default: { launch: capture.launch } }))
import { servePublicPage } from '../../server/page-previews'

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date'] })
  vi.setSystemTime(7200000)
  vi.clearAllMocks()
  capture.screenshot.mockResolvedValue(new Uint8Array([255, 216, 255, 217]))
  capture.launch.mockResolvedValue({ close: capture.close, newPage: async () => ({
    goto: capture.goto, screenshot: capture.screenshot,
    setViewport: vi.fn(), setJavaScriptEnabled: vi.fn(), waitForFunction: vi.fn(), waitForNetworkIdle: vi.fn(), evaluate: vi.fn(),
  }) })
})
afterEach(() => { vi.unstubAllGlobals(); vi.useRealTimers() })

function runtime() {
  const stored = new Map<string, Response>()
  vi.stubGlobal('caches', { default: {
    match: async (request: Request) => stored.get(request.url)?.clone(),
    put: async (request: Request, response: Response) => { stored.set(request.url, response) },
  } })
  const assets = { fetch: async () => Response.json({ sha: 'version-a' }) }
  const pending: Promise<unknown>[] = []
  const limit = vi.fn().mockResolvedValue({ success: true })
  return { env: { ASSETS: assets, BROWSER: { fetch }, OG_RENDER_LIMIT: { limit } }, ctx: { waitUntil: (p: Promise<unknown>) => { pending.push(p) } }, stored, pending, limit }
}

describe('public preview Worker', () => {
  test('rejects external URLs and private paths before launching a browser', async () => {
    const { env, ctx } = runtime()
    const results = []
    for (const view of ['https://evil.example/admin', '//evil.example/', '/admin/sessions', '/__movies/jobs/123']) {
      const url = new URL('https://ctbk.dev/og.jpg'); url.searchParams.set('view', view)
      const response = await servePublicPage(new Request(url), env, ctx)
      results.push({ status: response.status, text: await response.text() })
    }
    expect(results).toEqual(Array.from({ length: 4 }, () => ({ status: 400, text: 'Invalid public view\n' })))
    expect(capture.launch).toHaveBeenCalledTimes(0)
  })
  test('captures the real sanitized view once and caches by canonical state and current deploy', async () => {
    const { env, ctx, stored, pending, limit } = runtime()
    const request = new Request('https://ctbk.dev/og.jpg?view=%2F%3Fr%3Djh%26s%3Db%26job%3Dprivate&v=caller-cache-bust', { headers: { cookie: 'private-session=value' } })
    const response = await servePublicPage(request, env, ctx)
    await Promise.all(pending)
    expect({ status: response.status, type: response.headers.get('content-type'), bytes: [...new Uint8Array(await response.arrayBuffer())] }).toEqual({ status: 200, type: 'image/jpeg', bytes: [255, 216, 255, 217] })
    expect(capture.goto.mock.calls.map(([url]) => url)).toEqual(['https://ctbk.dev/?r=jh&s=b&__og=1&screenshot='])
    expect([...stored.keys()]).toEqual(['https://ctbk.dev/og.jpg?view=%2F%3Fr%3Djh%26s%3Db&v=version-a&at=2'])
    const cached = await servePublicPage(request, env, ctx)
    expect([...new Uint8Array(await cached.arrayBuffer())]).toEqual([255, 216, 255, 217])
    expect(capture.launch).toHaveBeenCalledTimes(1)
    expect(capture.close).toHaveBeenCalledTimes(1)
    expect(limit.mock.calls).toEqual([[{ key: 'public-capture' }]])
  })
  test('limits cold capture work without launching a browser', async () => {
    const { env, ctx, limit } = runtime()
    limit.mockResolvedValue({ success: false })
    const response = await servePublicPage(new Request('https://ctbk.dev/og.jpg?view=%2Fstations'), env, ctx)
    expect({ status: response.status, retry: response.headers.get('retry-after'), text: await response.text() }).toEqual({ status: 429, retry: '60', text: 'Preview capture rate exceeded\n' })
    expect(capture.launch).toHaveBeenCalledTimes(0)
  })
})
