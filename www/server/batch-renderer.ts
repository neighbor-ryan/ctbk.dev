import type { MovieRenderer } from './movie-jobs'
import { MovieRendererMissing, MovieSubmissionRejected } from './movie-jobs'

/** Lambda URL is configuration, never accepted from the browser. */
export function batchRenderer(endpoint: string, secret: string, fetcher = fetch): MovieRenderer {
  const base = new URL(endpoint)
  if (base.protocol !== 'https:') throw new Error('Movie gateway must use HTTPS.')
  const call = async <T>(path: string, method = 'GET', value?: unknown): Promise<T> => {
    const body = value === undefined ? '' : JSON.stringify(value)
    const stamp = String(Math.floor(Date.now() / 1000))
    const url = new URL(path, base)
    const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'])
    const signed = await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(`${stamp}\n${method}\n${url.pathname}\n${body}`))
    const signature = [...new Uint8Array(signed)].map((n) => n.toString(16).padStart(2, '0')).join('')
    const response = await fetcher(url, { method, signal: AbortSignal.timeout(15_000), headers: { 'content-type': 'application/json', 'x-movie-timestamp': stamp, 'x-movie-signature': signature }, ...(body ? { body } : {}) })
    if (response.status === 404) throw new MovieRendererMissing('Movie renderer has no submission record.')
    if (!response.ok) {
      const message = `Movie backend returned ${response.status}.`
      if (method === 'POST' && response.status >= 400 && response.status < 500) throw new MovieSubmissionRejected(message)
      throw new Error(message)
    }
    return response.json() as Promise<T>
  }
  return {
    submit: async (job) => (await call<{ id: string }>('/jobs', 'POST', job)).id,
    status: (id) => call('/jobs/' + encodeURIComponent(id)),
    cancel: (id) => call('/jobs/' + encodeURIComponent(id), 'DELETE'),
    file: async (id) => {
      const { url } = await call<{ url: string }>('/jobs/' + encodeURIComponent(id) + '/file')
      const target = new URL(url)
      if (target.protocol !== 'https:') throw new Error('Invalid movie download URL.')
      return new Response(null, { status: 302, headers: { location: url } })
    },
  }
}
