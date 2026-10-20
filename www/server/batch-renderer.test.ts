import { createHmac } from 'node:crypto'
import { describe, expect, it, vi } from 'vitest'
import { batchRenderer } from './batch-renderer'

describe('authenticated HCCS Batch gateway', () => {
  it('binds signatures to method, path, timestamp and exact body, keeping control credentials server-side', async () => {
    const calls: { url: string; init: RequestInit }[] = []
    const fetcher = vi.fn<typeof fetch>(async (url, init) => {
      calls.push({ url: String(url), init: init! })
      return Response.json({ state: 'rendering', frame: 20 })
    })
    const api = batchRenderer('https://gateway.lambda-url.us-east-1.on.aws/', 'test-secret', fetcher)
    expect(await api.status('job-123')).toEqual({ state: 'rendering', frame: 20 })
    const { url, init } = calls[0]
    const { signal, ...options } = init
    expect(signal).toBeInstanceOf(AbortSignal)
    const headers = init.headers as Record<string, string>
    expect({ url, ...options, headers: { ...headers, 'x-movie-timestamp': '<time>', 'x-movie-signature': '<signature>' } }).toEqual({ url: 'https://gateway.lambda-url.us-east-1.on.aws/jobs/job-123', method: 'GET', headers: { 'content-type': 'application/json', 'x-movie-timestamp': '<time>', 'x-movie-signature': '<signature>' } })
    expect(headers['x-movie-signature']).toBe(createHmac('sha256', 'test-secret').update(`${headers['x-movie-timestamp']}\nGET\n/jobs/job-123\n`).digest('hex'))
  })
  it('returns short-lived download redirects and hides backend error bodies', async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValueOnce(Response.json({ url: 'https://private.s3.amazonaws.com/movie.mp4?signature=temporary' })).mockResolvedValueOnce(new Response('private backend details', { status: 503 }))
    const api = batchRenderer('https://gateway.lambda-url.us-east-1.on.aws/', 'test-secret', fetcher)
    const response = await api.file('job', new Request('https://ctbk.dev/__movies/jobs/job/file'))
    expect([response.status, response.headers.get('location')]).toEqual([302, 'https://private.s3.amazonaws.com/movie.mp4?signature=temporary'])
    await expect(api.cancel('job')).rejects.toThrow('Movie backend returned 503.')
  })
})
