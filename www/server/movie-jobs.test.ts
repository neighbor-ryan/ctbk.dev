import { beforeEach, describe, expect, it, vi } from 'vitest'
import { movieJobs, MOVIE_LIMITS, type MovieAudit, type MovieIdentity, type MovieRenderer, type MovieStore, type StoredMovie } from './movie-jobs'

const query = 'anim=1&b=1m&d=260929-260930&clip=260929T04-260930T04&ll=40.745-73.985+12.15&theme=dark&sp=48&cfps=30&mlen=30&mw=1080&mh=1350'
const job: StoredMovie = { id: '00000000-0000-0000-0000-000000000001', owner: 'viewer@example.test', state: 'queued', frame: 0, frames: 900, width: 1080, height: 1350, fps: 30, seconds: 30, query, createdAt: '2026-10-20T12:00:00.000Z' }
describe('hosted movie jobs', () => {
  let identity: MovieIdentity | null
  let saved: StoredMovie | null
  let store: MovieStore
  let renderer: MovieRenderer
  let audit: ReturnType<typeof vi.fn<(event: MovieAudit) => Promise<void>>>
  let api: ReturnType<typeof movieJobs>
  const request = (path = '/jobs', method = 'POST', body: unknown = { query }, origin = 'https://ctbk.dev') => new Request('https://ctbk.dev/__movies' + path, { method, headers: { 'content-type': 'application/json', origin }, ...(method === 'POST' ? { body: JSON.stringify(body) } : {}) })
  const answer = async (r: Request) => { const response = (await api(r))!; return [response.status, await response.json()] }
  beforeEach(() => {
    identity = { email: 'viewer@example.test', admin: false }; saved = null
    store = { reserve: vi.fn(async (value) => { saved = { ...value }; return true }), get: vi.fn(async () => saved && { ...saved }), active: vi.fn(async () => []), update: vi.fn(async (value) => { saved = { ...value } }) }
    renderer = { submit: vi.fn(async () => 'batch-123'), status: vi.fn(async () => ({ state: 'rendering' as const, frame: 30 })), cancel: vi.fn(async () => {}), file: vi.fn(async () => new Response('movie bytes', { headers: { 'content-type': 'video/mp4' } })) }
    audit = vi.fn(async () => {})
    api = movieJobs({ identify: async () => identity, store, renderer, audit, signInUrl: '/auth/google', now: () => Date.parse(job.createdAt), id: () => job.id })
  })
  it('advertises sign-in without leaking identity or accepting anonymous jobs', async () => {
    identity = null
    expect(await answer(request('/capabilities', 'GET'))).toEqual([200, { available: false, mode: 'hosted', authenticationRequired: true, signInUrl: '/auth/google' }])
    expect(await answer(request())).toEqual([401, { error: 'Sign in to export movies.', signInUrl: '/auth/google' }])
    expect(renderer.submit).not.toHaveBeenCalled()
  })
  it('reserves quota atomically and submits exactly the immutable preview plan', async () => {
    const { owner: _owner, ...publicJob } = job
    expect(await answer(request())).toEqual([202, publicJob])
    expect(store.reserve).toHaveBeenCalledExactlyOnceWith(job, MOVIE_LIMITS)
    expect(renderer.submit).toHaveBeenCalledExactlyOnceWith(publicJob)
    expect(saved).toEqual({ ...job, rendererId: 'batch-123' })
    expect(audit.mock.calls).toEqual([[{ kind: 'movie.queued', email: job.owner, job: job.id }]])
  })
  it('rejects foreign-origin writes, invalid settings, oversized bodies and quota overflow', async () => {
    expect(await answer(request('/jobs', 'POST', { query }, 'https://evil.example'))).toEqual([403, { error: 'Use a same-origin request.' }])
    expect(await answer(request('/jobs', 'POST', { query: query + '&url=http://internal/' }))).toEqual([400, { error: 'Unsupported movie setting: url' }])
    expect(await answer(request('/jobs', 'POST', { query: query + '&mw=1080' }))).toEqual([400, { error: 'Duplicate movie setting: mw' }])
    expect(await answer(request('/jobs', 'POST', { query: 'x'.repeat(8193) }))).toEqual([413, { error: 'Export request is too large.' }])
    vi.mocked(store.reserve).mockResolvedValue(false)
    expect(await answer(request())).toEqual([429, { error: 'Export limit reached. Wait for running exports or try again tomorrow.' }])
    expect(renderer.submit).not.toHaveBeenCalled()
  })
  it('hides another user’s job, progress and file, including from signed-in visitors', async () => {
    saved = { ...job, owner: 'someone-else@example.test', rendererId: 'private-batch' }
    expect(await answer(request('/jobs/' + job.id, 'GET'))).toEqual([404, { error: 'Export not found.' }])
    expect(await answer(request('/jobs/' + job.id + '/file', 'GET'))).toEqual([404, { error: 'Export not found.' }])
    expect(renderer.status).not.toHaveBeenCalled()
    expect(renderer.file).not.toHaveBeenCalled()
  })
  it('updates progress, allows cancellation and serves a private download after completion', async () => {
    saved = { ...job, rendererId: 'batch-123' }
    const { owner: _owner, ...publicJob } = job
    expect(await answer(request('/jobs/' + job.id, 'GET'))).toEqual([200, { ...publicJob, state: 'rendering', frame: 30 }])
    expect(await answer(request('/jobs/' + job.id, 'DELETE'))).toEqual([200, { ...publicJob, state: 'cancelled', frame: 30 }])
    expect(renderer.cancel).toHaveBeenCalledExactlyOnceWith('batch-123')
    saved = { ...job, rendererId: 'batch-123' }
    vi.mocked(renderer.status).mockResolvedValue({ state: 'complete', frame: 900 })
    const response = (await api(request('/jobs/' + job.id + '/file?download=1', 'GET')))!
    expect([response.status, response.headers.get('cache-control'), response.headers.get('content-disposition'), await response.text()]).toEqual([200, 'private, no-store', `attachment; filename="ctbk-${job.id}.mp4"`, 'movie bytes'])
    expect(audit.mock.calls).toEqual([[{ kind: 'movie.cancelled', email: job.owner, job: job.id }], [{ kind: 'movie.download', email: job.owner, job: job.id }]])
  })
  it('records a failed submission without leaking backend error details', async () => {
    vi.mocked(renderer.submit).mockRejectedValue(new Error('backend credentials must stay private'))
    const { owner: _owner, ...publicJob } = job
    expect(await answer(request())).toEqual([503, { ...publicJob, state: 'failed', error: 'The renderer could not start. Please try again.' }])
    expect(saved).toEqual({ ...job, state: 'failed', error: 'The renderer could not start. Please try again.' })
    expect(audit.mock.calls).toEqual([[{ kind: 'movie.failed', email: job.owner, job: job.id }]])
  })
  it('releases completed render quota even when the original browser stopped polling', async () => {
    const old = { ...job, id: '00000000-0000-0000-0000-000000000002', rendererId: 'old-batch' }
    vi.mocked(store.active).mockResolvedValue([old])
    vi.mocked(renderer.status).mockResolvedValue({ state: 'complete', frame: 900 })
    expect((await answer(request()))[0]).toBe(202)
    expect(vi.mocked(store.update).mock.calls).toEqual([[{ ...old, state: 'complete', frame: 900 }], [{ ...job, rendererId: 'batch-123' }]])
  })
})
