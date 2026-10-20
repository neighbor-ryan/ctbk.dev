import { movieRequest, type MovieCapabilities, type MovieJob } from '../src/query/movieExport'

export type MovieIdentity = { email: string; admin: boolean }
export type StoredMovie = MovieJob & { owner: string; rendererId?: string }
export type MovieLimits = { activePerUser: number; dailyPerUser: number; activeTotal: number }
export const MOVIE_LIMITS: MovieLimits = { activePerUser: 2, dailyPerUser: 5, activeTotal: 20 }

export interface MovieStore {
  /** Atomically checks quotas and inserts, including jobs awaiting renderer submission. */
  reserve(job: StoredMovie, limits: MovieLimits): Promise<boolean>
  get(id: string): Promise<StoredMovie | null>
  active(): Promise<StoredMovie[]>
  update(job: StoredMovie): Promise<void>
}
export interface MovieRenderer {
  submit(job: MovieJob): Promise<string>
  status(rendererId: string): Promise<Pick<MovieJob, 'state' | 'frame' | 'error'>>
  cancel(rendererId: string): Promise<void>
  /** Authenticated backend delivery, or a short-lived signed URL; never a permanent public object. */
  file(rendererId: string, request: Request): Promise<Response>
}
export type MovieAudit = { kind: 'movie.queued' | 'movie.cancelled' | 'movie.failed' | 'movie.download'; email: string; job: string }
type Dependencies = {
  identify(request: Request): Promise<MovieIdentity | null>
  store: MovieStore
  renderer: MovieRenderer
  audit(event: MovieAudit): Promise<void>
  signInUrl: string
  now?: () => number
  id?: () => string
}
const json = (value: unknown, status = 200) => Response.json(value, { status, headers: { 'cache-control': 'no-store' } })
const publicJob = ({ owner: _owner, rendererId: _rendererId, ...job }: StoredMovie): MovieJob => job
const active = (job: Pick<MovieJob, 'state'>) => job.state === 'queued' || job.state === 'rendering'

/** Hosted counterpart of the localhost API. Authentication happens before job lookup. */
export function movieJobs({ identify, store, renderer, audit, signInUrl, now = Date.now, id = () => crypto.randomUUID() }: Dependencies) {
  return async (request: Request): Promise<Response | null> => {
    const url = new URL(request.url)
    if (!url.pathname.startsWith('/__movies/')) return null
    const identity = await identify(request)
    if (url.pathname === '/__movies/capabilities' && request.method === 'GET') {
      return json({ available: !!identity, mode: 'hosted', authenticationRequired: !identity, signInUrl, ...(identity ? { email: identity.email } : {}) } satisfies MovieCapabilities)
    }
    if (!identity) return json({ error: 'Sign in to export movies.', signInUrl }, 401)
    if (!['GET', 'HEAD'].includes(request.method) && (request.headers.get('origin') !== url.origin || request.headers.get('sec-fetch-site') === 'cross-site')) return json({ error: 'Use a same-origin request.' }, 403)
    if (request.method === 'POST' && url.pathname === '/__movies/jobs') {
      if (!request.headers.get('content-type')?.startsWith('application/json')) return json({ error: 'Use a JSON export request.' }, 415)
      if (Number(request.headers.get('content-length') ?? 0) > 8192) return json({ error: 'Export request is too large.' }, 413)
      const reader = request.body?.getReader()
      if (!reader) return json({ error: 'Export needs the preview query.' }, 400)
      const chunks: Uint8Array[] = []
      let bytes = 0
      while (true) {
        const result = await reader.read()
        if (result.done) break
        bytes += result.value.length
        if (bytes > 8192) { await reader.cancel(); return json({ error: 'Export request is too large.' }, 413) }
        chunks.push(result.value)
      }
      const raw = new Uint8Array(bytes)
      let offset = 0
      for (const chunk of chunks) { raw.set(chunk, offset); offset += chunk.length }
      let plan: ReturnType<typeof movieRequest>
      try { plan = movieRequest(JSON.parse(new TextDecoder().decode(raw))) }
      catch (e) { return json({ error: e instanceof SyntaxError ? 'Use a JSON export request.' : (e as Error).message }, 400) }
      const { settings, query } = plan
      // Finished jobs must release quota even when their browser tab was closed.
      for (const pending of await store.active()) {
        if (pending.rendererId) {
          const status = await renderer.status(pending.rendererId)
          if (!active(status)) await store.update({ ...pending, ...status })
        }
      }
      const job: StoredMovie = {
        id: id(), owner: identity.email.toLowerCase(), state: 'queued', frame: 0,
        frames: Math.ceil(settings.seconds * settings.fps), width: settings.width, height: settings.height,
        fps: settings.fps, seconds: settings.seconds, query, createdAt: new Date(now()).toISOString(),
      }
      if (!await store.reserve({ ...job }, MOVIE_LIMITS)) return json({ error: 'Export limit reached. Wait for running exports or try again tomorrow.' }, 429)
      try {
        job.rendererId = await renderer.submit(publicJob(job))
        await store.update(job)
      } catch {
        job.state = 'failed'; job.error = 'The renderer could not start. Please try again.'
        await store.update(job)
        await audit({ kind: 'movie.failed', email: identity.email, job: job.id })
        return json(publicJob(job), 503)
      }
      await audit({ kind: 'movie.queued', email: identity.email, job: job.id })
      return json(publicJob(job), 202)
    }
    const match = /^\/__movies\/jobs\/([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12})(\/file)?$/.exec(url.pathname)
    if (!match) return json({ error: 'Export not found.' }, 404)
    const job = await store.get(match[1])
    // The same response covers absent jobs and another visitor's jobs.
    if (!job || (!identity.admin && job.owner !== identity.email.toLowerCase())) return json({ error: 'Export not found.' }, 404)
    if (!['GET', 'DELETE'].includes(request.method) || (request.method === 'DELETE' && match[2])) return json({ error: 'Method not allowed.' }, 405)
    if (active(job) && job.rendererId) {
      const status = await renderer.status(job.rendererId)
      if (!Number.isInteger(status.frame) || status.frame < 0 || status.frame > job.frames) throw new Error('Renderer returned invalid progress.')
      job.state = status.state; job.frame = status.frame; job.error = status.error
      await store.update(job)
    }
    if (request.method === 'DELETE') {
      if (active(job)) {
        if (job.rendererId) await renderer.cancel(job.rendererId)
        job.state = 'cancelled'
        await store.update(job)
        await audit({ kind: 'movie.cancelled', email: identity.email, job: job.id })
      }
      return json(publicJob(job))
    }
    if (!match[2]) return json(publicJob(job))
    if (job.state !== 'complete' || !job.rendererId) return json({ error: 'Export is not complete.' }, 409)
    await audit({ kind: 'movie.download', email: identity.email, job: job.id })
    const result = await renderer.file(job.rendererId, request)
    const headers = new Headers(result.headers)
    headers.set('cache-control', 'private, no-store')
    headers.set('content-disposition', `${url.searchParams.has('download') ? 'attachment' : 'inline'}; filename="ctbk-${job.id}.mp4"`)
    return new Response(result.body, { status: result.status, headers })
  }
}
