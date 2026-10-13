import { spawn, type ChildProcess, type SpawnOptions } from 'node:child_process'
import { createReadStream, createWriteStream } from 'node:fs'
import { mkdir, readFile, stat, statfs } from 'node:fs/promises'
import { randomUUID } from 'node:crypto'
import { resolve } from 'node:path'
import type { IncomingMessage, ServerResponse } from 'node:http'
import type { Plugin } from 'vite'
import { studioDefaults, studioQuery } from '../src/query/movieStudio'
import { parseT } from '../src/query/timelapseFrames'

type Job = {
  id: string; state: 'queued' | 'rendering' | 'complete' | 'failed' | 'cancelled'
  frame: number; frames: number; width: number; height: number; fps: number; seconds: number
  query: string; createdAt: string; error?: string
}
export type Launch = (command: string, args: string[], options: SpawnOptions) => ChildProcess
type Options = { root: string; launch?: Launch; freeBytes?: () => Promise<number> }
const loopback = (ip: string | undefined) => ip === '::1' || ip === '127.0.0.1' || ip === '::ffff:127.0.0.1'
const allowedParams = new Set(['anim', 'b', 'd', 'clip', 'sp', 'cfps', 'mlen', 'mw', 'mh', 'll', 'theme', 'flow', 'clr', 'trail', 'w', 'wk', 'st', 'sz', 'src', 'cap'])

/** Local dev only: same-origin browser requests, bounded serial jobs, no caller URLs/paths. */
export function movieExportMiddleware({ root, launch = spawn, freeBytes }: Options) {
  const output = resolve(root, 'tmp/movies/exports')
  const jobs = new Map<string, Job>()
  const queue: { job: Job; origin: string }[] = []
  let active: { job: Job; child: ChildProcess } | undefined
  let starting = false
  let closed = false
  const json = (res: ServerResponse, status: number, value: unknown) => {
    res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' })
    res.end(JSON.stringify(value))
  }
  const stop = (child: ChildProcess) => {
    if (!child.pid) return
    try { process.kill(-child.pid, 'SIGTERM') }
    catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ESRCH') throw e }
  }
  const runNext = async () => {
    if (closed || active || starting || !queue.length) return
    starting = true
    const { job, origin } = queue.shift()!
    try {
      await mkdir(output, { recursive: true })
      const free = freeBytes ? await freeBytes() : await statfs(output).then((s) => s.bavail * s.bsize)
      if (free < 2 * 1024 ** 3) throw new Error('Less than 2 GiB free. Free disk space before exporting.')
      if (closed || job.state === 'cancelled') return
      job.state = 'rendering'
      const path = resolve(output, job.id + '.mp4')
      const child = launch(process.execPath, [resolve(root, 'scripts/render-timelapse.mjs'), '--fps', String(job.fps), '--width', String(job.width), '--height', String(job.height), origin + '/timelapse?' + job.query, path], { cwd: root, detached: true, stdio: ['ignore', 'pipe', 'pipe'] })
      active = { job, child }
      const log = createWriteStream(resolve(output, job.id + '.log'))
      let tail = ''
      const read = (data: Buffer) => {
        log.write(data)
        tail = (tail + data.toString()).slice(-4000)
        const matches = [...tail.matchAll(/frame (\d+)\/(\d+)/g)]
        if (matches.length) { job.frame = Number(matches[matches.length - 1][1]); job.frames = Number(matches[matches.length - 1][2]) }
      }
      child.stdout!.on('data', read)
      child.stderr!.on('data', read)
      child.on('error', (e) => { job.error = e.message })
      child.on('close', (code) => {
        log.end()
        if (job.state !== 'cancelled') {
          job.state = code === 0 && !job.error ? 'complete' : 'failed'
          if (job.state === 'complete') job.frame = job.frames
          else job.error ??= tail.slice(-2000).trim() || `Renderer exited with code ${code}`
        }
        active = undefined
        void runNext()
      })
    } catch (e) {
      job.state = 'failed'
      job.error = e instanceof Error ? e.message : String(e)
    } finally {
      starting = false
      if (!active) void runNext()
    }
  }
  const middleware = async (req: IncomingMessage, res: ServerResponse, next: () => void) => {
    if (!req.url?.startsWith('/__movies/')) return next()
    try {
      const host = req.headers.host ?? ''
      const origin = `http://${host}`
      const url = new URL(req.url, origin)
      const hostname = url.hostname
      if (!loopback(req.socket.remoteAddress) || !['localhost', '127.0.0.1', '[::1]'].includes(hostname) || Number(url.port) !== req.socket.localPort || req.headers['sec-fetch-site'] === 'cross-site' || (req.headers.origin && req.headers.origin !== origin)) return json(res, 403, { error: 'Movie exports are available only on localhost.' })
      if (req.method === 'GET' && url.pathname === '/__movies/capabilities') return json(res, 200, { available: true })
      if (req.method === 'POST' && url.pathname === '/__movies/jobs') {
        if (req.headers.origin !== origin || !req.headers['content-type']?.startsWith('application/json')) return json(res, 403, { error: 'Use a same-origin JSON request.' })
        if (queue.length + Number(!!active || starting) >= 4) return json(res, 429, { error: 'Four exports are already pending. Wait or cancel one.' })
        let body = ''
        for await (const chunk of req) {
          body += chunk
          if (Buffer.byteLength(body) > 8192) return json(res, 413, { error: 'Export request is too large.' })
        }
        const input = JSON.parse(body)
        if (typeof input.query !== 'string') return json(res, 400, { error: 'Export needs the preview query.' })
        const params = new URLSearchParams(input.query)
        for (const key of params.keys()) if (!allowedParams.has(key)) return json(res, 400, { error: `Unsupported movie setting: ${key}` })
        if (!params.has('clip') || !params.has('mlen') || !params.has('mw') || !params.has('mh') || !params.has('cfps')) return json(res, 400, { error: 'Export needs an explicit clip, length, FPS, width and height.' })
        const clip = params.get('clip')!.split('-').map(parseT)
        if (clip.length !== 2 || clip.some((value) => value === null)) return json(res, 400, { error: 'Export needs a valid clip.' })
        const settings = { ...studioDefaults(params), start: clip[0]!, end: clip[1]!, seconds: Number(params.get('mlen')), fps: Number(params.get('cfps')), width: Number(params.get('mw')), height: Number(params.get('mh')) }
        const canonical = studioQuery(params, settings)
        const job: Job = { id: randomUUID(), state: 'queued', frame: 0, frames: Math.ceil(settings.seconds * settings.fps), width: settings.width, height: settings.height, fps: settings.fps, seconds: settings.seconds, query: canonical.toString(), createdAt: new Date().toISOString() }
        jobs.set(job.id, job)
        queue.push({ job, origin })
        void runNext()
        return json(res, 202, job)
      }
      const match = /^\/__movies\/jobs\/([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12})(\/file)?$/.exec(url.pathname)
      let job = match ? jobs.get(match[1]) : undefined
      if (!job && match && req.method === 'GET') {
        try {
          const meta = JSON.parse(await readFile(resolve(output, match[1] + '.mp4.json'), 'utf8'))
          job = { id: match[1], state: 'complete', frame: meta.frames, frames: meta.frames, width: meta.width, height: meta.height, fps: meta.fps, seconds: meta.seconds, query: new URL(meta.url).search.slice(1), createdAt: (await stat(resolve(output, match[1] + '.mp4'))).birthtime.toISOString() }
          jobs.set(job.id, job)
        } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e }
      }
      if (!job) return json(res, 404, { error: 'Export not found.' })
      if (req.method === 'GET' && match![2]) {
        if (job.state !== 'complete') return json(res, 409, { error: 'Export is not complete.' })
        const path = resolve(output, job.id + '.mp4'), { size } = await stat(path)
        const range = req.headers.range ? /^bytes=(\d+)-(\d*)$/.exec(req.headers.range) : null
        const start = range ? Number(range[1]) : 0, end = range && range[2] ? Math.min(Number(range[2]), size - 1) : size - 1
        if ((req.headers.range && !range) || start >= size || end < start) {
          res.writeHead(416, { 'content-range': `bytes */${size}` }); return res.end()
        }
        res.writeHead(range ? 206 : 200, { 'content-type': 'video/mp4', 'accept-ranges': 'bytes', 'content-length': end - start + 1, ...(range ? { 'content-range': `bytes ${start}-${end}/${size}` } : {}), 'content-disposition': `${url.searchParams.has('download') ? 'attachment' : 'inline'}; filename="ctbk-${job.id}.mp4"`, 'cache-control': 'no-store' })
        createReadStream(path, { start, end }).pipe(res)
        return
      }
      if (req.method === 'GET') return json(res, 200, job)
      if (req.method === 'DELETE' && !match![2]) {
        if (req.headers.origin !== origin) return json(res, 403, { error: 'Use a same-origin request.' })
        if (job.state === 'queued' || job.state === 'rendering') {
          job.state = 'cancelled'
          const at = queue.findIndex((entry) => entry.job === job)
          if (at >= 0) queue.splice(at, 1)
          if (active?.job === job) stop(active.child)
        }
        return json(res, 200, job)
      }
      return json(res, 405, { error: 'Method not allowed.' })
    } catch (e) {
      json(res, 400, { error: e instanceof Error ? e.message : String(e) })
    }
  }
  return { middleware, close: () => {
    closed = true
    for (const job of jobs.values()) if (job.state === 'queued' || job.state === 'rendering') job.state = 'cancelled'
    if (active) stop(active.child)
    queue.length = 0
  } }
}

export function movieExports(): Plugin {
  return {
    name: 'local-movie-exports', apply: 'serve',
    configureServer(server) {
      const exports = movieExportMiddleware({ root: server.config.root })
      server.middlewares.use((req, res, next) => { void exports.middleware(req, res, next) })
      server.httpServer?.on('close', exports.close)
    },
  }
}
