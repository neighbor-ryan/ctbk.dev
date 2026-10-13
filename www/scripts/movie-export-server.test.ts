import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createServer, request, type Server } from 'node:http'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { PassThrough } from 'node:stream'
import { ChildProcess } from 'node:child_process'
import { movieExportMiddleware, type Launch } from './movie-export-server'

const query = 'anim=1&b=1m&d=260929-260930&clip=260929T04-260930T04&ll=40.745-73.985+12.15&theme=dark&sp=48&cfps=30&mlen=30&mw=1080&mh=1350'
describe('localhost movie export API', () => {
  let root: string, server: Server, origin: string, close: () => void
  let child: ChildProcess
  let free: number
  const launch = vi.fn<Launch>()
  beforeEach(async () => {
    await mkdir('tmp/movie-server-tests', { recursive: true })
    root = await mkdtemp(resolve('tmp/movie-server-tests/run-'))
    child = Object.assign(new ChildProcess(), { stdout: new PassThrough(), stderr: new PassThrough() })
    free = 4 * 1024 ** 3
    launch.mockReset().mockReturnValue(child)
    const api = movieExportMiddleware({ root, launch, freeBytes: async () => free })
    close = api.close
    server = createServer((req, res) => { void api.middleware(req, res, () => { res.writeHead(404); res.end() }) })
    await new Promise<void>((done) => server.listen(0, '127.0.0.1', done))
    const address = server.address() as { port: number }
    origin = `http://127.0.0.1:${address.port}`
  })
  afterEach(async () => {
    close()
    child.emit('close', null)
    await new Promise<void>((done) => server.close(() => done()))
    await rm(root, { recursive: true })
  })
  const post = (body = { query }, headers: Record<string, string> = {}) => fetch(origin + '/__movies/jobs', { method: 'POST', headers: { 'content-type': 'application/json', origin, ...headers }, body: JSON.stringify(body) })
  it('rejects cross-origin requests, non-local hosts, caller URLs and invalid dimensions', async () => {
    const cross = await post({ query }, { origin: 'https://ctbk.dev' })
    expect([cross.status, await cross.json()]).toEqual([403, { error: 'Movie exports are available only on localhost.' }])
    const remoteHost = await new Promise((done, reject) => {
      const req = request(origin + '/__movies/capabilities', { headers: { host: 'm3:3478' } }, (res) => {
        let body = ''
        res.on('data', (chunk) => { body += chunk })
        res.on('end', () => done([res.statusCode, JSON.parse(body)]))
      })
      req.on('error', reject); req.end()
    })
    expect(remoteHost).toEqual([403, { error: 'Movie exports are available only on localhost.' }])
    const url = await post({ query: query + '&url=http://private-service/' })
    expect([url.status, await url.json()]).toEqual([400, { error: 'Unsupported movie setting: url' }])
    const bad = await post({ query: query.replace('mw=1080', 'mw=1081') })
    expect([bad.status, await bad.json()]).toEqual([400, { error: 'Use even dimensions from 256 to 1920, up to 1920×1080 pixels.' }])
    expect(launch.mock.calls).toEqual([])
  })
  it('snapshots the view, streams progress, serializes jobs and serves a ranged download', async () => {
    const response = await post()
    const first = await response.json()
    expect(response.status).toBe(202)
    expect({ ...first, id: '<id>', createdAt: '<time>' }).toEqual({ id: '<id>', state: 'queued', frame: 0, frames: 900, width: 1080, height: 1350, fps: 30, seconds: 30, query, createdAt: '<time>' })
    await vi.waitFor(() => expect(launch.mock.calls.length).toBe(1))
    expect(launch.mock.calls[0]).toEqual([process.execPath, [resolve(root, 'scripts/render-timelapse.mjs'), '--fps', '30', '--width', '1080', '--height', '1350', origin + '/timelapse?' + query, resolve(root, `tmp/movies/exports/${first.id}.mp4`)], { cwd: root, detached: true, stdio: ['ignore', 'pipe', 'pipe'] }])
    child.stderr!.emit('data', Buffer.from('frame 40/900\n'))
    const progress = await fetch(origin + '/__movies/jobs/' + first.id).then((r) => r.json())
    expect({ ...progress, id: '<id>', createdAt: '<time>' }).toEqual({ ...first, id: '<id>', createdAt: '<time>', state: 'rendering', frame: 40 })
    const second = await post().then((r) => r.json())
    expect(launch.mock.calls.length).toBe(1)
    const cancel = await fetch(origin + '/__movies/jobs/' + second.id, { method: 'DELETE', headers: { origin } })
    expect((await cancel.json()).state).toBe('cancelled')
    await writeFile(resolve(root, `tmp/movies/exports/${first.id}.mp4`), '0123456789')
    child.emit('close', 0)
    await vi.waitFor(async () => expect((await fetch(origin + '/__movies/jobs/' + first.id).then((r) => r.json())).state).toBe('complete'))
    const download = await fetch(origin + `/__movies/jobs/${first.id}/file`, { headers: { range: 'bytes=2-5' } })
    expect([download.status, download.headers.get('content-range'), await download.text()]).toEqual([206, 'bytes 2-5/10', '2345'])
    expect(launch.mock.calls.length).toBe(1)
  })
  it('fails before launching a renderer when disk space is low', async () => {
    free = 1024 ** 3
    const queued = await post().then((r) => r.json())
    await vi.waitFor(async () => {
      const job = await fetch(origin + '/__movies/jobs/' + queued.id).then((r) => r.json())
      expect(job).toEqual({ ...queued, state: 'failed', error: 'Less than 2 GiB free. Free disk space before exporting.' })
    })
    expect(launch.mock.calls).toEqual([])
  })
  it('recovers completed exports from their manifest after a server restart', async () => {
    const id = '00000000-0000-0000-0000-000000000000'
    const dir = resolve(root, 'tmp/movies/exports')
    await mkdir(dir, { recursive: true })
    await writeFile(resolve(dir, id + '.mp4'), 'movie')
    await writeFile(resolve(dir, id + '.mp4.json'), JSON.stringify({ url: origin + '/timelapse?' + query, frames: 900, width: 1080, height: 1350, fps: 30, seconds: 30 }))
    const response = await fetch(origin + '/__movies/jobs/' + id)
    const job = await response.json()
    expect([response.status, { ...job, createdAt: '<time>' }]).toEqual([200, { id, state: 'complete', frame: 900, frames: 900, width: 1080, height: 1350, fps: 30, seconds: 30, query, createdAt: '<time>' }])
    expect(launch.mock.calls).toEqual([])
  })
})
