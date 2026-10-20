#!/usr/bin/env node
// Secrets arrive on stdin, never argv or logs. Tests backend auth/render/download;
// this deliberately does not claim to test a real OAuth/email-provider sign-in.
import { signSession } from '@open-athena/auth'
import { mkdir, writeFile } from 'node:fs/promises'
import { createWriteStream } from 'node:fs'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { spawnSync } from 'node:child_process'
import { resolve } from 'node:path'

let input = ''
for await (const chunk of process.stdin) input += chunk
const { origin, secret, output = 'tmp/movies/hosted-smoke.mp4' } = JSON.parse(input)
if (!origin || !secret) throw new Error('Expected {origin, secret, output?} on stdin.')
const proof = { origin, testedAt: new Date().toISOString(), authentication: 'synthetic signed test principal; provider sign-in not exercised' }
const cookie = `ctbk_session=${await signSession('e:movie-smoke@example.test', secret, Date.now(), 3600)}`
const other = `ctbk_session=${await signSession('e:movie-other-smoke@example.test', secret, Date.now(), 3600)}`
const call = async (path, { identity = cookie, ...options } = {}) => fetch(origin + path, { ...options, headers: { ...(identity ? { cookie: identity } : {}), origin, ...options.headers }, redirect: 'manual' })
const capability = await call('/__movies/capabilities', { identity: null }).then((r) => r.json())
if (capability.available || !capability.authenticationRequired) throw new Error('Anonymous export should require sign-in.')
const query = new URLSearchParams({ anim: '1', b: '1m', clip: '260929T04-260930T04', mlen: '1', cfps: '30', mw: '480', mh: '600', ll: '40.745-73.985 12.15', theme: 'dark' }).toString()
const response = await call('/__movies/jobs', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query }) })
let job = await response.json()
if (response.status !== 202) throw new Error(`Could not submit smoke render: ${response.status} ${JSON.stringify(job)}`)
proof.job = job.id
proof.query = job.query
console.log(`Submitted ${job.id}`)
const denied = await call('/__movies/jobs/' + job.id, { identity: other })
if (denied.status !== 404) throw new Error('Another principal must not see the job.')
proof.ownerIsolation = denied.status
let delay = 5000
const deadline = Date.now() + 45 * 60_000
while (job.state !== 'complete') {
  if (['failed', 'cancelled'].includes(job.state)) throw new Error(`Render ${job.state}: ${job.error ?? ''}`)
  if (Date.now() > deadline) throw new Error('Smoke render exceeded 45 minutes.')
  await new Promise((done) => setTimeout(done, delay))
  delay = Math.min(60_000, delay * 2)
  const status = await call('/__movies/jobs/' + job.id)
  if (!status.ok) throw new Error(`Status request failed: ${status.status}`)
  job = await status.json()
  console.log(`${job.state}: ${job.frame}/${job.frames}`)
}
const download = await call(`/__movies/jobs/${job.id}/file?download=1`)
if (download.status !== 302) throw new Error(`Download should be a private signed redirect (${download.status}).`)
const file = await fetch(download.headers.get('location'))
if (!file.ok || !file.body) throw new Error(`Signed file download failed: ${file.status}`)
const target = resolve(output)
await mkdir(resolve(target, '..'), { recursive: true })
await pipeline(Readable.fromWeb(file.body), createWriteStream(target))
const probe = spawnSync('ffprobe', ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', target], { encoding: 'utf8' })
if (probe.status !== 0) throw new Error('ffprobe could not verify the rendered MP4.')
const metadata = JSON.parse(probe.stdout)
const video = metadata.streams.find((stream) => stream.codec_type === 'video')
proof.video = { width: video.width, height: video.height, fps: video.avg_frame_rate, duration: Number(metadata.format.duration), bytes: Number(metadata.format.size) }
if (video.width !== 480 || video.height !== 600 || video.avg_frame_rate !== '30/1' || Math.abs(proof.video.duration - 1) > 0.05) throw new Error('Rendered MP4 differs from the requested preview plan.')
await writeFile(target + '.proof.json', JSON.stringify(proof, null, 2) + '\n')
console.log(`Verified ${target}: ${JSON.stringify(proof.video)}`)
