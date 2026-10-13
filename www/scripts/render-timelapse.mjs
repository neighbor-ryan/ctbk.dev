#!/usr/bin/env node
import { parseArgs } from 'node:util'
import { resolve, dirname, basename } from 'node:path'
import { mkdir, writeFile } from 'node:fs/promises'
import { chromium } from '@playwright/test'
import { resolveEngine, takeScreenshots } from 'scrns'

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    fps: { type: 'string', default: '30' },
    width: { type: 'string', default: '1080' },
    height: { type: 'string', default: '1080' },
  },
})
if (positionals.length !== 2) throw new Error('Usage: render-timelapse.mjs [--fps 30] [--width 1080] [--height 1080] URL OUTPUT.mp4')
const [input, output] = positionals
const url = new URL(input)
if (url.pathname !== '/timelapse') throw new Error('Use a /timelapse URL')
const positiveInt = (value, name) => {
  const n = Number(value)
  if (!Number.isInteger(n) || n < 1) throw new Error(`${name} must be a positive integer`)
  return n
}
const fps = positiveInt(values.fps, 'fps')
const width = positiveInt(values.width, 'width')
const height = positiveInt(values.height, 'height')
if (!output.endsWith('.mp4')) throw new Error('Output must end in .mp4')
url.searchParams.set('mv', '1')
url.searchParams.set('cfps', String(fps))
const browser = await chromium.launch({ args: ['--enable-webgl', '--use-gl=angle', '--use-angle=swiftshader'] })
let plan
try {
  const page = await browser.newPage({ viewport: { width, height } })
  await page.goto(url.href)
  await page.locator('[data-tl-frame]').waitFor({ timeout: 120000 })
  plan = await page.evaluate(() => {
    const { frames, fps, seconds, start, end } = window.__tl
    return { frames, fps, seconds, start, end }
  })
  url.href = page.url()
} finally {
  await browser.close()
}
const path = resolve(output)
await mkdir(dirname(path), { recursive: true })
console.error(`Capturing ${plan.frames} frames at ${plan.fps} fps (${plan.seconds}s)`)
await takeScreenshots({ movie: {
  query: url.pathname.slice(1) + url.search, width, height, path: basename(path), fps: plan.fps,
  selector: '[data-tl-frame]', loadTimeout: 120000,
  actions: [{ type: 'animate', frames: plan.frames, eval: '(i) => window.__tl.seek(i)' }],
} }, {
  baseUrl: url.origin, outputDir: dirname(path), engine: await resolveEngine('playwright'),
  log: (message) => console.error(message),
  browserArgs: ['--enable-webgl', '--use-gl=angle', '--use-angle=swiftshader'],
})
const manifest = { url: url.href, width, height, ...plan, path }
await writeFile(path + '.json', JSON.stringify(manifest, null, 2) + '\n')
console.log(JSON.stringify(manifest))
