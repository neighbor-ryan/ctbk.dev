import { test, expect } from '@playwright/test'

test('accepts colon-only seconds and keeps footer geometry stable at hours and midnight', async ({ page }) => {
  await page.goto('/timelapse?anim=1&b=1m&d=260929-260930&t=260929T1859&len=30')
  const length = page.getByRole('textbox', { name: 'Movie length', exact: true })
  await length.fill(':45')
  await length.press('Enter')
  await expect(length).toHaveValue('0:45')
  await expect(page.getByRole('alert')).toHaveCount(0)
  const stamp = page.getByTestId('tl-timestamp')
  const scrub = page.getByRole('slider', { name: 'Scrub', exact: true })
  await expect(stamp).toHaveText('260929T1859')
  const geometry = async () => [await stamp.boundingBox(), await scrub.boundingBox()]
  const before = await geometry()
  await page.getByRole('button', { name: 'Next minute', exact: true }).click()
  await expect(stamp).toHaveText('260929T19')
  expect(await geometry()).toEqual(before)

  await page.goto('/timelapse?anim=1&b=1m&d=260929-260930&t=260929T2359&len=45')
  await expect(stamp).toHaveText('260929T2359')
  const beforeMidnight = await geometry()
  await page.getByRole('button', { name: 'Next minute', exact: true }).click()
  await expect(stamp).toHaveText('260930')
  expect(await geometry()).toEqual(beforeMidnight)
})

/**
 * `/timelapse` (`specs/timelapse-map.md` P1): the GL surface mounts, the
 * clock shows the URL's `t`, and stepping writes `t` back (replace) after a
 * pause. No WebGL-pixel assertions (headless canvases capture black between
 * maplibre renders). The frame data path (interim shard/synth sources) is
 * exercised only as far as "no page errors": the shard read hits
 * `data.ctbk.dev`, so the clock/URL assertions don't wait on it.
 */
test('clock follows `t`; → steps `t` in the URL after pause', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/timelapse?d=260604-260806&t=260620')

  await expect(page.locator('.maplibregl-map')).toHaveCount(1)
  await expect(page.getByTestId('tl-clock')).toHaveText('Sat, Jun 20, 2026')
  await expect(page.getByRole('button', { name: 'Play' })).toHaveCount(1)

  // Play, then pause: the URL is rewritten only on pause (never per frame).
  await page.getByRole('button', { name: 'Play' }).click()
  await expect(page.getByRole('button', { name: 'Pause' })).toHaveCount(1)
  await page.getByRole('button', { name: 'Pause' }).click()
  await expect(page.getByRole('button', { name: 'Play' })).toHaveCount(1)
  const paused = new URL(page.url()).searchParams.get('t')!
  expect(paused).toMatch(/^26(06|07|08)\d{2}$/)

  // → steps one day: `t` advances by exactly one calendar day.
  await page.locator('body').press('ArrowRight')
  const next = new Date(Date.UTC(2000 + Number(paused.slice(0, 2)), Number(paused.slice(2, 4)) - 1, Number(paused.slice(4, 6)) + 1))
  const expected = `${String(next.getUTCFullYear() % 100).padStart(2, '0')}${String(next.getUTCMonth() + 1).padStart(2, '0')}${String(next.getUTCDate()).padStart(2, '0')}`
  await expect.poll(() => new URL(page.url()).searchParams.get('t')).toBe(expected)
  await expect(page.getByTestId('tl-clock')).toHaveText(
    new Intl.DateTimeFormat('en-US', { weekday: 'short', month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' }).format(next),
  )
  expect(errors).toEqual([])
})

test('movie capture samples playback speed and waits for trailing-window data', async ({ page }) => {
  await page.goto('/timelapse?mv=1&b=1h&d=250610-250610&sp=12&w=2h&cfps=12')
  await expect(page.locator('[data-tl-frame="0"]')).toHaveCount(1, { timeout: 120000 })
  expect(await page.evaluate(() => {
    const { frames, fps, seconds } = window.__tl!
    return { frames, fps, seconds }
  })).toEqual({ frames: 24, fps: 12, seconds: 2 })
  await page.evaluate(() => window.__tl!.seek(12))
  await expect(page.locator('[data-tl-frame="12"]')).toHaveCount(1)
  await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Jun 10, 2025 · 13:00 (last 2h)')
})
