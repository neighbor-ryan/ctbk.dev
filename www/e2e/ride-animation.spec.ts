import { expect, test } from '@playwright/test'

test('movie clips 04:00–04:00 with a complete fixed sparkline and minimal header', async ({ page }) => {
  await page.clock.install()
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  // Pin immutable payloads: September's public candidate index is provisional.
  await page.route('**/rides-anim-candidate/v1/index.json', (route) => route.fulfill({ json: {
    version: 1, days: {
      '2026-09-29': { key: 'objects/fe8bcce30d8e60b25006b02971e87f1e.bin.gz', stations: 'objects/71710c5cff1e8f32bf6830d28bb8f115.json', rides: 181246, carry: 366 },
      '2026-09-30': { key: 'objects/0717962ea5bf133017428e97234dc55b.bin.gz', stations: 'objects/71710c5cff1e8f32bf6830d28bb8f115.json', rides: 195899, carry: 640 },
    },
  } }))
  await page.goto('/timelapse?anim=1&mv=1&b=1m&d=260929-260930&len=60&cfps=30&clip=260929T04-260930T04')
  await expect(page.locator('[data-tl-frame="0"]')).toHaveCount(1, { timeout: 120000 })
  expect(await page.evaluate(() => {
    const { frames, fps, seconds, start, end } = window.__tl!
    return { frames, fps, seconds, start, end }
  })).toEqual({ frames: 900, fps: 30, seconds: 30, start: '260929T04', end: '260930T04' })
  await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Sep 29, 2026 · 04:00')
  await expect(page.getByTestId('tl-header')).toHaveText('12369AMTue, Sep 29, 2026 · 04:0081 rides in progress electric classic')
  await expect(page.getByRole('img', { name: '12-hour clock, 4:00 AM', exact: true })).toBeVisible()
  await expect(page.getByTestId('ride-clock-hand')).toHaveAttribute('transform', 'rotate(120 44 44)')
  await expect(page.getByTestId('ride-clock-minute-hand')).toHaveAttribute('transform', 'rotate(0 44 44)')
  await expect(page.getByTestId('movie-time-tick')).toHaveText(['04:00Tue Sep 29', '06:00', '09:00', '12:00', '15:00', '18:00', '21:00', '00:00Wed Sep 30', '04:00Wed Sep 30'])
  const sparkline = await page.getByTestId('movie-sparkline').getAttribute('d')
  expect(sparkline?.match(/[ML]/g)?.length).toBe(1440)
  // Software capture can outlive the inactive count chunks' ten-minute GC.
  await page.clock.fastForward(11 * 60_000)
  expect(await page.getByTestId('movie-sparkline').getAttribute('d')).toBe(sparkline)
  for (const frame of [609, 610, 611, 620]) {
    await page.evaluate((i) => window.__tl!.seek(i), frame)
    await expect(page.locator('[data-tl-frame]')).toHaveAttribute('data-tl-frame', String(frame))
  }
  await page.evaluate(() => window.__tl!.seek(750))
  await expect(page.locator('[data-tl-frame="750"]')).toHaveCount(1)
  await expect(page.getByTestId('tl-clock')).toHaveText('Wed, Sep 30, 2026 · 00:00')
  await expect(page.getByTestId('ride-clock-hand')).toHaveAttribute('transform', 'rotate(0 44 44)')
  expect(await page.getByTestId('movie-sparkline').getAttribute('d')).toBe(sparkline)
  expect(Number(await page.getByTestId('movie-playhead').getAttribute('x1'))).toBe(1200 / 1439 * 1000)
  await page.evaluate(() => window.__tl!.seek(751))
  await expect(page.locator('[data-tl-frame="751"]')).toHaveCount(1)
  expect(Number((await page.getByTestId('ride-clock-hand').getAttribute('transform'))!.split(' ')[0].slice(7))).toBeCloseTo(0.8, 8)
  expect(errors).toEqual([])
})

test('June animation shows actual concurrency, steps, and supports deterministic movie seek', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/timelapse?anim=1&b=1m&d=250610-250610&t=250610T1800&len=60')
  await expect(page.getByTestId('ride-active')).toHaveText('3,870 rides in progress', { timeout: 120000 })
  await page.getByRole('button', { name: 'Next minute', exact: true }).click()
  await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Jun 10, 2025 · 18:01')
  await expect(page.getByTestId('ride-active')).toHaveText('3,851 rides in progress')
  await page.getByRole('combobox', { name: 'Ride color' }).selectOption('user')
  expect(new URL(page.url()).searchParams.get('clr')).toBe('user')
  await page.goto('/timelapse?anim=1&mv=1&b=1m&d=250610-250610&len=60&cfps=30')
  await expect(page.locator('[data-tl-frame="0"]')).toHaveCount(1, { timeout: 120000 })
  expect(await page.evaluate(() => {
    const { frames, fps, seconds } = window.__tl!
    return { frames, fps, seconds }
  })).toEqual({ frames: 1800, fps: 30, seconds: 60 })
  await page.evaluate(() => window.__tl!.seek(1350))
  await expect(page.locator('[data-tl-frame="1350"]')).toHaveCount(1)
  await expect(page.getByTestId('ride-active')).toHaveText('3,870 rides in progress')
  expect(errors).toEqual([])
})
