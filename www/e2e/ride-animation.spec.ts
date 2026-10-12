import { expect, test } from '@playwright/test'

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
