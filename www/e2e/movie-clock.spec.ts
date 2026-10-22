import { expect, test } from '@playwright/test'

test.use({ timezoneId: 'Pacific/Honolulu' })

test('movie clock follows simulated 06:56 independently of video length and FPS', async ({ page }) => {
  test.setTimeout(90_000)
  for (const [seconds, fps, frame] of [[30, 30, 110], [60, 60, 440]]) {
    await page.goto(`/timelapse?anim=1&mv=1&b=1m&d=260929-260930&clip=260929T04-260930T04&sp=${1440 / seconds}&cfps=${fps}&theme=dark`)
    await expect(page.locator('[data-tl-frame="0"]')).toHaveCount(1, { timeout: 120000 })
    await page.evaluate((i) => window.__tl!.seek(i), frame)
    await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Sep 29, 2026 · 06:56')
    await expect(page.getByTestId('ride-clock')).toHaveAttribute('aria-label', '12-hour clock, 6:56 AM')
    await expect(page.getByTestId('ride-clock-hand')).toHaveAttribute('transform', 'rotate(208 44 44)')
    await expect(page.getByTestId('ride-clock-minute-hand')).toHaveAttribute('transform', 'rotate(336 44 44)')
  }
})

test('two-day Movie Studio shows simulation hours and distinct midnight dates', async ({ page }) => {
  await page.goto('/movie?anim=1&b=1m&d=260929-260930&clip=260929T00-261001T00&mlen=60&cfps=30&mw=1080&mh=1350&ll=40.745-73.985+12.15&theme=dark')
  await expect(page.getByRole('button', { name: 'Play movie preview', exact: true })).toBeEnabled({ timeout: 120000 })
  const frame = page.frameLocator('iframe')
  await expect(frame.getByTestId('movie-time-tick')).toHaveText([
    '00:00Tue Sep 29', '06:00', '12:00', '18:00', '00:00Wed Sep 30', '06:00', '12:00', '18:00', '00:00Thu Oct 1',
  ])
  await expect(frame.getByTestId('movie-time-tick').nth(4)).toHaveAttribute('data-time', '2026-09-30T00:00:00.000Z')
})
