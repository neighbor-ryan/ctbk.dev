import { test, expect, type Page } from '@playwright/test'

/**
 * `/stations` time mode (`?tm`, GL only; `specs/timelapse-map.md`): the
 * timelapse dock replaces the month picker, ←/→ step the playhead (`tt`,
 * since `t` is the tile code here) instead of the month, the dock sits
 * above the rides panel, and × leaves the mode. No WebGL-pixel assertions.
 */

const ttOf = (page: Page) => new URL(page.url()).searchParams.get('tt')

test('toggle in, → steps `tt` by one day, × back to the monthly map', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/stations?d=260604-260806&tt=260620')
  await page.getByTestId('tm-toggle').click()
  await expect(page.getByTestId('tm-dock')).toBeVisible()
  await expect(page.getByTestId('stations-header')).toHaveText('Citi Bike rides by station, over timeby month')
  await expect(page.getByTestId('tl-clock')).toHaveText('Sat, Jun 20, 2026')

  await page.locator('body').press('ArrowRight')
  await expect.poll(() => ttOf(page)).toBe('260621')
  await expect(page.getByTestId('tl-clock')).toHaveText('Sun, Jun 21, 2026')

  await page.getByTestId('tm-exit').click()
  await expect(page.getByTestId('tm-dock')).toHaveCount(0)
  expect(new URL(page.url()).searchParams.has('tm')).toBe(false)
  await expect(page.getByTestId('tm-toggle')).toHaveText('▶ time')
  expect(errors).toEqual([])
})

test.describe('phone', () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true })

  test('dock above the rides panel; ⚙ toggles settings', async ({ page }) => {
    await page.goto('/stations?tm&sel=HB101&d=260604-260806&tt=260620')
    const dock = page.getByTestId('tm-dock')
    await expect(dock).toBeVisible()
    await expect(page.getByTestId('tl-clock')).toHaveText('Sat Jun 20, 2026')
    await expect(page.getByTestId('tl-settings')).toHaveCount(0)
    await page.getByTestId('tl-gear').click()
    await expect(page.getByTestId('tl-settings')).toBeVisible()

    const panel = page.getByTestId('rides-panel')
    await expect(panel).toBeVisible()
    const d = (await dock.boundingBox())!
    const p = (await panel.boundingBox())!
    expect(d.y + d.height).toBeLessThanOrEqual(p.y)
  })
})
