import { expect, test } from '@playwright/test'

test('section links preserve chart filters and reader scrolling updates the shared URL', async ({ page }) => {
  await page.goto('/?r=jh&s=b&section=states')
  await expect(page.locator('#states')).toBeVisible({ timeout: 30000 })
  await expect.poll(() => page.evaluate(() => new URLSearchParams(location.search).get('section'))).toBe('states')
  expect(await page.evaluate(() => ({ region: new URLSearchParams(location.search).get('r'), stack: new URLSearchParams(location.search).get('s') }))).toEqual({ region: 'jh', stack: 'b' })
  await page.waitForLoadState('networkidle')
  await page.mouse.move(1100, 300)
  await page.mouse.wheel(0, -10000)
  await expect.poll(() => page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), y: scrollY }))).toEqual({ section: null, y: 0 })
  expect(await page.evaluate(() => ({ region: new URLSearchParams(location.search).get('r'), stack: new URLSearchParams(location.search).get('s') }))).toEqual({ region: 'jh', stack: 'b' })
})

test('capture readiness waits for the real chart and legacy heading links become server-visible params', async ({ page }) => {
  await page.goto('/?r=jh&s=b#states')
  await expect(page.locator('#states')).toBeVisible({ timeout: 30000 })
  await expect.poll(() => page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), hash: location.hash }))).toEqual({ section: 'states', hash: '' })
  await page.goto('/?r=jh&s=b&__og=1&screenshot')
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true')
  await expect(page.locator('.js-plotly-plot .legendtext')).toHaveText([
    'Unknown', 'Unknown (12mo)', 'Classic', 'Classic (12mo)', 'Electric', 'Electric (12mo)',
  ])
  await expect(page.getByText('Bike Type', { exact: true })).toHaveCount(0)
})
