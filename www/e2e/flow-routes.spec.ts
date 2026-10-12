import { expect, test } from '@playwright/test'

test('street flows toggle preserves the source and view and survives reload', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/stations?m=2506&sel=JC014&fan=1&flow=routes&ll=40.724-74.040+14.00')
  const toggle = page.getByRole('checkbox', { name: 'Inferred bike routes', exact: true })
  await expect(toggle).toBeChecked({ timeout: 30000 })
  await expect(page.getByText('100% routed · JC/Hoboken catalog + NYC sample · chevrons show direction', { exact: true })).toBeVisible({ timeout: 30000 })
  const state = () => {
    const q = new URL(page.url()).searchParams
    return Object.fromEntries(['sel', 'll', 'flow'].map((key) => [key, q.get(key)]))
  }
  await toggle.uncheck()
  expect(state()).toEqual({ sel: 'JC014', ll: '40.724-74.040 14.00', flow: null })
  await toggle.check()
  await page.reload()
  await expect(toggle).toBeChecked({ timeout: 30000 })
  expect(state()).toEqual({ sel: 'JC014', ll: '40.724-74.040 14.00', flow: 'routes' })
  await page.getByRole('button', { name: '⇄ where riders go', exact: true }).click()
  await expect(page.getByText('100% routed · JC/Hoboken catalog + NYC sample · chevrons show direction', { exact: true })).toBeVisible()
  expect(new URL(page.url()).searchParams.get('dir')).toBe('i')
  await page.getByRole('button', { name: '⇄ where riders come from', exact: true }).click()
  expect(new URL(page.url()).searchParams.get('dir')).toBe(null)
  expect(errors).toEqual([])
})

test('routed animation preserves actual concurrency and deterministic movie seek', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/timelapse?anim=1&flow=routes&b=1m&d=250610-250610&t=250610T1800&len=60')
  await expect(page.getByTestId('ride-active')).toHaveText('3,870 rides in progress', { timeout: 120000 })
  await expect(page.getByTestId('ride-routed')).toHaveText('110 active rides on inferred bike routes', { timeout: 30000 })
  await page.getByRole('checkbox', { name: 'Inferred bike routes', exact: true }).uncheck()
  expect(new URL(page.url()).searchParams.get('flow')).toBe(null)
  await expect(page.getByTestId('ride-active')).toHaveText('3,870 rides in progress')
  await expect(page.getByTestId('ride-routed')).toHaveCount(0)
  await page.goto('/timelapse?anim=1&flow=routes&mv=1&b=1m&d=250610-250610&len=60&cfps=30')
  await expect(page.locator('[data-tl-frame="0"]')).toHaveCount(1, { timeout: 120000 })
  await page.evaluate(() => window.__tl!.seek(1350))
  await expect(page.locator('[data-tl-frame="1350"]')).toHaveCount(1)
  await expect(page.getByTestId('ride-routed')).toHaveText('110 active rides on inferred bike routes')
  await expect(page.getByTestId('ride-active')).toHaveText('3,870 rides in progress')
  expect(errors).toEqual([])
})

test('station-count link animates the current day and instant', async ({ page }) => {
  await page.goto('/timelapse?b=15m&d=260930-260930&t=260930T1800')
  const link = page.getByRole('link', { name: 'Animate this day', exact: true })
  await expect(link).toBeVisible({ timeout: 30000 })
  const q = new URL((await link.getAttribute('href'))!, 'http://localhost').searchParams
  expect([q.get('anim'), q.get('d'), q.get('b'), q.get('t')]).toEqual(['1', '260930-260930', '1m', '260930T18'])
})
