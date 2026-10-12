import { test, expect, type Page } from '@playwright/test'

/**
 * `/stations` time mode (`?tm`, GL only; `specs/timelapse-map.md`): the
 * timelapse dock replaces the month picker, ←/→ step the playhead (`tt`,
 * since `t` is the tile code here) instead of the month, the dock sits
 * above the rides panel, and × leaves the mode. No WebGL-pixel assertions.
 */

const ttOf = (page: Page) => new URL(page.url()).searchParams.get('tt')

test('dropdown selection releases focus for playback and bin shortcuts', async ({ page }) => {
  await page.goto('/stations?tm&b=10m&w=2h&d=250610-250610&tt=250610T19')
  for (const [name, value] of [['Bin', '5m'], ['Window kernel', 'tri'], ['Style', 'act'], ['Circle size', '2']]) {
    const select = page.getByRole('combobox', { name, exact: true })
    await select.focus()
    await select.selectOption(value)
    await expect(select).toHaveValue(value)
    await expect(select).not.toBeFocused()
    await page.keyboard.press('Space')
    await expect(page.getByRole('button', { name: 'Pause', exact: true })).toBeVisible()
    await page.keyboard.press('Space')
    await expect(page.getByRole('button', { name: 'Play', exact: true })).toBeVisible()
  }
  await page.keyboard.press('b')
  await expect(page.getByRole('combobox', { name: 'Bin', exact: true })).toHaveValue('10m')
  await page.keyboard.press('Shift+B')
  await expect(page.getByRole('combobox', { name: 'Bin', exact: true })).toHaveValue('5m')
})

// These tests exercise controls and chart synchronization. Fixed chart
// records remove cold Worker fetch latency from the playhead assertion;
// real frame/ride rendering is also checked in the live Chrome preview.
test.beforeEach(async ({ page }) => {
  await page.route(/\/api\/rides\?/, (route) => route.fulfill({ json: {
    records: [14, 20, 28].map((day) => ({ dt: Date.UTC(2026, 8, day), count: 23, duration: 600 })),
    plan: { outputBin: '1d' },
  } }))
})

test('toggle in, → steps `tt` by one day, × back to the monthly map', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/stations?b=1d&d=260604-260806&tt=260620')
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
    await page.goto('/stations?tm&b=1d&sel=HB101&d=260604-260806&tt=260620')
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

test('time mode defaults to hourly, with a synced rides-chart playhead', async ({ page }) => {
  await page.goto('/stations?tm&sel=HB101&d=260914-260920&tt=260920T12')
  await expect(page.getByLabel('Bin', { exact: true })).toHaveValue('1h')
  await expect(page.getByTestId('rides-playhead')).toHaveAttribute('data-time', String(Date.UTC(2026, 8, 20, 12)))
  await page.getByRole('button', { name: 'Next hour', exact: true }).click()
  await expect(page.getByTestId('rides-playhead')).toHaveAttribute('data-time', String(Date.UTC(2026, 8, 20, 13)))
  const href = await page.getByRole('link', { name: '▶ timelapse', exact: true }).getAttribute('href')
  const query = new URL(href!, 'http://localhost').searchParams
  expect([query.get('b'), query.get('d'), query.get('t'), query.get('sel')]).toEqual(['1h', '260914-260920', '260920T13', 'HB101'])
})

test('preserves a deep-linked hour while default date bounds load', async ({ page }) => {
  let release!: () => void
  const gate = new Promise<void>((resolve) => { release = resolve })
  await page.route('**/assets/station-urls.json', async (route) => {
    const response = await route.fetch()
    const manifest = await response.json()
    await gate
    await route.fulfill({ response, json: { ...manifest, latestMonth: '202609' } })
  })
  await page.goto('/stations?tm&sel=HB101&tt=260928T12')
  await expect(page.getByTestId('tm-dock')).toBeVisible()
  expect(ttOf(page)).toBe('260928T12')
  release()
  await expect(page.getByTestId('rides-playhead')).toHaveAttribute('data-time', String(Date.UTC(2026, 8, 28, 12)))
  expect(ttOf(page)).toBe('260928T12')
})

test('custom minute bins preserve the instant, and windows survive fullscreen navigation', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/stations?tm&sel=HB101&b=15m&w=1h&wk=tri&d=250610-250610&tt=250610T1330')
  await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Jun 10, 2025 · 13:45')
  await expect(page.getByTestId('tl-window')).toHaveText('rolling 1h')
  await page.getByLabel('Custom bin', { exact: true }).fill('45m')
  await page.getByRole('button', { name: 'apply', exact: true }).nth(0).click()
  await expect.poll(() => [new URL(page.url()).searchParams.get('b'), ttOf(page)]).toEqual(['45m', '250610T1330'])
  await page.getByLabel('Trailing window', { exact: true }).fill('3h')
  await page.getByRole('button', { name: 'apply', exact: true }).nth(1).click()
  await expect(page.getByTestId('tl-clock')).toHaveText('Tue, Jun 10, 2025 · 14:15')
  await expect(page.getByTestId('tl-window')).toHaveText('rolling 3h')
  const href = await page.getByRole('link', { name: '▶ timelapse', exact: true }).getAttribute('href')
  const params = new URL(href!, 'http://localhost').searchParams
  expect([params.get('b'), params.get('w'), params.get('wk'), params.get('t')]).toEqual(['45m', '3h', 'tri', '250610T1330'])
  expect(errors).toEqual([])
})

test('a linked length derives fractional fps and stays held when the bin changes', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  await page.goto('/stations?tm&sel=HB101&b=1h&d=250604-250610&tt=250610T13&len=60')
  await expect(page.getByLabel('Frames per second', { exact: true })).toHaveValue('2.8')
  await expect(page.getByLabel('Movie length', { exact: true })).toHaveValue('1:00')
  await page.getByLabel('Bin', { exact: true }).selectOption('15m')
  await expect(page.getByLabel('Frames per second', { exact: true })).toHaveValue('11.2')
  await expect(page.getByLabel('Movie length', { exact: true })).toHaveValue('1:00')
  const params = new URL(page.url()).searchParams
  expect([params.get('len'), params.get('sp'), params.get('lru')]).toEqual(['60', '11.2', 'fdLb'])
  expect(errors).toEqual([])
})
