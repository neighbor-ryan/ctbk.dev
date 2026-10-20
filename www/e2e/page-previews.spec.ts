import { expect, test, type Page } from '@playwright/test'

async function anchorDistance(page: Page, id: string): Promise<number> {
  return page.locator(`[id="${id}"]`).evaluate(el => {
    const margin = parseFloat(getComputedStyle(el).scrollMarginTop) || 0
    const target = el.getBoundingClientRect().top + scrollY - margin
    const maximum = Math.max(0, document.documentElement.scrollHeight - innerHeight)
    return Math.round(scrollY - Math.max(0, Math.min(target, maximum)))
  })
}

test('section links preserve chart filters and reader scrolling updates the shared URL', async ({ page }) => {
  await page.goto('/?r=jh&s=b&section=states')
  await expect(page.locator('#states')).toBeVisible({ timeout: 30000 })
  await expect.poll(() => page.evaluate(() => new URLSearchParams(location.search).get('section'))).toBe('states')
  expect(await page.evaluate(() => ({ region: new URLSearchParams(location.search).get('r'), stack: new URLSearchParams(location.search).get('s') }))).toEqual({ region: 'jh', stack: 'b' })
  await expect(page.locator('[data-smg] .uplot')).toHaveCount(1, { timeout: 30000 })
  await page.mouse.move(1100, 300)
  await page.mouse.wheel(0, -10000)
  await expect.poll(() => page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), y: scrollY }))).toEqual({ section: null, y: 0 })
  expect(await page.evaluate(() => ({ region: new URLSearchParams(location.search).get('r'), stack: new URLSearchParams(location.search).get('s') }))).toEqual({ region: 'jh', stack: 'b' })
})

test('Pipeline and health sections restore from server-visible links', async ({ page }) => {
  for (const path of ['/pipeline?section=source-data&theme=dark&__og=1', '/health?section=feed&theme=dark&__og=1']) {
    await page.goto(path)
    const section = new URL(page.url()).searchParams.get('section')!
    await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true', undefined, { timeout: 30000 })
    await expect.poll(() => page.locator(`[id="${section}"]`).evaluate(el => Math.round(el.getBoundingClientRect().top))).toBe(0)
    expect(await page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), theme: new URLSearchParams(location.search).get('theme') }))).toEqual({ section, theme: 'dark' })
  }
  await page.goto('/pipeline?theme=dark#serving')
  await expect.poll(() => page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), hash: location.hash }))).toEqual({ section: 'serving', hash: '' })
  await expect(page.getByRole('link', { name: 'Source Data', exact: true })).toHaveAttribute('href', '/pipeline?theme=dark&section=source-data')
})

test('feed metrics share section, range and width without clearing other params', async ({ page }) => {
  await page.goto('/health/feed?section=intervals&from=2026-10-18&to=2026-10-19&wide&theme=dark&__og=1')
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true')
  await expect.poll(() => anchorDistance(page, 'intervals')).toBe(0)
  await page.getByRole('button', { name: '▣ wide', exact: true }).click()
  expect(await page.evaluate(() => Object.fromEntries(new URLSearchParams(location.search)))).toEqual({ section: 'intervals', from: '2026-10-18', to: '2026-10-19', theme: 'dark', __og: '1' })
  await page.getByRole('button', { name: '7d', exact: true }).click()
  expect(await page.evaluate(() => Object.fromEntries(new URLSearchParams(location.search)))).toEqual({ section: 'intervals', theme: 'dark', __og: '1', span: '7d' })
  await page.reload()
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true')
  await expect.poll(() => anchorDistance(page, 'intervals')).toBe(0)
})

test('station trip section survives canonical redirects and preserves chart filters', async ({ page }) => {
  test.setTimeout(60000)
  await page.goto('/s/JC014?r=2h&section=trips&ts=b&theme=dark&__og=1')
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true', undefined, { timeout: 45000 })
  await expect(page.locator('#trips .js-plotly-plot')).toHaveCount(1)
  await expect.poll(() => page.locator('#trips').evaluate(el => Math.round(el.getBoundingClientRect().top))).toBe(0)
  expect(await page.evaluate(() => ({ section: new URLSearchParams(location.search).get('section'), stack: new URLSearchParams(location.search).get('ts'), range: new URLSearchParams(location.search).get('r') }))).toEqual({ section: 'trips', stack: 'b', range: '2h' })
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

test('embedded map links and reload preserve selection, camera, and Home filters', async ({ page }) => {
  await page.goto('/?r=jh&s=b&section=map&mapSel=JC014,JC009&mapLl=40.724-74.04+14.00&theme=dark&__og=1')
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true')
  await expect(page.getByTestId('source-info').getByText('2 selected sources', { exact: true })).toHaveCount(1)
  const compare = page.getByRole('link', { name: 'Compare on the stations page →' })
  const fullscreen = page.getByRole('link', { name: 'Full screen version →' })
  const manifest = await (await page.request.get('/assets/station-urls.json')).json()
  const before = await fullscreen.getAttribute('href')
  expect(before).toBe(`/stations?theme=dark&sel=JC014%2CJC009&ll=40.724-74.040+14.00&m=${manifest.latestMonth.slice(2)}`)
  await expect(compare).toHaveAttribute('href', before!)
  const canvas = page.locator('.maplibregl-canvas').first()
  const box = await canvas.boundingBox()
  if (!box) throw new Error('Embedded map canvas did not render')
  const x = box.x + box.width * 0.5
  const y = Math.min(box.y + 300, 550)
  await page.mouse.move(x, y)
  await page.mouse.down()
  await page.mouse.move(x + 100, y + 20, { steps: 12 })
  await page.mouse.up()
  await expect.poll(() => fullscreen.getAttribute('href')).not.toBe(before)
  const moved = await fullscreen.getAttribute('href')
  await expect(compare).toHaveAttribute('href', moved!)
  const state = await page.evaluate(() => Object.fromEntries(new URLSearchParams(location.search)))
  expect({ region: state.r, stack: state.s, section: state.section, selection: state.mapSel }).toEqual({ region: 'jh', stack: 'b', section: 'map', selection: 'JC014,JC009' })
  await page.reload()
  await page.waitForFunction(() => document.documentElement.dataset.ogReady === 'true')
  await expect(fullscreen).toHaveAttribute('href', moved!)
  await expect(page.getByTestId('source-info').getByText('2 selected sources', { exact: true })).toHaveCount(1)
})
