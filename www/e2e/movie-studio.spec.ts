import { test, expect } from '@playwright/test'

test('movie preview steps, scrubs and exports the exact viewport and clip', async ({ page }) => {
  const errors: string[] = []
  page.on('pageerror', (e) => errors.push(e.message))
  let releaseMetadata!: () => void
  const metadataGate = new Promise<void>((done) => { releaseMetadata = done })
  await page.route('**/assets/station-urls.json', async (route) => {
    await metadataGate
    await route.continue()
  })
  await page.route('**/rides-anim/v1/index.json', async (route) => {
    const response = await route.fetch()
    const index = await response.json()
    Object.assign(index.months['202609'].days, {
      '20260929': { key: 'objects/fe8bcce30d8e60b25006b02971e87f1e.bin.gz', rides: 181246, carry: 366 },
      '20260930': { key: 'objects/0717962ea5bf133017428e97234dc55b.bin.gz', rides: 195899, carry: 640 },
    })
    index.months['202609'].stations = 'objects/71710c5cff1e8f32bf6830d28bb8f115.json'
    await route.fulfill({ response, json: index })
  })
  let submitted: unknown = null
  await page.route('**/__movies/capabilities', (route) => route.fulfill({ json: { available: true } }))
  await page.route('**/__movies/jobs', async (route) => {
    submitted = route.request().postDataJSON()
    await route.fulfill({ status: 202, json: { id: 'test-job', state: 'queued', frame: 0, frames: 900 } })
  })
  await page.route('**/__movies/jobs/test-job', (route) => route.fulfill({ json: { id: 'test-job', state: 'complete', frame: 900, frames: 900 } }))
  await page.goto('/movie?anim=1&b=1m&d=260929-260930&len=60&clip=260929T04-260930T04&ll=40.745-73.985+12.15&theme=dark&cfps=30')
  const frame = page.frameLocator('iframe')
  await expect(page.getByRole('button', { name: 'Play movie preview', exact: true })).toBeEnabled({ timeout: 120000 })
  await expect(frame.getByTestId('ride-active')).toHaveText('81 rides in progress')
  const metadataResponse = page.waitForResponse('**/assets/station-urls.json')
  releaseMetadata()
  await metadataResponse
  expect(await page.locator('iframe').evaluate((element) => ({ width: (element as HTMLIFrameElement).contentWindow!.innerWidth, height: (element as HTMLIFrameElement).contentWindow!.innerHeight }))).toEqual({ width: 1080, height: 1350 })
  await page.getByRole('button', { name: 'Next movie frame', exact: true }).click()
  await expect(page.getByRole('slider', { name: 'Movie playhead' })).toHaveValue('1')
  await expect(frame.getByTestId('tl-clock')).toHaveText('Tue, Sep 29, 2026 · 04:01')
  await page.getByRole('slider', { name: 'Movie playhead' }).press('End')
  await expect(frame.getByTestId('tl-clock')).toHaveText('Wed, Sep 30, 2026 · 03:58')
  await expect(page.getByRole('slider', { name: 'Movie playhead' })).toHaveValue('899')
  await page.getByRole('slider', { name: 'Movie playhead' }).press('Home')
  await expect(frame.getByTestId('tl-clock')).toHaveText('Tue, Sep 29, 2026 · 04:00')
  await page.getByRole('button', { name: 'Play movie preview', exact: true }).click()
  await expect.poll(async () => Number(await page.getByRole('slider', { name: 'Movie playhead' }).inputValue())).toBeGreaterThan(5)
  await page.getByRole('button', { name: 'Pause movie preview', exact: true }).click()
  await frame.locator('canvas.maplibregl-canvas').press('Space')
  await expect(page.getByRole('button', { name: 'Pause movie preview', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Pause movie preview', exact: true }).click()
  await page.getByRole('button', { name: 'Export MP4', exact: true }).click()
  const { query } = submitted as { query: string }
  expect(Object.fromEntries(new URLSearchParams(query))).toEqual({ anim: '1', b: '1m', d: '260929-260930', clip: '260929T04-260930T04', ll: '40.745000-73.985000 12.150', theme: 'dark', cfps: '30', sp: '48', mlen: '30', mw: '1080', mh: '1350' })
  await expect(page.getByTestId('movie-export-status')).toHaveText('MP4 readyDownload MP4')
  await expect(page.getByRole('link', { name: 'Download MP4', exact: true })).toHaveAttribute('href', '/__movies/jobs/test-job/file?download=1')
  expect(errors).toEqual([])
})
