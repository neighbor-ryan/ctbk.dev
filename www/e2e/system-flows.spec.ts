import { expect, test } from '@playwright/test'
import { waitForGLMap } from './glMap'

test('whole-network monthly flow is opt-in, keeps its viewport, and reports actual routed coverage', async ({ page }, testInfo) => {
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  const requests: string[] = []
  await page.route('https://data.ctbk.dev/flows/v1/**', async (route) => {
    const key = route.request().url().split('/flows/v1/')[1]
    requests.push(key)
    const data = key === 'index.json' ? { version: 1, months: { '202506': { key: 'month.json', bytes: 1 } } } : {
      version: 1, month: '202506', context: 'fixture', graph: { key: 'graph.json', bytes: 1 }, routesPrefix: 'routes-city/v1',
      volumes: [[1, 1000], [-1, 500]], encodedRides: 2100, routedRides: 1500, fallbackRides: 500, roundTrips: 100, provisional: false,
    }
    await route.fulfill({ json: data })
  })
  await page.route('https://data.ctbk.dev/routes-city/v1/graph.json', (route) => route.fulfill({ json: { version: 1, segments: [{ path: [[-74.015, 40.72], [-74.006, 40.73], [-73.995, 40.74]] }] } }))
  await page.goto('/stations?m=2506&ll=40.735-74.010+13.00')
  const checkbox = page.getByRole('checkbox', { name: 'Whole-network bike routes', exact: true })
  await expect(checkbox).not.toBeChecked({ timeout: 30000 })
  expect(requests).toEqual([])
  await checkbox.check()
  await waitForGLMap(page)
  await expect(page.getByTestId('network-legend')).toHaveText('Whole-network bike routes75% of non-roundtrip rides routed · inferred streets · width shows trips')
  expect(requests.sort()).toEqual(['index.json', 'month.json'])
  expect(new URL(page.url()).searchParams.get('ll')).toBe('40.735-74.010 13.00')
  expect(new URL(page.url()).searchParams.get('network')).toBe('')
  await page.reload()
  await expect(checkbox).toBeChecked({ timeout: 30000 })
  await expect(page.getByTestId('network-legend')).toHaveText('Whole-network bike routes75% of non-roundtrip rides routed · inferred streets · width shows trips')
  await expect(page.getByText('Loading...', { exact: true })).toHaveCount(0, { timeout: 30000 })
  await page.screenshot({ path: testInfo.outputPath('network.png') })
  expect(errors).toEqual([])
})

test('an unbuilt month is explicit rather than displaying an empty complete network', async ({ page }) => {
  await page.route('https://data.ctbk.dev/flows/v1/index.json', (route) => route.fulfill({ json: { version: 1, months: {} } }))
  await page.goto('/stations?m=2506&network=1')
  await expect(page.getByTestId('network-legend')).toHaveText('Whole-network bike routesStreet flows have not been built for this month.', { timeout: 30000 })
})
