import { expect, test } from '@playwright/test'

test('health separates primary degradation from combined coverage and renders its shared card', async ({ page }) => {
  const coverage = {
    date: '2026-10-20', settled: 3, primary: 1, backup: 3, combined: 3, recovered: 2,
    hours: [[3, 1, 3, 3], ...Array.from({ length: 23 }, () => [0, 0, 0, 0])],
  }
  await page.route('**/api/health*', (route) => route.fulfill({ json: {
    generatedAt: 1792497600,
    feed: {
      latestPoll: null, drift: null, todayCount: 1, todayExpected: 5, last7Days: [],
      coverage: { complete: true, today: coverage, missing: [], unexplained: [], recent: { primary: ['00:01', '00:02'], backup: [], combined: [] } },
      coverageHistory: [coverage],
    },
    compactions: { daily: { latestDate: null, count: 0 }, hourly: { latestKey: null, todayCount: 0 } },
    cascade: { cells: [], expectedCells: [] }, pyramids: [], tripdata: null, builds: [],
  } }))
  const errors: string[] = []
  page.on('pageerror', (error) => errors.push(error.message))
  await page.goto('/health')
  await expect(page.getByText('Combined: 3/3', { exact: true })).toBeVisible()
  const description = page.getByText('settled minute slots observed.', { exact: false })
  await expect(description).toHaveText('Combined: 3/3 settled minute slots observed. Backup recovered 2; 0 slots absent from both feeds. A minute slot can be empty because an upstream timestamp crossed a minute boundary; this is not a count of lost generations.')
  const image = page.getByRole('img', { name: 'Recent feed coverage by UTC hour, with separate primary, backup and combined rows.' })
  await expect(image).toBeVisible()
  await expect.poll(() => image.evaluate((el: HTMLImageElement) => [el.naturalWidth, el.naturalHeight])).toEqual([1200, 630])
  expect(errors).toEqual([])
})
