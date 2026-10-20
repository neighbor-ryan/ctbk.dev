import { test, expect } from '@playwright/test'

test('hosted movie preview asks for sign-in and preserves the edited movie URL', async ({ page }) => {
  await page.route('**/timelapse?**', (route) => route.fulfill({ contentType: 'text/html', body: '<div data-tl-frame></div><script>window.__tl={frames:900,fps:30,seconds:30,seek:async()=>{}}</script>' }))
  await page.route('**/__movies/capabilities', (route) => route.fulfill({ json: { available: false, mode: 'hosted', authenticationRequired: true, signInUrl: '/signin' } }))
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: { google: true, email: true } }))
  await page.goto('/movie?anim=1&b=1m&clip=260929T04-260930T04&mlen=30&cfps=30&mw=1080&mh=1350')
  await expect(page.getByRole('button', { name: 'Export MP4', exact: true })).toBeDisabled()
  await expect(page.getByText('Sign in to render and download your movie. Any email is welcome.')).toBeVisible()
  const next = await page.getByRole('link', { name: 'Sign in to export' }).getAttribute('href')
  expect(new URL('http://localhost' + next).searchParams.get('next')).toBe('/movie?anim=1&b=1m&clip=260929T04-260930T04&mlen=30&cfps=30&mw=1080&mh=1350')
  await page.getByRole('link', { name: 'Sign in to export' }).click()
  await expect(page.getByRole('link', { name: 'Continue with Google' })).toHaveAttribute('href', '/auth/google?next=' + encodeURIComponent('/movie?anim=1&b=1m&clip=260929T04-260930T04&mlen=30&cfps=30&mw=1080&mh=1350'))
  await expect(page.getByLabel('Email')).toBeVisible()
})

test('sign-in rejects an external return URL and exposes only configured providers', async ({ page }) => {
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: { google: true, email: false } }))
  await page.goto('/signin?next=https://evil.example/')
  await expect(page.getByRole('link', { name: 'Continue with Google' })).toHaveAttribute('href', '/auth/google?next=%2Fmovie')
  await expect(page.getByRole('link', { name: 'Back to movie preview' })).toHaveAttribute('href', '/movie')
  await expect(page.getByLabel('Email')).toHaveCount(0)
})

test('email signup verifies the emailed code before returning to the movie', async ({ page }) => {
  const requests: unknown[] = []
  await page.route('**/api/auth/providers', (route) => route.fulfill({ json: { google: false, email: true } }))
  await page.route('**/api/auth/email/start', async (route) => { requests.push(route.request().postDataJSON()); await route.fulfill({ json: { status: 'sent', id: 'flow-123' } }) })
  await page.route('**/api/auth/email/code', async (route) => { requests.push(route.request().postDataJSON()); await route.fulfill({ json: { ok: true } }) })
  await page.goto('/signin?next=%2Fmovie%3Fanim%3D1')
  await page.getByLabel('Email').fill('anyone@example.test')
  await page.getByRole('button', { name: 'Email me a code' }).click()
  await page.getByLabel('Code').fill('123456')
  await page.getByRole('button', { name: 'Sign in', exact: true }).click()
  await expect(page).toHaveURL('/movie?anim=1')
  expect(requests).toEqual([{ email: 'anyone@example.test' }, { id: 'flow-123', code: '123456' }])
})
