import { describe, expect, test } from 'vitest'
import { captureView, imageUrl, previewTitle, publicView } from './pagePreview'

describe('public page previews', () => {
  test('canonicalizes visual state while removing auth, export job, and backend parameters', () => {
    expect(publicView(new URL('https://ctbk.dev/?r=jh&section=states&api=dev&token=private&job=123&s=b'))?.href).toBe('https://ctbk.dev/?r=jh&s=b&section=states')
    expect(publicView(new URL('https://ctbk.dev/admin/sessions'))).toBeNull()
    expect(publicView(new URL('https://ctbk.dev/__movies/jobs/123'))).toBeNull()
  })
  test('includes section, filters, deploy version and refresh hour in the image URL', () => {
    const view = publicView(new URL('https://ctbk.dev/?s=b&section=states'))!
    expect(imageUrl(view, 'abc', 7200000).href).toBe('https://ctbk.dev/og.jpg?view=%2F%3Fs%3Db%26section%3Dstates&v=abc&at=2')
    expect(previewTitle(view)).toBe('Citi Bike station states')
  })
  test('movie preview captures its public renderer and preserves viewport and clip', () => {
    expect(captureView(new URL('https://ctbk.dev/movie?anim&clip=260929T04-260930T04&ll=40.7,-74,12')).href).toBe('https://ctbk.dev/movie?anim=&clip=260929T04-260930T04&ll=40.7%2C-74%2C12&__og=1')
  })
})
