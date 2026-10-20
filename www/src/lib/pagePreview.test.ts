import { describe, expect, test } from 'vitest'
import { captureView, imageUrl, previewTitle, publicView } from './pagePreview'

describe('public page previews', () => {
  test('canonicalizes visual state while removing auth, export jobs, and arbitrary backend URLs', () => {
    expect(publicView(new URL('https://ctbk.dev/?r=jh&section=states&api=dev&token=private&job=123&s=b'))?.href).toBe('https://ctbk.dev/?api=dev&r=jh&s=b&section=states')
    expect(publicView(new URL('https://ctbk.dev/admin/sessions'))).toBeNull()
    expect(publicView(new URL('https://ctbk.dev/__movies/jobs/123'))).toBeNull()
    expect(publicView(new URL('https://ctbk.dev/stations?m=2609&network=1'))?.href).toBe('https://ctbk.dev/stations?m=2609&network=1')
    expect(publicView(new URL('https://ctbk.dev/stations?api=1&pr=2h'))?.href).toBe('https://ctbk.dev/stations?api=&pr=2h')
    expect(publicView(new URL('https://ctbk.dev/?api=https://example.com&backend=https://example.com'))?.href).toBe('https://ctbk.dev/')
    expect(publicView(new URL('https://ctbk.dev/?api=prod&api=dev'))?.href).toBe('https://ctbk.dev/?api=prod')
    expect(publicView(new URL('https://ctbk.dev/?mapSel=JC014&mapLl=40.724-74.04+14&section=map&s=b&r=jh'))?.href).toBe('https://ctbk.dev/?mapLl=40.724-74.04+14&mapSel=JC014&r=jh&s=b&section=map')
  })
  test('includes section, filters, deploy version and refresh hour in the image URL', () => {
    const view = publicView(new URL('https://ctbk.dev/?s=b&section=states'))!
    expect(imageUrl(view, 'abc', 7200000).href).toBe('https://ctbk.dev/og.jpg?view=%2F%3Fs%3Db%26section%3Dstates&v=abc&at=2')
    expect(previewTitle(view)).toBe('Citi Bike station states')
  })
  test('movie preview captures its public renderer and preserves viewport and clip', () => {
    expect(captureView(new URL('https://ctbk.dev/movie?anim&clip=260929T04-260930T04&ll=40.7,-74,12')).href).toBe('https://ctbk.dev/movie?anim=&clip=260929T04-260930T04&ll=40.7%2C-74%2C12&__og=1')
  })
  test('long public pages preserve valid plot sections and range filters while rejecting unknown anchors', () => {
    const views = [
      'https://ctbk.dev/health?section=feed&theme=dark',
      'https://ctbk.dev/health/feed?section=intervals&from=2026-10-18&to=2026-10-19&wide',
      'https://ctbk.dev/pipeline?section=serving',
      'https://ctbk.dev/s/Columbus%2BDrive?section=trips&ts=b',
      'https://ctbk.dev/stations/JC014?section=states',
      'https://ctbk.dev/health?section=not-a-section',
    ].map(href => publicView(new URL(href))!)
    expect(views.map(view => ({ path: view.pathname, query: view.search, title: previewTitle(view) }))).toEqual([
      { path: '/health', query: '?section=feed&theme=dark', title: 'Citi Bike health · Feed coverage' },
      { path: '/health/feed', query: '?from=2026-10-18&section=intervals&to=2026-10-19&wide=', title: 'Citi Bike health · Update intervals' },
      { path: '/pipeline', query: '?section=serving', title: 'Citi Bike data pipeline · Serving' },
      { path: '/s/Columbus%2BDrive', query: '?section=trips&ts=b', title: 'Citi Bike station details · Monthly trips' },
      { path: '/stations/JC014', query: '?section=states', title: 'Citi Bike station details · Station state' },
      { path: '/health', query: '', title: 'Citi Bike feed and pipeline health' },
    ])
  })
})
