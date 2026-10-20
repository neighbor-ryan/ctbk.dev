import { describe, expect, test } from 'vitest'
import { EMBED_DEFAULT_VIEW, embedViewParam, stationsMapHref } from './embedMap'

describe('embedded map sharing', () => {
  test('uses the fullscreen camera codec without colliding with Home filter keys', () => {
    const view = { lat: 40.724, lng: -74.04, zoom: 14 }
    expect(embedViewParam.encode(view)).toBe('40.724-74.040 14.00')
    expect(embedViewParam.decode('40.724-74.040 14.00')).toEqual(view)
    expect(embedViewParam.decode(undefined)).toEqual(EMBED_DEFAULT_VIEW)
    expect(stationsMapHref(view, ['JC014', 'JC009'], 'dark', '202609')).toBe('/stations?theme=dark&sel=JC014%2CJC009&ll=40.724-74.040+14.00&m=2609')
  })
})
