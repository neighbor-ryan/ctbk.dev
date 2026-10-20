import { describe, expect, it } from 'vitest'
import { localDateTime, parseLocalDateTime, studioDefaults, studioQuery } from './movieStudio'
import { movieGrid } from './timelapseMovie'

const start = Date.UTC(2026, 8, 29, 4), end = start + 86_400_000
const settings = { start, end, seconds: 30, fps: 30, width: 1080, height: 1350 }
describe('movie studio snapshot', () => {
  it('preserves the exact clip, current view and styling, removing linked controls', () => {
    const input = new URLSearchParams('anim=1&b=1m&d=260929-260930&clip=260929T04-260930T04&len=60&ll=40.745-73.985+12.15&theme=dark&flow=routes&clr=user&trail=300&job=old&fpb=4&t=260929T12&__og=1')
    expect(studioDefaults(input)).toEqual(settings)
    expect(studioQuery(input, settings).toString()).toBe('anim=1&b=1m&d=260929-260930&clip=260929T04-260930T04&ll=40.745-73.985+12.15&theme=dark&flow=routes&clr=user&trail=300&sp=48&cfps=30&mlen=30&mw=1080&mh=1350')
  })
  it('uses an explicit movie speed even when it exceeds interactive speed limits', () => {
    const p = studioQuery(new URLSearchParams('anim=1&b=1m'), { ...settings, seconds: 1 })
    expect(Object.fromEntries(p)).toEqual({ anim: '1', b: '1m', d: '260929-260930', clip: '260929T04-260930T04', sp: '1440', cfps: '30', mlen: '1', mw: '1080', mh: '1350' })
    const grid = movieGrid(1440, Number(p.get('sp')), 30)
    expect([grid.frames, grid.seconds, grid.offset(29)]).toEqual([30, 1, 1392])
  })
  it('uses New York wall time independent of the machine timezone', () => {
    expect(localDateTime(start)).toBe('2026-09-29T04:00')
    expect(parseLocalDateTime('2026-09-29T04:00')).toBe(start)
    expect(() => parseLocalDateTime('2026-02-30T04:00')).toThrow('Choose a valid date and time.')
  })
  it('rejects unaligned clips and invalid render settings', () => {
    expect(() => studioQuery(new URLSearchParams('b=1h'), { ...settings, start: start + 60_000 })).toThrow('Start and end must align to 1h bins.')
    expect(() => studioQuery(new URLSearchParams('b=1m'), { ...settings, width: 1081 })).toThrow('Use even dimensions from 256 to 1920, up to 1920×1080 pixels.')
    expect(() => studioQuery(new URLSearchParams('b=1m'), { ...settings, fps: 61 })).toThrow('FPS must be between 1 and 60.')
    expect(() => studioQuery(new URLSearchParams('b=1m'), { ...settings, seconds: 0 })).toThrow('Length must be between 1 and 120 seconds.')
  })
})
