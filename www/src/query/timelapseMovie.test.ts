import { describe, expect, it } from 'vitest'
import { movieGrid, movieRange } from './timelapseMovie'
import { frameIndex } from './timelapseFrames'

describe('movie capture clock', () => {
  it('clips a two-day range from 04:00 to 04:00 without changing playback speed', () => {
    const start = frameIndex('1m', Date.UTC(2026, 8, 29))
    const end = start + 2880 - 1
    const range = movieRange('1m', start, end, '260929T04-260930T04')
    expect(range).toEqual([start + 240, start + 1680])
    expect(movieRange('1m', start, end)).toEqual([start, end + 1])
    const g = movieGrid(range[1] - range[0], 48, 30)
    expect([g.frames, g.seconds, g.offset(0), g.offset(899)]).toEqual([900, 30, 0, 1438.4])
    expect(() => movieRange('1m', start, end, '260930T04-260929T04')).toThrow('Movie clip needs a start before its exclusive end')
    expect(() => movieRange('1h', 0, 1000000, '260929T0401-260930T04')).toThrow('Movie clip must align to bin edges')
    expect(() => movieRange('1m', start, end, '260929T04-261001T04')).toThrow('Movie clip is outside the date range')
  })
  it('exports one minute bins over one day in exactly sixty seconds', () => {
    const g = movieGrid(1440, 24, 30)
    expect([g.frames, g.fps, g.seconds, g.step, g.offset(0), g.offset(1799)]).toEqual([1800, 30, 60, .8, 0, 1439.2])
    expect(() => g.offset(1800)).toThrow('Movie frame is out of range')
  })
  it('holds a sixty second duration with fifteen minute bins', () => {
    const g = movieGrid(96, 1.6, 30)
    expect([g.frames, g.seconds, g.offset(30)]).toEqual([1800, 60, 1.6])
  })
  it('rounds fractional capture durations up by at most one output frame', () => {
    const g = movieGrid(24, 7, 30)
    expect([g.frames, g.seconds]).toEqual([103, 103 / 30])
  })
  it('retains explicit frames-per-bin presets', () => {
    const g = movieGrid(168, 8, 30, 4)
    expect([g.frames, g.seconds, g.offset(671)]).toEqual([672, 22.4, 167.75])
  })
})
