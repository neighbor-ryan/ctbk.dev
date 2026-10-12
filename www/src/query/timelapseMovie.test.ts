import { describe, expect, it } from 'vitest'
import { movieGrid } from './timelapseMovie'

describe('movie capture clock', () => {
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
