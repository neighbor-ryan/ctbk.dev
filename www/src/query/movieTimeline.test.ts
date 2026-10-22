import { describe, expect, it } from 'vitest'
import { movieTimeTicks, simulationClock } from './movieTimeline'
import { movieGrid } from './timelapseMovie'

describe('simulation clock', () => {
  it('shows the same 06:56 clock independently of movie FPS or duration', () => {
    const start = Date.UTC(2026, 8, 29, 4)
    const clocks = [[30, 30, 110], [60, 30, 220], [30, 60, 220]].map(([seconds, fps, frame]) => {
      const grid = movieGrid(1440, 1440 / seconds, fps)
      return simulationClock(start + grid.offset(frame) * 60_000)
    })
    expect(clocks).toEqual(Array(3).fill({ label: '06:56', dialLabel: '6:56 AM', hourDegrees: 208, minuteDegrees: 336, period: 'AM' }))
    expect(simulationClock(Date.UTC(2026, 8, 29, 18, 56))).toEqual({ label: '18:56', dialLabel: '6:56 PM', hourDegrees: 208, minuteDegrees: 336, period: 'PM' })
  })
  it('distinguishes midnight and noon while both hands point up', () => {
    expect([0, 12, 24].map((hour) => simulationClock(Date.UTC(2026, 8, 29, hour)))).toEqual([
      { label: '00:00', dialLabel: '12:00 AM', hourDegrees: 0, minuteDegrees: 0, period: 'AM' },
      { label: '12:00', dialLabel: '12:00 PM', hourDegrees: 0, minuteDegrees: 0, period: 'PM' },
      { label: '00:00', dialLabel: '12:00 AM', hourDegrees: 0, minuteDegrees: 0, period: 'AM' },
    ])
  })
})

describe('movie simulation-time ticks', () => {
  it('labels a 24-hour low-to-low clip with an explicit midnight date', () => {
    const start = Date.UTC(2026, 8, 29, 4), end = start + 86_400_000
    expect(movieTimeTicks(start, end, 1000).map(({ label, date }) => [label, date])).toEqual([
      ['04:00', 'Tue Sep 29'], ['06:00', null], ['09:00', null], ['12:00', null], ['15:00', null],
      ['18:00', null], ['21:00', null], ['00:00', 'Wed Sep 30'], ['04:00', 'Wed Sep 30'],
    ])
    expect(movieTimeTicks(start, end, 480).map(({ label, date }) => [label, date])).toEqual([
      ['04:00', 'Tue Sep 29'], ['12:00', null], ['18:00', null], ['00:00', 'Wed Sep 30'], ['04:00', 'Wed Sep 30'],
    ])
  })
  it('keeps day boundaries and exact positions across a 48-hour clip', () => {
    const start = Date.UTC(2026, 8, 29), end = Date.UTC(2026, 9, 1)
    expect(movieTimeTicks(start, end, 1000).map(({ position, label, date }) => [position, label, date])).toEqual([
      [0, '00:00', 'Tue Sep 29'], [.125, '06:00', null], [.25, '12:00', null], [.375, '18:00', null],
      [.5, '00:00', 'Wed Sep 30'], [.625, '06:00', null], [.75, '12:00', null], [.875, '18:00', null], [1, '00:00', 'Thu Oct 1'],
    ])
  })
})
