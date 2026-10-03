import { describe, expect, it } from 'vitest'
import { buildStationTable } from '../../query/timelapseFrames'
import { scaleNote } from './controls'
import { pinIndices } from './layers'

describe('pinIndices', () => {
  const table = buildStationTable({
    HB101: { name: 'Hoboken Terminal', lat: 40.735, lng: -74.029 },
    JC115: { name: 'Grove St PATH', lat: 40.719, lng: -74.043 },
    '6602.03': { lat: 40.756, lng: -73.99 },
  })
  it('maps ids to table rows in selection order, dropping unknown ids', () => {
    expect(table.ids).toEqual(['6602.03', 'HB101', 'JC115'])
    expect(pinIndices(table, ['JC115', 'nope', '6602.03'])).toEqual([2, 0])
  })
  it('is empty before the table loads', () => {
    expect(pinIndices(null, ['HB101'])).toEqual([])
  })
})

describe('scaleNote', () => {
  it('frozen scale, default size, zoomed in', () => {
    expect(scaleNote(1234.4, true, 1, 12)).toBe('size = √(rides ÷ 1,234), p99 of the first loaded chunk, fixed for the session')
  })
  it('provisional, size and zoom factors', () => {
    expect(scaleNote(50, false, 1.5, 9)).toBe('size = √(rides ÷ 50) ×1.5 (size) ×0.5 (zoomed out), provisional')
  })
})
