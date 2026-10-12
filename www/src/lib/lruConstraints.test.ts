import { describe, expect, it } from 'vitest'
import { LruSystem, type VarSpec } from './lruConstraints'

type Field = 'b' | 'R' | 'f' | 'L'
const day = 86400
const specs: VarSpec<Field>[] = [
  { name: 'b', domain: { kind: 'discrete', values: [900, 3600] } },
  { name: 'R', domain: { kind: 'step', step: day, min: day } },
  { name: 'f', domain: { kind: 'continuous', min: .25, max: 60, absorb: true } },
  { name: 'L', domain: { kind: 'continuous', min: 1, absorb: true } },
]
const system = (order: Field[] = ['L', 'f', 'R', 'b']) => new LruSystem(specs, [{ exps: { R: 1, b: -1, f: -1, L: -1 } }], { b: 3600, R: 7 * day, f: 8, L: 21 }, order)

describe('linked LRU constraints', () => {
  it('derives length by default, then fps after a length edit, without touching derived recency', () => {
    const model = system()
    expect(model.values()).toEqual({ b: 3600, R: 604800, f: 8, L: 21 })
    expect([...model.derived()]).toEqual(['L'])
    expect(model.set('L', 60)).toEqual({ b: 3600, R: 604800, f: 2.8, L: 60 })
    expect(model.order()).toEqual(['f', 'R', 'b', 'L'])
    expect(model.set('b', 900)).toEqual({ b: 900, R: 604800, f: 11.2, L: 60 })
    expect(model.set('R', 90 * day)).toEqual({ b: 900, R: 7776000, f: 60, L: 144 })
    expect([...model.derived()]).toEqual(['f', 'L'])
    expect(model.order()).toEqual(['f', 'L', 'b', 'R'])
  })

  it('snaps the oldest whole-day range, then lets fps absorb the rounding error', () => {
    const model = system(['R', 'f', 'b', 'L'])
    expect(model.set('L', 50)).toEqual({ b: 3600, R: 1468800, f: 8.16, L: 50 })
    expect([...model.derived()]).toEqual(['R', 'f'])
  })

  it('skips a rank-deficient pair in a two-constraint system', () => {
    const vars = ['a', 'b', 'c', 'd'] as const
    const model = new LruSystem(vars.map((name) => ({ name, domain: { kind: 'continuous', absorb: true } })), [
      { exps: { a: 1, b: 1, c: 1 } }, { exps: { a: 1, b: 1, d: 1 } },
    ], { a: 1, b: 1, c: 1, d: 1 }, [...vars])
    expect(model.set('d', 4)).toEqual({ a: .25, b: 1, c: 4, d: 4 })
    expect([...model.derived()]).toEqual(['a', 'c'])
  })
})
