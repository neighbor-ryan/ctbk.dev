/** Positive monomial constraints, solved in log space. Derived values do
 *  not become recently edited; snapped values spill to the next variable. */
export type Domain =
  | { kind: 'continuous'; min?: number; max?: number; absorb: true }
  | { kind: 'discrete'; values: readonly number[] }
  | { kind: 'step'; step: number; min?: number; max?: number }
export interface VarSpec<K extends string> { name: K; domain: Domain }
export interface Monomial<K extends string> { exps: Partial<Record<K, number>>; c?: number }

function snap(value: number, domain: Domain): number {
  if (!Number.isFinite(value) || value <= 0) throw new Error('Constraint values must be finite and positive')
  if (domain.kind === 'discrete') {
    return domain.values.reduce((a, b) => Math.abs(Math.log(b / value)) < Math.abs(Math.log(a / value)) ? b : a)
  }
  const rounded = domain.kind === 'step' ? Math.round(value / domain.step) * domain.step : value
  return Math.max(domain.min ?? (domain.kind === 'step' ? domain.step : Number.MIN_VALUE), Math.min(domain.max ?? Infinity, rounded))
}

function rank(matrix: number[][]): number {
  if (!matrix.length) return 0
  const a = matrix.map((row) => [...row])
  let row = 0
  for (let col = 0; col < a[0].length && row < a.length; col++) {
    const pivot = a.findIndex((r, i) => i >= row && Math.abs(r[col]) > 1e-10)
    if (pivot < 0) continue
    ;[a[row], a[pivot]] = [a[pivot], a[row]]
    const divisor = a[row][col]
    for (let c = col; c < a[0].length; c++) a[row][c] /= divisor
    for (let r = row + 1; r < a.length; r++) {
      const factor = a[r][col]
      for (let c = col; c < a[0].length; c++) a[r][c] -= factor * a[row][c]
    }
    row++
  }
  return row
}

function solve(matrix: number[][], rhs: number[]): number[] {
  const n = rhs.length
  const a = matrix.map((row, i) => [...row, rhs[i]])
  for (let col = 0; col < n; col++) {
    const pivot = a.findIndex((r, i) => i >= col && Math.abs(r[col]) > 1e-10)
    if (pivot < 0) throw new Error('Constraint matrix is singular')
    ;[a[col], a[pivot]] = [a[pivot], a[col]]
    const divisor = a[col][col]
    for (let c = col; c <= n; c++) a[col][c] /= divisor
    for (let r = 0; r < n; r++) {
      if (r === col) continue
      const factor = a[r][col]
      for (let c = col; c <= n; c++) a[r][c] -= factor * a[col][c]
    }
  }
  return a.map((row) => row[n])
}

export class LruSystem<K extends string> {
  private current: Record<K, number>
  private recency: K[]
  private computed = new Set<K>()

  constructor(private readonly specs: VarSpec<K>[], private readonly constraints: Monomial<K>[], init: Record<K, number>, order: K[]) {
    this.current = { ...init }
    this.recency = [...order]
    if (new Set(order).size !== specs.length || specs.some((s) => !order.includes(s.name))) throw new Error('LRU order must contain every variable exactly once')
    for (const spec of specs) this.current[spec.name] = snap(init[spec.name], spec.domain)
    this.derive(new Set())
  }

  set(name: K, value: number): Record<K, number> {
    const spec = this.specs.find((s) => s.name === name)
    if (!spec) throw new Error(`Unknown constraint variable '${name}'`)
    const previous = { ...this.current }
    const order = [...this.recency]
    const derived = new Set(this.computed)
    this.current[name] = snap(value, spec.domain)
    this.recency = [...this.recency.filter((key) => key !== name), name]
    try { this.derive(new Set([name])) } catch (err) { this.current = previous; this.recency = order; this.computed = derived; throw err }
    return this.values()
  }

  values(): Record<K, number> { return { ...this.current } }
  order(): K[] { return [...this.recency] }
  derived(): Set<K> { return new Set(this.computed) }

  private derive(fixed: Set<K>): void {
    this.computed = new Set()
    const m = this.constraints.length
    for (let attempt = 0; attempt <= this.specs.length; attempt++) {
      const chosen: K[] = []
      for (const name of this.recency) {
        if (fixed.has(name)) continue
        const trial = [...chosen, name]
        if (rank(this.constraints.map((c) => trial.map((key) => c.exps[key] ?? 0))) === trial.length) chosen.push(name)
        if (chosen.length === m) break
      }
      if (chosen.length !== m) throw new Error('Cannot satisfy constraints in available domains')
      const matrix = this.constraints.map((c) => chosen.map((name) => c.exps[name] ?? 0))
      const rhs = this.constraints.map((c) => Math.log(c.c ?? 1) - this.specs.reduce((sum, s) => chosen.includes(s.name) ? sum : sum + (c.exps[s.name] ?? 0) * Math.log(this.current[s.name]), 0))
      const values = solve(matrix, rhs)
      let clipped = false
      chosen.forEach((name, i) => {
        const exact = Number(Math.exp(values[i]).toPrecision(12))
        const value = snap(exact, this.specs.find((s) => s.name === name)!.domain)
        this.current[name] = value
        this.computed.add(name)
        if (Math.abs(Math.log(value / exact)) > 1e-9) { fixed.add(name); clipped = true }
      })
      if (!clipped) return
    }
    throw new Error('Constraint snapping did not converge')
  }
}
