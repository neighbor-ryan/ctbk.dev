import { LruSystem, type VarSpec } from '../lib/lruConstraints'
import { BIN_OPTIONS, binMs, DAY_MS, frameIndex, frameStartMs, GENESIS_MS } from './timelapseFrames'
import { dayOf, HARD_FRAMES, rangeFrames, type Range } from './timelapseControls'

export type LinkedField = 'b' | 'R' | 'f' | 'L'
export const DEFAULT_ORDER: LinkedField[] = ['L', 'f', 'R', 'b']
const codes: Record<LinkedField, string> = { b: 'b', R: 'd', f: 'f', L: 'L' }

export function decodeOrder(raw: string | undefined): LinkedField[] {
  const fields: Record<string, LinkedField> = { b: 'b', d: 'R', f: 'f', L: 'L' }
  const order = [...(raw ?? '')].map((code) => fields[code])
  return order.length === 4 && order.every(Boolean) && new Set(order).size === 4 ? order : [...DEFAULT_ORDER]
}
export function encodeOrder(order: LinkedField[]): string | undefined {
  const raw = order.map((field) => codes[field]).join('')
  return raw === 'Lfdb' ? undefined : raw
}

export function parseLength(raw: string | undefined): number | null {
  if (!raw) return null
  const numeric = Number(raw)
  if (Number.isFinite(numeric) && numeric >= 1) return numeric
  const clock = /^(\d+):(\d{2}(?:\.\d+)?)$/.exec(raw)
  if (clock && Number(clock[2]) < 60) {
    const seconds = Number(clock[1]) * 60 + Number(clock[2])
    return seconds >= 1 ? seconds : null
  }
  const match = /^(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?$/.exec(raw)
  if (!match || (!match[1] && !match[2])) return null
  const seconds = Number(match[1] ?? 0) * 60 + Number(match[2] ?? 0)
  return seconds >= 1 ? seconds : null
}

/** Length uses the actual number of calendar/fixed frames in the range.
 *  The monomial's range unit is the frame count × nominal bin width. */
export function linkedModel(bin: string, range: Range, fps: number, length: number | null, order: LinkedField[], lastDay: number, extraBin?: string) {
  const step = binMs(bin) / 1000
  const [a, b] = rangeFrames(bin, range)
  const bins = [...new Set([...BIN_OPTIONS, bin, ...(extraBin ? [extraBin] : [])])]
  const rangeStep = Math.max(DAY_MS / 1000, step)
  const availableFrames = frameIndex(bin, lastDay + DAY_MS - 1) - frameIndex(bin, GENESIS_MS) + 1
  const maxRange = Math.floor(Math.min(availableFrames, HARD_FRAMES) * step / rangeStep) * rangeStep
  const specs: VarSpec<LinkedField>[] = [
    { name: 'b', domain: { kind: 'discrete', values: bins.map((code) => binMs(code) / 1000) } },
    { name: 'R', domain: { kind: 'step', step: rangeStep, min: rangeStep, max: Math.max(rangeStep, maxRange) } },
    { name: 'f', domain: { kind: 'continuous', min: .25, max: 60, absorb: true } },
    { name: 'L', domain: { kind: 'continuous', min: 1, absorb: true } },
  ]
  const model = new LruSystem(specs, [{ exps: { R: 1, b: -1, f: -1, L: -1 } }], { b: step, R: (b - a + 1) * step, f: fps, L: length ?? (b - a + 1) / fps }, order)
  return { model, binFor: (value: number) => bins.find((code) => Math.abs(binMs(code) / 1000 - value) < 1e-6)! }
}

/** Keep the chosen start; shift it back only at the published-data edge. */
export function rangeForFrames(bin: string, start: number, frames: number, lastDay: number): Range {
  const count = Math.max(1, Math.round(frames))
  const first = frameIndex(bin, start)
  const end = dayOf(frameStartMs(bin, first + count) - 1)
  if (end <= lastDay) return [start, end]
  const final = frameIndex(bin, lastDay + DAY_MS - 1)
  return [Math.max(GENESIS_MS, dayOf(frameStartMs(bin, final - count + 1))), lastDay]
}

export function formatLength(seconds: number): string {
  const minutes = Math.floor(seconds / 60)
  return `${minutes}:${String(Number((seconds - minutes * 60).toFixed(3))).padStart(2, '0')}`
}
