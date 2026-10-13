import { DAY_MS, formatT, formatYmd, frameIndex, frameStartMs, parseT, parseYmd } from './timelapseFrames'
import { movieGrid } from './timelapseMovie'
import { parseLength } from './timelapseLinked'
import { canonBin } from '../lib/tlGrid'

export type MovieSettings = { start: number; end: number; seconds: number; fps: number; width: number; height: number }

export function studioDefaults(p: URLSearchParams): MovieSettings {
  const clip = p.get('clip')?.split('-')
  const days = p.get('d')?.split('-')
  const start = parseT(clip?.[0] ?? '') ?? parseYmd(days?.[0] ?? '') ?? Date.UTC(2026, 8, 29, 4)
  const end = parseT(clip?.[1] ?? '') ?? ((parseYmd(days?.[1] ?? '') ?? start) + DAY_MS)
  const bin = canonBin(p.get('b') ?? (p.get('anim') ? '1m' : '1d'))
  const bins = frameIndex(bin, end) - frameIndex(bin, start)
  const speed = Number(p.get('sp') ?? 0)
  const fullBins = days ? frameIndex(bin, (parseYmd(days[1]) ?? end) + DAY_MS) - frameIndex(bin, parseYmd(days[0]) ?? start) : bins
  const seconds = Number(p.get('mlen')) || (speed > 0 ? bins / speed : (parseLength(p.get('len') ?? undefined) ?? 30) * bins / fullBins)
  return { start, end, seconds: Math.max(1, Math.min(120, seconds)), fps: Number(p.get('cfps')) || 30, width: Number(p.get('mw')) || 1080, height: Number(p.get('mh')) || 1350 }
}

/** Keep visual params; replace linked day-range controls with an explicit clip/speed. */
export function studioQuery(input: URLSearchParams, s: MovieSettings): URLSearchParams {
  const p = new URLSearchParams(input)
  const bin = canonBin(p.get('b') ?? (p.get('anim') ? '1m' : '1d'))
  const first = frameIndex(bin, s.start), after = frameIndex(bin, s.end)
  if (s.start >= s.end) throw new Error('Start must be before end.')
  if (frameStartMs(bin, first) !== s.start || frameStartMs(bin, after) !== s.end) throw new Error(`Start and end must align to ${bin} bins.`)
  if (after - first > 20_160) throw new Error('Use a shorter clip or coarser bins (maximum 20,160 bins).')
  if (!Number.isFinite(s.seconds) || s.seconds < 1 || s.seconds > 120) throw new Error('Length must be between 1 and 120 seconds.')
  if (!Number.isInteger(s.fps) || s.fps < 1 || s.fps > 60) throw new Error('FPS must be between 1 and 60.')
  if (![s.width, s.height].every((n) => Number.isInteger(n) && n >= 256 && n <= 1920 && n % 2 === 0) || s.width * s.height > 2_073_600) throw new Error('Use even dimensions from 256 to 1920, up to 1920×1080 pixels.')
  const speed = (after - first) / s.seconds
  const grid = movieGrid(after - first, speed, s.fps)
  for (const key of ['mv', 'pv', 'len', 'lru', 'fpb', 't', 'lp', 'job']) p.delete(key)
  p.set('b', bin)
  p.set('d', `${formatYmd(s.start)}-${formatYmd(s.end - 1)}`)
  p.set('clip', `${formatT(s.start)}-${formatT(s.end)}`)
  p.set('sp', String(speed))
  p.set('cfps', String(s.fps))
  p.set('mlen', String(grid.seconds))
  p.set('mw', String(s.width))
  p.set('mh', String(s.height))
  return p
}

export const localDateTime = (ms: number) => new Date(ms).toISOString().slice(0, 16)
export function parseLocalDateTime(text: string): number {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(text)) throw new Error('Choose a valid date and time.')
  const ms = Date.parse(text + ':00Z')
  if (!Number.isFinite(ms) || localDateTime(ms) !== text) throw new Error('Choose a valid date and time.')
  return ms
}
