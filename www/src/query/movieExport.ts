import { studioDefaults, studioQuery, type MovieSettings } from './movieStudio'
import { parseT } from './timelapseFrames'

export type MovieJob = {
  id: string
  state: 'queued' | 'rendering' | 'complete' | 'failed' | 'cancelled'
  frame: number
  frames: number
  width: number
  height: number
  fps: number
  seconds: number
  query: string
  createdAt: string
  error?: string
}

export type MovieCapabilities = {
  available: boolean
  mode?: 'local' | 'hosted'
  authenticationRequired?: boolean
  signInUrl?: string
  email?: string
}

const allowedParams = new Set(['anim', 'b', 'd', 'clip', 'sp', 'cfps', 'mlen', 'mw', 'mh', 'll', 'theme', 'flow', 'clr', 'trail', 'w', 'wk', 'st', 'sz', 'src', 'cap'])

/** The browser, local renderer and hosted queue share one immutable render plan. */
export function movieRequest(input: unknown): { query: string; settings: MovieSettings } {
  if (!input || typeof input !== 'object' || !('query' in input) || typeof input.query !== 'string') throw new Error('Export needs the preview query.')
  const params = new URLSearchParams(input.query)
  for (const key of params.keys()) {
    if (!allowedParams.has(key)) throw new Error(`Unsupported movie setting: ${key}`)
    if (params.getAll(key).length !== 1) throw new Error(`Duplicate movie setting: ${key}`)
  }
  if (!['clip', 'mlen', 'mw', 'mh', 'cfps'].every((key) => params.has(key))) throw new Error('Export needs an explicit clip, length, FPS, width and height.')
  const clip = params.get('clip')!.split('-').map(parseT)
  if (clip.length !== 2 || clip.some((value) => value === null)) throw new Error('Export needs a valid clip.')
  const settings = {
    ...studioDefaults(params), start: clip[0]!, end: clip[1]!, seconds: Number(params.get('mlen')),
    fps: Number(params.get('cfps')), width: Number(params.get('mw')), height: Number(params.get('mh')),
  }
  return { query: studioQuery(params, settings).toString(), settings }
}
