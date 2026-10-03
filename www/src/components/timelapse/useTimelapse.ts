/**
 * Timelapse state + data, shared by `/timelapse` and the `/stations` time
 * mode (`specs/timelapse-map.md`): the URL-backed controls (`b` bin, `d`
 * range, the playhead (`t` on `/timelapse`; `tKey`), `sp` speed, `st`
 * preset, `lp` loop, `sz` size, `src`), the continuous playhead with its
 * rAF driver, scrubber hover-preview, chunked frames + idle prefetch, the
 * session-frozen scale, per-frame station attributes (`frame`), the range
 * totals sparkline, and the `tl:*` hotkeys. Chrome and map are the
 * caller's (`components/timelapse/controls.tsx`, `layers.ts`).
 */
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useAction } from 'use-kbd'
import { boolParam, codeParam, intParam, stringParam, useUrlState, type Param } from 'use-prms'
import {
  cachedChunks, useTlFrames, useTlLastDay, useTlPrefetch, useTlTotals, type SourceMode,
} from '../../query/timelapse'
import {
  BIG_STEP, BIN_LABEL, capRange, DEFAULT_SPAN, dayOf, fitRangeToBin, frameCount, rangeFrames, spanEnding, spanRange,
  stepSpeed, UNIT, type Range, type Span,
} from '../../query/timelapseControls'
import {
  accumulateFrame, actAttributes, buildStationTable, chunkIndexMap, chunkOf, chunksCovering, flowAttributes, formatT,
  formatYmd, frameIndex, frameStartMs, parseT, parseYmd, scaleFromChunks, snapToCached, splitAttributes,
  type Bin, type Chunk, type Preset, type StationTable,
  BINS, DAY_MS, DEFAULT_SCALE, GENESIS_MS, PRESETS,
} from '../../query/timelapseFrames'
import { parseSize } from '../../query/timelapseSize'

const { floor, max, min } = Math

export const binParam = codeParam<Bin>('1d', BINS.map((b) => [b, b]))
export const presetParam = codeParam<Preset>('flow', [['flow', 'flow'], ['act', 'act'], ['split', 'split']])
export const srcParam = codeParam<SourceMode>('auto', [['auto', 'a'], ['api', 'api'], ['shard', 'sh'], ['synth', 'sy']])

/** Today's local calendar date as a local-as-UTC midnight. The only
 *  wall-clock read, and only for the default `d` (movies pass `d`). */
export function todayMs(): number {
  const d = new Date()
  return Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())
}

/** Default range: the bin's `DEFAULT_SPAN` (a week at `1h`, a year at
 *  `1d`, …) ending on the last published day. */
function defaultRange(bin: Bin, lastDay: number): Range {
  return spanFrom(DEFAULT_SPAN[bin], lastDay)
}

function spanFrom(span: Span, end: number): Range {
  return span === 'all' ? [GENESIS_MS, end] : spanEnding(span, end, GENESIS_MS)
}

/** `?d=YYMMDD-YYMMDD`: inclusive local day range. */
function rangeParam(bin: Bin, lastDay: number): Param<Range> {
  const def = defaultRange(bin, lastDay)
  return {
    encode: (v) => (v[0] === def[0] && v[1] === def[1] ? undefined : `${formatYmd(v[0])}-${formatYmd(v[1])}`),
    decode: (raw) => {
      const m = raw ? /^(\d{6})-(\d{6})$/.exec(raw) : null
      const a = m ? parseYmd(m[1]) : null
      const b = m ? parseYmd(m[2]) : null
      return a !== null && b !== null && a <= b ? [a, b] : def
    },
  }
}

/** `?t=YYMMDD[THH]`: playhead instant (local-as-UTC ms); absent = range start.
 *  Bin-independent, so switching bins keeps the same instant. */
const tParam: Param<number | undefined> = {
  encode: (v) => (v === undefined ? undefined : formatT(v)),
  decode: (raw) => (raw ? parseT(raw) ?? undefined : undefined),
}

type StationMeta = { name?: string; lat: number; lng: number }
type Regional = Record<string, { lat: number; lng: number; name?: string }>
type Luc = { by_short_name: Record<string, { lat: number; lng: number }> }

/** Station positions + names from the static assets. `stations-regional`
 *  (named, current) wins; `station-luc` fills in retired ids so old frames
 *  still place every station. GAP: the spec's `tl-stations.json` sidecar
 *  (`first`/`last` per station, positions by construction) doesn't exist
 *  yet; ids the frames carry that neither asset places are counted as
 *  "unmapped" in the clock badge. */
export function useStationTable(): StationTable | null {
  const q = useQuery<StationTable>({
    queryKey: ['tl-stations'],
    staleTime: Infinity,
    queryFn: async () => {
      const [regional, luc] = await Promise.all([
        fetch('/assets/stations-regional.json').then((r) => r.json() as Promise<Regional>),
        fetch('/assets/station-luc.json').then((r) => r.json() as Promise<Luc>),
      ])
      const out: Record<string, StationMeta> = {}
      for (const [id, s] of Object.entries(luc.by_short_name)) {
        if (typeof s.lat === 'number' && typeof s.lng === 'number') out[id] = { lat: s.lat, lng: s.lng }
      }
      for (const [id, s] of Object.entries(regional)) out[id] = { name: s.name, lat: s.lat, lng: s.lng }
      return buildStationTable(out)
    },
  })
  return q.data ?? null
}

export interface UseTimelapseOpts {
  /** URL key for the playhead: `t` on `/timelapse`; `/stations` uses `t` for
   *  its tile code, so its time mode passes `tt`. */
  tKey?: string
  /** Movie mode (`/timelapse?mv=1`): no URL writes from the playhead, no
   *  range trim on mount, hotkeys off. */
  mv?: boolean
  /** Register the `tl:*` hotkeys (default on). */
  keysEnabled?: boolean
}

export function useTimelapse({ tKey = 't', mv = false, keysEnabled = true }: UseTimelapseOpts = {}) {
  const qc = useQueryClient()
  const [bin, setBin] = useUrlState('b', binParam)
  // Date bounds for the range picker (and the default range's end): the last
  // published day, falling back to yesterday until `station-urls.json` loads.
  const lastDay = useTlLastDay() ?? todayMs() - DAY_MS
  const rangeP = useMemo(() => rangeParam(bin, lastDay), [bin, lastDay])
  const [range, setRange] = useUrlState('d', rangeP)
  const [tUrl, setTUrl] = useUrlState(tKey, tParam)
  const [sp, setSp] = useUrlState('sp', intParam(8))
  const [preset, setPreset] = useUrlState('st', presetParam)
  const [loop, setLoop] = useUrlState('lp', boolParam)
  const [src] = useUrlState('src', srcParam)
  const [szRaw, setSzRaw] = useUrlState('sz', stringParam())
  const sz = parseSize(szRaw)
  const setSz = useCallback((v: number) => setSzRaw(v === 1 ? undefined : String(v)), [setSzRaw])

  // Inclusive frame range: the frames containing the range's first and last instants.
  const [iStart, iEnd] = rangeFrames(bin, range)
  const clampI = useCallback((i: number) => max(iStart, min(iEnd, i)), [iStart, iEnd])
  const tToI = useCallback((ms: number | undefined) => clampI(ms === undefined ? iStart : frameIndex(bin, ms)), [bin, clampI, iStart])

  // Continuous playhead (frame index + φ). The ref is the source of truth for
  // the rAF loop; the state mirrors it for rendering.
  const tRef = useRef(tToI(tUrl))
  const [t, setT] = useState(tRef.current)
  const [playing, setPlaying] = useState(false)

  // Write `t` (replace) only on pause/step/scrub — never per frame.
  const commitT = useCallback((i: number) => {
    const c = clampI(i)
    tRef.current = c
    setT(c)
    setTUrl(c === iStart ? undefined : frameStartMs(bin, c))
  }, [bin, clampI, iStart, setTUrl])

  // On mount and on a bin switch: re-derive the frame under the URL's
  // instant (`t` is bin-independent) and normalize it (clamped to the
  // range, floored to the bin). On a range change: clamp the playhead into
  // the new range. On mount, a hand-written `d` too long for the bin is
  // trimmed (`capRange`).
  const prevGrid = useRef<{ bin: Bin; iStart: number; iEnd: number } | null>(null)
  useEffect(() => {
    const p = prevGrid.current
    prevGrid.current = { bin, iStart, iEnd }
    if (!p && !mv) {
      const c = capRange(bin, range, 'end')
      if (c.capped) setRange(c.range)
    }
    if (!p || p.bin !== bin) {
      setPlaying(false)
      const i = tToI(tUrl)
      tRef.current = i
      setT(i)
      if (!mv && tUrl !== undefined && frameStartMs(bin, i) !== tUrl) commitT(i)
    } else if (p.iStart !== iStart || p.iEnd !== iEnd) {
      if (!mv) commitT(floor(tRef.current))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [bin, iStart, iEnd])

  /** The playhead's day, for range presets / re-fits. */
  const tDay = useCallback(() => dayOf(frameStartMs(bin, floor(tRef.current))), [bin])
  // Set by a range edit the frame-count guard trimmed; cleared by the next edit.
  const [capNote, setCapNote] = useState<string | null>(null)
  const setRangeEdit = useCallback((r: Range, keep: 'start' | 'end') => {
    const c = capRange(bin, r, keep)
    setCapNote(c.capped ? `capped to ${frameCount(bin, c.range).toLocaleString('en-US')} frames at ${BIN_LABEL[bin]}` : null)
    setRange(c.range)
  }, [bin, setRange])
  const applySpan = useCallback((span: Span) => {
    setCapNote(null)
    setRange(spanRange(span, range, tDay(), GENESIS_MS, lastDay))
  }, [range, tDay, lastDay, setRange])
  // Switching bins keeps `d`, unless it's too many frames at a finer bin or
  // too few at a coarser one (`fitRangeToBin`). The URL `t` is written first
  // (it isn't while playing), since the bin effect re-derives from it.
  const changeBin = useCallback((nb: Bin) => {
    if (nb === bin) return
    const td = tDay()
    if (!mv) setTUrl(frameStartMs(bin, floor(tRef.current)))
    const r = fitRangeToBin(bin, nb, range, td, GENESIS_MS, lastDay)
    setCapNote(null)
    setBin(nb)
    if (r[0] !== range[0] || r[1] !== range[1]) setRange(r)
  }, [bin, mv, range, lastDay, tDay, setBin, setRange, setTUrl])

  // Scrubber hover preview (`peek`): a frame shown on the map + clock
  // without moving the playhead or touching the URL. Hovering pauses
  // playback; leaving resumes it if hovering paused it.
  const [peek, setPeek] = useState<number | null>(null)
  const resumeRef = useRef(false)
  const onPeek = useCallback((i: number | null) => {
    if (i !== null && playing) {
      resumeRef.current = true
      setPlaying(false)
    }
    if (i === null && resumeRef.current) {
      resumeRef.current = false
      setPlaying(true)
    }
    setPeek(i)
  }, [playing])
  const tView = peek ?? t

  const table = useStationTable()
  const frames = useTlFrames(src, bin, tView, 1)
  const readyRef = useRef(frames.ready)
  readyRef.current = frames.ready
  const rangeChunks = useMemo(() => chunksCovering(bin, iStart, iEnd), [bin, iStart, iEnd])
  useTlPrefetch(src, bin, chunkOf(bin, floor(tView)), rangeChunks[0], rangeChunks[rangeChunks.length - 1])

  // Global scale, frozen per bin for the session: p99 of per-station-frame
  // starts + ends over the first chunk pair the playhead lands on (the
  // spec's builder-computed `tl-scale.json` doesn't exist yet). Taken from
  // exactly that chunk, so a movie URL always gets the same scale.
  const [scales, setScales] = useState<Partial<Record<Bin, number>>>({})
  useEffect(() => {
    if (scales[bin] !== undefined) return
    const { start, end } = frames.chunkA
    if (!start || !end) return
    setScales((s) => ({ ...s, [bin]: scaleFromChunks([{ start, end }]) ?? DEFAULT_SCALE[bin] }))
  }, [frames.chunkA, bin, scales])
  const scale = scales[bin] ?? DEFAULT_SCALE[bin]
  const scaleFrozen = scales[bin] !== undefined

  const play = useCallback(() => {
    resumeRef.current = false
    if (floor(tRef.current) >= iEnd) commitT(iStart)
    setPlaying(true)
  }, [iEnd, iStart, commitT])
  const pause = useCallback(() => {
    resumeRef.current = false
    setPlaying(false)
    commitT(floor(tRef.current))
  }, [commitT])
  const toggle = useCallback(() => (playing ? pause() : play()), [playing, pause, play])
  const step = useCallback((n: number) => {
    setPlaying(false)
    commitT(floor(tRef.current) + n)
  }, [commitT])

  // One rAF driver: advance `t` by `sp × dt` (bins/s), stalling while the
  // frame pair under the playhead isn't cached; pause (or loop) at the end.
  useEffect(() => {
    if (!playing) return
    let raf = 0
    let last = performance.now()
    const tick = (now: number) => {
      const dt = (now - last) / 1000
      last = now
      if (readyRef.current) {
        let next = tRef.current + sp * dt
        if (next >= iEnd) {
          if (loop) next = iStart
          else {
            tRef.current = iEnd
            setT(iEnd)
            setPlaying(false)
            setTUrl(frameStartMs(bin, iEnd))
            return
          }
        }
        tRef.current = next
        setT(next)
      }
      raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
  }, [playing, sp, iStart, iEnd, loop, setTUrl, bin])

  // Keyboard (`use-kbd`): all show up in the ShortcutsModal / Omnibar. Off in
  // movie mode (interaction disabled) and whenever the caller says so.
  const unit = UNIT[bin]
  const big = BIG_STEP[bin]
  const on = !mv && keysEnabled
  useAction('tl:play', { label: 'Play / pause', group: 'Timelapse', defaultBindings: ['space'], handler: toggle, enabled: on })
  useAction('tl:prev', { label: `Previous ${unit}`, group: 'Timelapse', defaultBindings: ['arrowleft'], handler: () => step(-1), enabled: on })
  useAction('tl:next', { label: `Next ${unit}`, group: 'Timelapse', defaultBindings: ['arrowright'], handler: () => step(1), enabled: on })
  useAction('tl:prev-week', { label: `Back one ${big.label}`, group: 'Timelapse', defaultBindings: ['shift+arrowleft'], handler: () => step(-big.n), enabled: on })
  useAction('tl:next-week', { label: `Forward one ${big.label}`, group: 'Timelapse', defaultBindings: ['shift+arrowright'], handler: () => step(big.n), enabled: on })
  useAction('tl:slower', { label: 'Slower', group: 'Timelapse', defaultBindings: ['['], handler: () => setSp(stepSpeed(sp, -1)), enabled: on })
  useAction('tl:faster', { label: 'Faster', group: 'Timelapse', defaultBindings: [']'], handler: () => setSp(stepSpeed(sp, 1)), enabled: on })
  useAction('tl:home', { label: 'Jump to range start', group: 'Timelapse', defaultBindings: ['home'], handler: () => { setPlaying(false); commitT(iStart) }, enabled: on })
  useAction('tl:end', { label: 'Jump to range end', group: 'Timelapse', defaultBindings: ['end'], handler: () => { setPlaying(false); commitT(iEnd) }, enabled: on })
  useAction('tl:bin', { label: 'Coarser bin', group: 'Timelapse', defaultBindings: ['b'], handler: () => changeBin(BINS[min(BINS.length - 1, BINS.indexOf(bin) + 1)]), enabled: on })
  useAction('tl:bin-finer', { label: 'Finer bin', group: 'Timelapse', defaultBindings: ['shift+b'], handler: () => changeBin(BINS[max(0, BINS.indexOf(bin) - 1)]), enabled: on })
  useAction('tl:style', { label: 'Cycle style (flow / activity / split)', group: 'Timelapse', defaultBindings: ['s'], handler: () => setPreset(PRESETS[(PRESETS.indexOf(preset) + 1) % PRESETS.length]), enabled: on })
  useAction('tl:loop', { label: 'Toggle loop', group: 'Timelapse', defaultBindings: ['l'], handler: () => setLoop(!loop), enabled: on })

  // Jumps land on the nearest cached frame while the real chunk loads (the
  // prefetch queue has already re-targeted to the new chunk).
  const shown = useMemo(() => {
    if (frames.ready) return tView
    const cached = cachedChunks(qc, src, bin, rangeChunks)
    return snapToCached(cached, bin, floor(tView), 1) ?? tView
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [frames, tView, qc, src, bin, rangeChunks])
  const shownFrames = useTlFrames(src, bin, shown, 0)

  // Per-frame attributes: lerp starts/ends between frames ⌊t⌋ and ⌊t⌋+1
  // (frame ⌊t⌋ alone when ⌊t⌋+1 is past the pyramid's tip), then the
  // preset's radius/color. Fresh typed arrays per frame so deck re-uploads
  // them (same-instance binary values are skipped).
  const mapCache = useRef(new WeakMap<Chunk, Int32Array>())
  const frame = useMemo(() => {
    if (!table) return null
    const n = table.ids.length
    const starts = new Float32Array(n)
    const ends = new Float32Array(n)
    const phi = shown - floor(shown)
    let unmapped = 0
    const map = (c: Chunk) => {
      let m = mapCache.current.get(c)
      if (!m) { m = chunkIndexMap(c, table); mapCache.current.set(c, m) }
      return m
    }
    const add = (out: Float32Array, c: Chunk | undefined, slice: Uint32Array | null, w: number) => {
      if (c && slice && w > 0) unmapped += accumulateFrame(out, slice, map(c), w)
    }
    const f = shownFrames
    const wA = f.bMissing ? 1 : 1 - phi
    add(starts, f.chunkA.start, f.startA, wA)
    add(ends, f.chunkA.end, f.endA, wA)
    if (phi > 0 && !f.bMissing) {
      add(starts, f.chunkB.start, f.startB, phi)
      add(ends, f.chunkB.end, f.endB, phi)
    }
    const flow = preset === 'flow' ? flowAttributes(starts, ends, scale) : null
    const act = preset === 'act' ? actAttributes(starts, ends, scale) : null
    const split = preset === 'split' ? splitAttributes(starts, ends, scale) : null
    return { starts, ends, flow, act, split, unmapped, source: f.chunkA.start?.source, missing: f.aMissing }
  }, [table, shown, shownFrames, preset, scale])

  const totals = useTlTotals(src, bin, iStart, iEnd)

  return {
    qc, src, bin, changeBin, range, setRangeEdit, applySpan, capNote, lastDay, iStart, iEnd, tUrl, tToI,
    tRef, t, setT, playing, play, pause, toggle, step, commitT, peek, onPeek, tView, table, frames, rangeChunks,
    shown, shownFrames, frame, scale, scaleFrozen, preset, setPreset, sp, setSp, loop, setLoop, sz, setSz, totals,
  }
}

export type Timelapse = ReturnType<typeof useTimelapse>
