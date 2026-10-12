/**
 * Timelapse chrome shared by `/timelapse` (`pages/Timelapse.tsx`) and the
 * `/stations` time mode (`StationsTimeMode.tsx`): transport icons, the
 * scrubber (totals sparkline + hover preview), range picker, preset legend
 * and the hover card. Presentational only; state lives in `useTimelapse`.
 */
import { FloatingPortal, offset, shift, useFloating } from '@floating-ui/react'
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { useCanHover } from '../../lib/useMediaQuery'
import {
  bigStep, binLabel, dayOf, editRange, isoDay, parseIsoDay, rangePresets, shortLabel, SOFT_FRAMES, spanRange, speedLabel,
  suggestBin, binUnit, type Range, type Span,
} from '../../query/timelapseControls'
import {
  ACT, BIN_OPTIONS, binMs, COOL, DAY_MS, formatT, formatYmd, frameStartMs, GENESIS_MS, NEUTRAL, PRESETS, SPLIT, WARM, type Bin, type Preset,
} from '../../query/timelapseFrames'
import { formatLength, parseLength } from '../../query/timelapseLinked'
import { canonBin } from '../../lib/tlGrid'
import { factorLabel, SIZES, zoomFactor } from '../../query/timelapseSize'
import css from '../../timelapse.module.css'
import { rampRgb } from '../flowLens'
import { Tip, type TipProps } from '../Tip'
import type { Timelapse } from './useTimelapse'

const { floor, max, min, round } = Math

export const PIN_RGB: [number, number, number] = [255, 210, 74]
export const rgbCss = ([r, g, b]: readonly [number, number, number], a = 1) => `rgba(${r}, ${g}, ${b}, ${a})`
export const ACT_GRADIENT = `linear-gradient(to right, ${[0, 0.25, 0.5, 0.75, 1].map((f) => rgbCss(rampRgb(f))).join(', ')})`
export const PRESET_LABEL: Record<Preset, string> = { flow: 'net flow', act: 'activity', split: 'starts / ends' }
export const FLOW_GRADIENT = `linear-gradient(to right, ${rgbCss(COOL)}, ${rgbCss(NEUTRAL)}, ${rgbCss(WARM)})`
/** Wide enough for the docked panel + always-expanded controls. */
export const WIDE = '(min-width: 768px)'

/** `Tip` on hover-capable devices only: on touch, a tap would open it and
 *  leave it stuck on screen. */
export function HTip(props: TipProps) {
  const canHover = useCanHover()
  return canHover ? <Tip {...props} /> : props.children
}

export function WindowNote({ tl }: { tl: Timelapse }) {
  return tl.windowWidth > 1 ? <span className={css.windowNote} data-testid="tl-window">rolling {tl.windowCode}</span> : null
}

export function Chevron({ dir }: { dir: 'left' | 'right' }) {
  return (
    <svg width={14} height={14} viewBox="0 0 14 14" aria-hidden>
      <path d={dir === 'left' ? 'M9 2 L4 7 L9 12' : 'M5 2 L10 7 L5 12'} fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

export function PlayIcon({ playing }: { playing: boolean }) {
  return (
    <svg width={18} height={18} viewBox="0 0 18 18" aria-hidden>
      {playing
        ? <><rect x={4} y={3} width={3.5} height={12} rx={1} fill="currentColor" /><rect x={10.5} y={3} width={3.5} height={12} rx={1} fill="currentColor" /></>
        : <path d="M5 2.5 L15 9 L5 15.5 Z" fill="currentColor" strokeLinejoin="round" />}
    </svg>
  )
}

export function GearIcon() {
  return (
    <svg width={16} height={16} viewBox="0 0 24 24" aria-hidden>
      <path
        fill="currentColor"
        d="M19.4 13a7.6 7.6 0 0 0 0-2l2.1-1.6-2-3.5-2.5 1a7.4 7.4 0 0 0-1.7-1L15 3h-4l-.4 2.9a7.4 7.4 0 0 0-1.7 1l-2.5-1-2 3.5L6.6 11a7.6 7.6 0 0 0 0 2l-2.1 1.6 2 3.5 2.5-1c.5.4 1.1.8 1.7 1L11 21h4l.4-2.9c.6-.2 1.2-.6 1.7-1l2.5 1 2-3.5zM13 15.5a3.5 3.5 0 1 1 0-7 3.5 3.5 0 0 1 0 7z"
        transform="translate(-1 0)"
      />
    </svg>
  )
}

/** The preset's color key as one thin bar (split: two glyphs). */
export function LegendBar({ preset }: { preset: Preset }) {
  if (preset === 'split') {
    return (
      <span className={css.legendGlyphs}>
        <span><svg width={12} height={12}><circle cx={6} cy={6} r={5} fill={rgbCss(WARM, SPLIT.alpha / 255)} /></svg> starts</span>
        <span><svg width={12} height={12}><circle cx={6} cy={6} r={4.5} fill="none" stroke={rgbCss(COOL)} strokeWidth={SPLIT.ringWidth} /></svg> ends</span>
      </span>
    )
  }
  return <span className={css.legendBar} style={preset === 'flow' ? { background: FLOW_GRADIENT } : { background: ACT_GRADIENT, opacity: ACT.alpha / 255 + 0.2 }} />
}

/** The legend's size note: the scale, any size / zoom factors, and whether
 *  the scale is frozen yet. */
export function scaleNote(scale: number, frozen: boolean, sz: number, zoom: number): string {
  const fmt = (n: number) => round(n).toLocaleString('en-US')
  const zf = zoomFactor(zoom)
  const terms = [sz !== 1 && `${factorLabel(sz)} (size)`, zf < 1 && `${factorLabel(zf)} (zoomed out)`].filter(Boolean).join(' ')
  return `size = √(rides ÷ ${fmt(scale)})${terms ? ` ${terms}` : ''}, ${frozen ? 'p99 of the first loaded chunk, fixed for the session' : 'provisional'}`
}

/** The expanded legend: title, bar labels, and the shared size note. */
export function Legend({ preset, unit, note }: { preset: Preset; unit: string; note: string }) {
  return (
    <>
      <div className={css.legendTitle}>Rides per {unit}, by station · {PRESET_LABEL[preset]}</div>
      {preset === 'flow' && (
        <div className={css.legendLabels}>
          <span>net arrivals</span>
          <span>balanced</span>
          <span>net departures</span>
        </div>
      )}
      {preset === 'act' && (
        <div className={css.legendLabels}>
          <span>quiet</span>
          <span>busy (overlaps glow)</span>
        </div>
      )}
      {preset === 'split' && <div className={css.legendLabels}><span>disk = starts, ring = ends</span></div>}
      <div className={css.legendNote}>{note}; faint dot = no rides that {unit}</div>
    </>
  )
}

const HOVER_W = 260

/** Mouse-only: the hovered station's current-frame stats, by the cursor. */
export function HoverCard({ x, y, w, name, starts, ends, selected, multi }: {
  x: number
  y: number
  /** Page width: the card flips to the cursor's left near the right edge. */
  w: number
  name: string
  starts: number
  ends: number
  selected: boolean
  multi: boolean
}) {
  const hint = multi ? 'click to toggle' : selected ? 'shift/⌘-click to remove' : 'click to select · shift/⌘-click to add'
  return (
    <div className={css.hoverCard} style={{ left: x + 14 + HOVER_W > w ? max(8, x - 14 - HOVER_W) : x + 14, top: y + 14, width: HOVER_W }}>
      <span className={css.cardName}>{name}</span>
      <span className={css.cardStat}><Stats starts={starts} ends={ends} /></span>
      <span className={css.cardHint}>{hint}</span>
    </div>
  )
}

export function Stats({ starts, ends }: { starts: number; ends: number }) {
  const fmt = (v: number) => round(v).toLocaleString('en-US')
  const net = starts - ends
  return <>{fmt(starts)} starts · {fmt(ends)} ends · net {net >= 0 ? '+' : ''}{fmt(net)}</>
}

/** SVG path over `values` (NaN = gap), scaled to its own peak. Point `f`
 *  sits at its slot's center (`center`), or at `f / (n − 1)` of the width
 *  (`edge`: where a range input's thumb sits for value `f`). */
export function sparkPath(values: Float64Array, W: number, H: number, align: 'center' | 'edge' = 'center'): string {
  const n = values.length
  if (!n) return ''
  let peak = 0
  for (const v of values) if (v === v && v > peak) peak = v
  if (!peak) return ''
  const parts: string[] = []
  let pen = false
  for (let f = 0; f < n; f++) {
    const v = values[f]
    if (v !== v) { pen = false; continue }
    const x = align === 'edge' ? edgeX(f, n) * W : ((f + 0.5) / n) * W
    const y = H - 2 - (v / peak) * (H - 6)
    parts.push(`${pen ? 'L' : 'M'}${x.toFixed(1)},${y.toFixed(1)}`)
    pen = true
  }
  return parts.join(' ')
}

/** Fraction of the width at which frame `f` of `n` sits under a range input. */
export function edgeX(f: number, n: number): number {
  return n > 1 ? f / (n - 1) : 0.5
}

/** Range picker for `d=`: two native date inputs (genesis … the last
 *  published day) and the bin's quick presets (`RANGE_PRESETS`). */
export function RangePicker({ bin, range, lastDay, tDay, onEdit, onSpan, onTouch, derived }: {
  bin: Bin
  range: Range
  lastDay: number
  tDay: number
  onEdit: (r: Range, keep: 'start' | 'end') => void
  onSpan: (span: Span) => void
  onTouch?: () => void
  derived?: boolean
}) {
  const lo = isoDay(GENESIS_MS)
  const hi = isoDay(lastDay)
  const edit = (side: 'start' | 'end', v: string) => {
    const ms = parseIsoDay(v)
    if (ms !== null) onEdit(editRange(range, side, ms, GENESIS_MS, lastDay), side)
  }
  return (
    <div className={css.group} role="group" aria-label="Range" style={{ fontStyle: derived ? 'italic' : undefined }}>
      <input type="date" className={css.date} value={isoDay(range[0])} min={lo} max={hi} aria-label="Range start" onFocus={onTouch} onChange={(e) => edit('start', e.target.value)} />
      <span className={css.dash}>–</span>
      <input type="date" className={css.date} value={isoDay(range[1])} min={lo} max={hi} aria-label="Range end" onFocus={onTouch} onChange={(e) => edit('end', e.target.value)} />
      {rangePresets(bin).map(({ label, span }) => {
        const r = spanRange(span, range, tDay, GENESIS_MS, lastDay)
        // A day span clamped at genesis / the last day doesn't count as picked.
        const active = r[0] === range[0] && r[1] === range[1] && (span === 'all' || span.u === 'mo' || r[1] - r[0] === (span.n - 1) * DAY_MS)
        const desc = span === 'all' ? 'all of history' : `${span.n} ${span.u === 'd' ? 'day' : 'month'}${span.n === 1 ? '' : 's'}`
        return (
          <Tip key={label} content={`Range: ${desc} (${formatYmd(r[0])}–${formatYmd(r[1])})`}>
            <button type="button" className={`${css.btn} ${css.preset}`} aria-pressed={active} onClick={() => onSpan(span)}>{label}</button>
          </Tip>
        )
      })}
    </div>
  )
}

/**
 * Range input over the frame range with the totals strip behind it.
 * Hovering (mouse) previews the frame under the pointer (`onPeek`: map +
 * clock, a hover line and a tip with the frame's Σ starts); click commits
 * it. Touch keeps the native behavior (tap/drag → commit on release).
 */
export function Scrubber({
  bin,
  iStart,
  iEnd,
  i,
  peek,
  totals,
  onScrub,
  onCommit,
  onPeek,
}: {
  bin: Bin
  iStart: number
  iEnd: number
  i: number
  peek: number | null
  totals: Float64Array
  onScrub: (i: number) => void
  onCommit: (i: number) => void
  onPeek: (i: number | null) => void
}) {
  const W = 1000
  const H = 34
  const n = iEnd - iStart + 1
  const path = useMemo(() => sparkPath(totals, W, H, 'edge'), [totals])
  const boxRef = useRef<HTMLDivElement>(null)
  const frameAt = (clientX: number) => {
    const r = boxRef.current!.getBoundingClientRect()
    const f = n > 1 ? round(((clientX - r.left) / r.width) * (n - 1)) : 0
    return iStart + max(0, min(n - 1, f))
  }
  const px = peek === null ? null : edgeX(peek - iStart, n)
  const { refs, floatingStyles } = useFloating({
    open: peek !== null,
    placement: 'top',
    middleware: [offset(8), shift({ padding: 8 })],
  })
  useLayoutEffect(() => {
    const box = boxRef.current
    if (px === null || !box) return
    refs.setPositionReference({
      getBoundingClientRect: () => {
        const r = box.getBoundingClientRect()
        const x = r.left + px * r.width
        return new DOMRect(x, r.top, 0, r.height)
      },
    })
  }, [px, refs])
  const rides = peek === null ? NaN : totals[peek - iStart]
  return (
    <div
      ref={boxRef}
      className={css.scrub}
      onPointerMove={(e) => { if (e.pointerType === 'mouse') onPeek(frameAt(e.clientX)) }}
      onPointerLeave={(e) => { if (e.pointerType === 'mouse') onPeek(null) }}
    >
      <svg className={css.strip} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
        <rect x={0} y={0} width={W} height={H} fill="rgba(255,255,255,0.06)" />
        {path && <path d={path} fill="none" stroke="#7fb3ff" strokeWidth={2} vectorEffect="non-scaling-stroke" />}
        {px !== null && <line x1={px * W} x2={px * W} y1={0} y2={H} stroke="rgba(255,255,255,0.85)" strokeWidth={1} vectorEffect="non-scaling-stroke" data-testid="tl-hover-line" />}
      </svg>
      <input
        type="range"
        className={css.range}
        min={iStart}
        max={iEnd}
        step={1}
        value={i}
        aria-label="Scrub"
        onChange={(e) => onScrub(Number(e.target.value))}
        onPointerUp={(e) => onCommit(e.pointerType === 'mouse' && peek !== null ? peek : Number((e.target as HTMLInputElement).value))}
        onKeyUp={(e) => onCommit(Number((e.target as HTMLInputElement).value))}
      />
      {peek !== null && (
        <FloatingPortal>
          <div ref={refs.setFloating} style={floatingStyles} className={css.scrubTip} data-testid="tl-scrub-tip">
            {shortLabel(bin, frameStartMs(bin, peek))} · {rides === rides ? `${round(rides).toLocaleString('en-US')} rides` : 'loading…'}
          </div>
        </FloatingPortal>
      )}
    </div>
  )
}

/** The ⚙ settings row: range, bin, speed, style, circle size, loop, plus
 *  the too-many-frames bin hint and (touch) a key cheat sheet. */
export function TlSettings({ tl, animation = false }: { tl: Timelapse; animation?: boolean }) {
  const { bin, changeBin, range, setRangeEdit, applySpan, capNote, lastDay, iStart, iEnd, t, sp, setSp, preset, setPreset, sz, setSz, loop, setLoop } = tl
  const canHover = useCanHover()
  const [customBin, setCustomBin] = useState('')
  const [binError, setBinError] = useState<string | null>(null)
  const [windowDraft, setWindowDraft] = useState(tl.windowCode)
  const [windowError, setWindowError] = useState<string | null>(null)
  const [fpsDraft, setFpsDraft] = useState(String(sp))
  const [lengthDraft, setLengthDraft] = useState(formatLength(tl.length))
  const [linkedError, setLinkedError] = useState<string | null>(null)
  useEffect(() => setFpsDraft(String(sp)), [sp])
  useEffect(() => setLengthDraft(formatLength(tl.length)), [tl.length])
  useEffect(() => setWindowDraft(tl.windowCode), [tl.windowCode])
  const unit = binUnit(bin)
  const big = bigStep(bin)
  const nFramesRange = iEnd - iStart + 1
  // Sub-day bins only: `1d` over all of history (~4,850 frames) is a preset.
  const suggested = nFramesRange > SOFT_FRAMES && binMs(bin) < DAY_MS ? suggestBin(range) : null
  const fmt = (n: number) => round(n).toLocaleString('en-US')
  return (
    <div className={css.row} data-testid="tl-settings">
      <RangePicker
        bin={bin}
        range={range}
        lastDay={lastDay}
        tDay={dayOf(frameStartMs(bin, floor(t)))}
        onEdit={setRangeEdit}
        onSpan={applySpan}
        onTouch={() => tl.touch('R')}
        derived={tl.derived.has('R')}
      />
      <HTip content={`Bin: time per frame (b = coarser, shift+b = finer). ${tl.derived.has('b') ? 'Derived; click to make it user-set.' : 'Editing bin derives the least recently edited control.'}`}>
        <label className={css.field} style={{ fontStyle: tl.derived.has('b') ? 'italic' : undefined }}>
          <span className={css.fieldLabel}>bin</span>
          <select className={css.select} value={bin} onFocus={() => tl.touch('b')} onChange={(e) => changeBin(e.target.value as Bin)} aria-label="Bin">
            {(BIN_OPTIONS.includes(bin) ? BIN_OPTIONS : [...BIN_OPTIONS, bin]).map((b) => <option key={b} value={b}>{binLabel(b)}</option>)}
          </select>
        </label>
      </HTip>
      <form className={css.group} onSubmit={(e) => {
        e.preventDefault()
        try { changeBin(canonBin(customBin)); setBinError(null) } catch (err) { setBinError((err as Error).message) }
      }}>
        <HTip content="Custom bin: e.g. 20m, 45m, 2h, 4mo. Fixed bins divide a day or span whole days; calendar bins divide a year.">
          <input className={css.date} style={{ width: 72 }} value={customBin} onChange={(e) => setCustomBin(e.target.value)} placeholder="20m" aria-label="Custom bin" />
        </HTip>
        <button type="submit" className={css.btn}>apply</button>
        {binError && <span role="alert" className={css.note}>{binError}</span>}
      </form>
      {!animation && <form className={css.group} onSubmit={(e) => {
        e.preventDefault()
        try { tl.changeWindow(windowDraft); setWindowError(null) } catch (err) { setWindowError((err as Error).message) }
      }}>
        <HTip content="Trailing window: sum whole bins ending at this frame. Must fit within one chunk.">
          <label className={css.field}>
            <span className={css.fieldLabel}>rolling</span>
            <input className={css.date} style={{ width: 72 }} value={windowDraft} onChange={(e) => setWindowDraft(e.target.value)} aria-label="Trailing window" />
          </label>
        </HTip>
        <button type="submit" className={css.btn}>apply</button>
        <HTip content="Box weights every bin equally; triangle gives recent bins more weight, preserving the total window mass.">
          <select className={css.select} value={tl.kernel} onChange={(e) => tl.setKernel(e.target.value as 'box' | 'tri')} aria-label="Window kernel">
            <option value="box">box</option>
            <option value="tri">triangle</option>
          </select>
        </HTip>
        {windowError && <span role="alert" className={css.note}>{windowError}</span>}
      </form>}
      <form className={css.group} onSubmit={(e) => {
        e.preventDefault()
        const value = Number(fpsDraft)
        if (!Number.isFinite(value) || value < .25 || value > 60) { setLinkedError('FPS must be between 0.25 and 60'); return }
        try { setSp(value); setLinkedError(null) } catch (err) { setLinkedError((err as Error).message) }
      }}>
        <HTip content={`Playback: ${speedLabel(bin, sp)}. ${tl.derived.has('f') ? 'Derived from the other controls; edit to make fps user-set.' : 'Editing fps derives the least recently edited control.'}`}>
          <label className={css.field} style={{ fontStyle: tl.derived.has('f') ? 'italic' : undefined }}>
            <span className={css.fieldLabel}>fps</span>
            <input className={css.date} style={{ width: 70 }} type="number" min="0.25" max="60" step="any" value={fpsDraft} onFocus={() => tl.touch('f')} onChange={(e) => setFpsDraft(e.target.value)} aria-label="Frames per second" />
          </label>
        </HTip>
        <button type="submit" className={css.btn}>apply</button>
      </form>
      <form className={css.group} onSubmit={(e) => {
        e.preventDefault()
        const value = parseLength(lengthDraft)
        if (value === null) { setLinkedError('Length must be at least 1 second (m:ss, seconds, or 1m30s)'); return }
        try { tl.setLength(value); setLinkedError(null) } catch (err) { setLinkedError((err as Error).message) }
      }}>
        <HTip content={`Movie length = frames ÷ fps. ${tl.derived.has('L') ? 'Derived from the other controls; edit to hold the length while changing bin or range.' : 'Editing length derives the least recently edited control.'}`}>
          <label className={css.field} style={{ fontStyle: tl.derived.has('L') ? 'italic' : undefined }}>
            <span className={css.fieldLabel}>length</span>
            <input className={css.date} style={{ width: 80 }} value={lengthDraft} onFocus={() => tl.touch('L')} onChange={(e) => setLengthDraft(e.target.value)} aria-label="Movie length" />
          </label>
        </HTip>
        <button type="submit" className={css.btn}>apply</button>
      </form>
      {linkedError && <span role="alert" className={css.note}>{linkedError}</span>}
      {!animation && <HTip content="Style preset (s)">
        <label className={css.field}>
          <span className={css.fieldLabel}>style</span>
          <select className={css.select} value={preset} onChange={(e) => setPreset(e.target.value as Preset)} aria-label="Style">
            {PRESETS.map((p) => <option key={p} value={p}>{PRESET_LABEL[p]}</option>)}
          </select>
        </label>
      </HTip>}
      {!animation && <HTip content="Circle size (multiplies every radius; circles also shrink when zoomed out)">
        <label className={css.field}>
          <span className={css.fieldLabel}>size</span>
          <select className={css.select} value={sz} onChange={(e) => setSz(Number(e.target.value))} aria-label="Circle size" data-testid="tl-size">
            {(SIZES.includes(sz as typeof SIZES[number]) ? [...SIZES] : [...SIZES, sz].sort((a, b) => a - b)).map((v) => (
              <option key={v} value={v}>{factorLabel(v)}</option>
            ))}
          </select>
        </label>
      </HTip>}
      <HTip content="Loop at the end of the range (l)">
        <label className={css.check}>
          <input type="checkbox" checked={loop} onChange={(e) => setLoop(e.target.checked)} /> loop
        </label>
      </HTip>
      {suggested && suggested !== bin && (
        <HTip content={`${fmt(nFramesRange)} frames at ${binLabel(bin)}: long to load and play. Switch to ${binLabel(suggested)} bins?`}>
          <button type="button" className={`${css.btn} ${css.hint}`} onClick={() => changeBin(suggested)} data-testid="tl-suggest-bin">
            {fmt(nFramesRange)} frames · use {suggested}
          </button>
        </HTip>
      )}
      {capNote && <span className={css.note} data-testid="tl-cap-note">{capNote}</span>}
      {!canHover && (
        <div className={css.keys}>
              Keys: space play · ←/→ {unit} · shift+←/→ {big.label} · [ ] speed · b / shift+b bin · s style · l loop · esc clear selection.
          {' '}Long-press a station to multi-select; long-press + drag to box-select.
        </div>
      )}
    </div>
  )
}

/** Prev / play / next, the scrubber, the playhead's date, and (phones) ⚙. */
export function TlTransport({ tl, wide, settingsOpen, onToggleSettings }: {
  tl: Timelapse
  wide: boolean
  settingsOpen: boolean
  onToggleSettings: () => void
}) {
  const { bin, iStart, iEnd, t, tRef, setT, shown, playing, toggle, step, commitT, peek, onPeek, totals } = tl
  const unit = binUnit(bin)
  const big = bigStep(bin)
  const iMs = frameStartMs(bin, floor(shown))
  return (
    <div className={css.transportRow}>
      <div className={css.transport}>
        <HTip content={`Previous ${unit} (←; shift+← = ${big.label})`}>
          <button type="button" className={css.chev} onClick={() => step(-1)} aria-label={`Previous ${unit}`}><Chevron dir="left" /></button>
        </HTip>
        <HTip content="Play / pause (space)">
          <button type="button" className={css.play} onClick={toggle} aria-label={playing ? 'Pause' : 'Play'} data-testid="tl-play">
            <PlayIcon playing={playing} />
          </button>
        </HTip>
        <HTip content={`Next ${unit} (→; shift+→ = ${big.label})`}>
          <button type="button" className={css.chev} onClick={() => step(1)} aria-label={`Next ${unit}`}><Chevron dir="right" /></button>
        </HTip>
      </div>
      <Scrubber
        bin={bin}
        iStart={iStart}
        iEnd={iEnd}
        i={floor(t)}
        peek={peek}
        totals={totals}
        onScrub={(v) => { tRef.current = v; setT(v) }}
        onCommit={commitT}
        onPeek={onPeek}
      />
      <span className={css.rangeLabel} data-testid="tl-timestamp">{formatT(iMs)}</span>
      {!wide && (
        <button type="button" className={css.gear} onClick={onToggleSettings} aria-expanded={settingsOpen} aria-label="Settings" data-testid="tl-gear">
          <GearIcon />
        </button>
      )}
    </div>
  )
}
