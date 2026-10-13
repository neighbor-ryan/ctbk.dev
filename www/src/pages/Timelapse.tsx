/**
 * `/timelapse` (`specs/timelapse-map.md`, P1–P3): every station, one bin
 * (any `/api/tl` tier, `1h` … `1mo`) per frame, on the shared `GLMap`. `ScatterplotLayer`s with
 * binary per-frame attributes in one of three presets (`st=`: `flow` =
 * diverging color on damped net share, `act` = single-hue glow, `split` =
 * disk/ring for starts/ends), all sized against one per-bin scale frozen for
 * the session. Frames are lerped in JS from a continuous playhead `t` (frame
 * index + φ) driven by one rAF loop that stalls (badge) while the next chunk
 * isn't cached; an idle fill prefetches the rest of the range behind it.
 * Selection (`sel=`; the shared `lib/mapSelection` model, as on
 * `/stations`): tap selects one station, long-press enters multi-select
 * mode, shift/⌘-click toggles, and a long-press- or shift-drag rectangle
 * adds (fed native pointer events from the map's canvas container, so
 * overlays never start a gesture); selected stations get a
 * ring and a row in the selection panel (docked right on desktop, a bottom
 * sheet on phones). Mobile-first chrome: a header strip (date + color bar,
 * tap to expand the legend) and a control bar (prev / play / next +
 * scrubber, with a ⚙ panel for range, bin, speed, style, loop and circle
 * size `sz=`; always expanded on wide screens). The scrubber's totals
 * sparkline previews a frame on hover (click commits `t`).
 * Movie mode (`mv=1`): chrome and interaction off, `window.__tl.seek(i)` for
 * frame-by-frame capture (`scrns.timelapse.json`).
 *
 * Chunks come from `query/timelapse.ts` (`/api/tl` first, interim sources
 * where it has no coverage yet); station positions/names from the static
 * assets (`stations-regional.json` + `station-luc.json`) until the
 * `tl-stations.json` sidecar exists.
 */
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { useAction } from 'use-kbd'
import { boolParam, intParam, llzParam, stringParam, useUrlState, type LLZ } from 'use-prms'
import GLMap from '../components/GLMap'
import {
  Chevron, edgeX, HoverCard, Legend, LegendBar, PIN_RGB, rgbCss, scaleNote, sparkPath, Stats, TlSettings, TlTransport, WIDE, WindowNote,
} from '../components/timelapse/controls'
import { pinIndices, timelapseLayers } from '../components/timelapse/layers'
import { useTimelapse } from '../components/timelapse/useTimelapse'
import { selParam, stationsInRect, useSelection, useSelectionGestures, type Pt, type Rect } from '../lib/mapSelection'
import { useCanHover, useMediaQuery } from '../lib/useMediaQuery'
import { ensureChunk, useTlStationSeries, type SourceMode } from '../query/timelapse'
import { clockLabel, shortLabel, binUnit } from '../query/timelapseControls'
import { ANCHORS, chunksCovering, chunkOf, formatT, formatYmd, frameStartMs, type Bin, type StationTable } from '../query/timelapseFrames'
import { radiusFactor } from '../query/timelapseSize'
import { movieGrid, movieRange } from '../query/timelapseMovie'
import { useRideAnimation } from '../query/useRideAnimation'
import { BIKE_COLORS, movingRideLayers, ridePaths, rideRouteOrigins } from '../components/timelapse/rideLayers'
import { useFlowRoutes } from '../query/flowRoutes'
import css from '../timelapse.module.css'
import type { Layer, PickingInfo } from '@deck.gl/core'
import type { MapboxOverlay } from '@deck.gl/mapbox'
import type { Map as MaplibreMapInstance } from 'maplibre-gl'

const { floor, max, round } = Math

declare global {
  interface Window {
    /** Movie-mode seek resolves after data, deck redraw, and map idle. */
    __tl?: { frames: number; fps: number; seconds: number; start: string; end: string; seek: (i: number) => Promise<void> }
  }
}

const SYSTEM_LLZ: LLZ = { lat: 40.735, lng: -73.975, zoom: 11 }
const viewParam = llzParam({ default: SYSTEM_LLZ, latLngDecimals: 3 })

export default function Timelapse() {
  const [mv] = useUrlState('mv', boolParam)
  const [anim] = useUrlState('anim', boolParam)
  const [flow, setFlow] = useUrlState('flow', stringParam())
  const routeMode = flow === 'routes'
  const [colorRaw, setColorRaw] = useUrlState('clr', stringParam())
  const [trailRaw, setTrailRaw] = useUrlState('trail', intParam(120))
  const colorBy = colorRaw === 'user' ? 'user' : 'bike'
  const trail = Math.min(600, max(5, trailRaw))
  const animationReady = useRef(false)
  const tl = useTimelapse({ mv, defaultBin: anim ? '1m' : '1d', clockReady: anim ? animationReady : undefined })
  const {
    qc, src, bin, range, iStart, iEnd, tUrl, tToI, tRef, setT, playing, peek, table, frames, shown: countShown, shownFrames, frame,
    scale, scaleFrozen, preset, sz, sp, shownWindows, windowWidth,
  } = tl
  const shown = anim ? tl.tView : countShown
  const frameMs = frameStartMs(bin, floor(shown))
  const shownMs = frameMs + (shown - floor(shown)) * (frameStartMs(bin, floor(shown) + 1) - frameMs)
  const animation = useRideAnimation(shownMs, anim)
  const bucket = floor(animation.seconds / 900) * 900
  const routeOrigins = useMemo(() => rideRouteOrigins(animation.day, bucket, trail), [animation.day, bucket, trail])
  const routeData = useFlowRoutes(anim && routeMode, routeOrigins)
  const ready = animation.ready && (!routeMode || !!routeData.data || !!routeData.error)
  animationReady.current = ready
  const midnight = floor(shownMs / 86400000) * 86400
  const paths = useMemo(() => animation.day ? ridePaths(animation.day, bucket, trail, routeMode ? routeData.data : undefined, midnight) : [], [animation.day, bucket, trail, routeMode, routeData.data, midnight])
  const moving = useMemo(() => movingRideLayers(paths, animation.seconds, trail, colorBy), [paths, animation.seconds, trail, colorBy])
  const [view, setView] = useUrlState('ll', viewParam)
  const [pins, setPins] = useUrlState('sel', selParam)
  const [fpbRaw] = useUrlState('fpb', stringParam())
  const [captureFps] = useUrlState('cfps', intParam(30))
  const [clip] = useUrlState('clip', stringParam())
  const [captureStart, captureAfter] = useMemo(() => movieRange(bin, iStart, iEnd, mv ? clip : undefined), [bin, iStart, iEnd, mv, clip])
  const movieSeries = useMemo(() => tl.totals.slice(captureStart - iStart, captureAfter - iStart), [tl.totals, captureStart, captureAfter, iStart])
  const movieTotalsReady = movieSeries.every(Number.isFinite)
  const fpb = /^\d+$/.test(fpbRaw ?? '') ? max(1, Number(fpbRaw)) : undefined
  const grid = useMemo(() => movieGrid(captureAfter - captureStart, sp, max(1, captureFps), fpb), [captureStart, captureAfter, sp, captureFps, fpb])
  const [cap] = useUrlState('cap', stringParam())
  const [tileBase] = useUrlState('tileBase', stringParam())
  const wide = useMediaQuery(WIDE)
  const canHover = useCanHover()

  // Selection: `sel=` (URL) + multi-select mode (session-local).
  const { multi, apply: applySel } = useSelection(pins, setPins)
  useAction('tl:unpin', { label: 'Clear selection', group: 'Timelapse', defaultBindings: ['escape'], handler: () => applySel({ t: 'clear' }), enabled: !mv && (pins.length > 0 || multi) })

  const pinIdx = useMemo(() => pinIndices(table, pins), [table, pins])

  // Hovered station (mouse only) + where, for the floating hover card.
  const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null)
  // Circle size: `sz` × a zoomed-out shrink, on top of the presets' radii.
  const rf = radiusFactor(sz, view.zoom)
  const circleLayers = useMemo<Layer[]>(() => (table && frame
    ? timelapseLayers({
      table,
      frame,
      preset,
      rf,
      pinIdx,
      pickable: !mv,
      onHover: canHover ? (info: PickingInfo) => setHover(info.index >= 0 ? { i: info.index, x: info.x, y: info.y } : null) : undefined,
    })
    : []), [table, frame, preset, pinIdx, mv, canHover, rf])
  const layers = anim ? moving.layers : circleLayers

  // ---- Movie mode: `window.__tl.seek(i)` -----------------------------------
  // Each seek: fetch the chunk(s) under the sampled playback clock, set `t`, then
  // wait for (1) a committed render showing exactly that `t` with its frame
  // pair ready and the scale frozen, (2) a deck redraw of those layers
  // (`onAfterRender`), (3) MapLibre idle; then set `data-tl-frame`.
  const pageRef = useRef<HTMLDivElement>(null)
  const mapRef = useRef<MaplibreMapInstance | null>(null)
  const waiters = useRef(new Set<() => void>())
  const notify = useCallback(() => { for (const w of Array.from(waiters.current)) w() }, [])
  const waitFor = useCallback((pred: () => boolean) => new Promise<void>((resolve) => {
    if (pred()) return resolve()
    const w = () => { if (pred()) { waiters.current.delete(w); resolve() } }
    waiters.current.add(w)
  }), [])
  const rendered = useRef({ t: NaN, ready: false, layers: [] as Layer[] })
  const drawn = useRef<Layer[] | null>(null)
  const layersRef = useRef(layers)
  layersRef.current = layers
  useLayoutEffect(() => {
    rendered.current = { t: shown, ready: (!mv || movieTotalsReady) && (anim ? ready : shownFrames.ready && (windowWidth === 1 || shownWindows.ready) && scaleFrozen && layers.length > 0), layers }
    notify()
  })
  const onAfterRender = useCallback(() => {
    drawn.current = layersRef.current
    notify()
  }, [notify])
  const seekChain = useRef<Promise<void>>(Promise.resolve())
  useEffect(() => {
    if (!mv) return
    const totalsReady = Promise.all(chunksCovering(bin, captureStart, captureAfter - 1).map((k) => ensureChunk(qc, src, 'start', bin, k)))
    const doSeek = async (i: number) => {
      await totalsReady
      const tt = captureStart + grid.offset(i)
      const a = floor(tt)
      const ks = Array.from(new Set([chunkOf(bin, a), chunkOf(bin, a + 1)]))
      if (!anim) await Promise.all(ks.flatMap((k) => ANCHORS.map((anchor) => ensureChunk(qc, src, anchor, bin, k))))
      tRef.current = tt
      setT(tt)
      await waitFor(() => rendered.current.t === tt && rendered.current.ready)
      await waitFor(() => drawn.current === rendered.current.layers)
      await waitFor(() => !!mapRef.current)
      const m = mapRef.current!
      if (!m.loaded() || !m.areTilesLoaded()) await new Promise((r) => m.once('idle', r))
      pageRef.current?.setAttribute('data-tl-frame', String(i))
    }
    const seek = (i: number) => {
      const p = seekChain.current.then(() => doSeek(i))
      seekChain.current = p.catch(() => {})
      return p
    }
    window.__tl = { frames: grid.frames, fps: grid.fps, seconds: grid.seconds, start: formatT(frameStartMs(bin, captureStart)), end: formatT(frameStartMs(bin, captureAfter)), seek }
    // Land on the URL's `t` so `[data-tl-frame]` appears without a caller.
    seek(Math.max(0, Math.min(grid.frames - 1, round((tToI(tUrl) - captureStart) / grid.step)))).catch((e) => console.error('__tl.seek', e))
    return () => { delete window.__tl }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mv, bin, captureStart, captureAfter, grid, src, anim])

  // ---- Selection gestures (`lib/mapSelection`) -----------------------------
  // Taps pick with deck's `pickObject` (a wider radius for fingers);
  // rectangles test projected station positions (`stationsInRect`).
  const overlayRef = useRef<MapboxOverlay | null>(null)
  const [gestureMap, setGestureMap] = useState<MaplibreMapInstance | null>(null)
  const dragRect = useSelectionGestures(gestureMap, {
    pickAt: (at: Pt, touch: boolean) => {
      const o = overlayRef.current
      if (!o || !table) return null
      const info = o.pickObject({ x: at.x, y: at.y, radius: touch ? 12 : 3 })
      return info && info.index >= 0 ? table.ids[info.index] : null
    },
    pickRect: (r: Rect) => {
      const m = gestureMap
      if (!m || !table || !frame) return []
      // Current (named) stations, plus any retired one active this frame.
      const keep = (j: number) => table.names[j] !== table.ids[j] || frame.starts[j] + frame.ends[j] > 0
      const project = (lng: number, lat: number): [number, number] => {
        const p = m.project([lng, lat])
        return [p.x, p.y]
      }
      return stationsInRect(table.positions, project, r, keep).map((j) => table.ids[j])
    },
    apply: applySel,
  }, !mv && !anim)

  // Control-bar height → `--tl-controls-h`, so the selection panel / bottom
  // sheet sits just above it.
  const controlsRef = useRef<HTMLDivElement>(null)
  useLayoutEffect(() => {
    const c = controlsRef.current
    const page = pageRef.current
    if (!c || !page) return
    const set = () => page.style.setProperty('--tl-controls-h', `${c.offsetHeight}px`)
    set()
    const ro = new ResizeObserver(set)
    ro.observe(c)
    return () => ro.disconnect()
  }, [mv])

  const [legendOpen, setLegendOpen] = useState(() => mv || window.matchMedia(WIDE).matches)
  const [settingsOpen, setSettingsOpen] = useState(false)

  const i = floor(shown)
  const stalled = playing && !(anim ? ready : frames.ready)
  const iMs = frameStartMs(bin, i + (!anim && tl.windowWidth > 1 ? 1 : 0))
  const dateStr = wide || mv ? clockLabel(bin, iMs) : shortLabel(bin, iMs)
  const nFramesRange = iEnd - iStart + 1
  const fmt = (n: number) => round(n).toLocaleString('en-US')
  const unit = binUnit(tl.windowCode)
  const note = scaleNote(scale, scaleFrozen, sz, view.zoom)
  const showSettings = wide || settingsOpen

  return (
    <div ref={pageRef} className={css.page} data-tl-frame={mv ? undefined : i} onMouseLeave={() => setHover(null)}>
      <GLMap
        layers={layers}
        center={[view.lat, view.lng]}
        zoom={view.zoom}
        onMove={mv ? undefined : (la, ln, z) => setView({ lat: la, lng: ln, zoom: z })}
        onReady={(m) => { mapRef.current = m; notify() }}
        onOverlay={(o, m) => { overlayRef.current = o; setGestureMap(m) }}
        onAfterRender={mv ? onAfterRender : undefined}
        interactive={!mv}
        preserveDrawingBuffer={mv}
        tileBase={tileBase}
        cursor={hover !== null ? 'pointer' : 'grab'}
        className={css.map}
      >
        <div className={css.header} data-testid="tl-header">
          <div className={css.headRow}>
            <span className={css.clockDate} data-testid="tl-clock">{dateStr}</span>
            {!anim && <WindowNote tl={tl} />}
            <div className={css.badges}>
              {(anim ? animation.missing : frame?.missing) && <span className={`${css.badge} ${css.badgeWarn}`} data-testid="tl-no-data">no data</span>}
              {!mv && <>
                {stalled && <span className={`${css.badge} ${css.badgeWarn}`}>buffering…</span>}
                {!(anim ? ready : frames.ready) && !playing && <span className={css.badge}>loading…</span>}
                {anim && routeMode && routeData.error && <span className={`${css.badge} ${css.badgeWarn}`}>routes unavailable · straight paths</span>}
                {(anim ? animation.error : frames.error) && <span className={`${css.badge} ${css.badgeError}`}>error: {(anim ? animation.error : frames.error)!.message}</span>}
                {!anim && frame?.source === 'synth' && <span className={`${css.badge} ${css.badgeWarn}`}>synthetic</span>}
              </>}
            </div>
            {!mv && (
              <button type="button" className={css.info} onClick={() => setLegendOpen(!legendOpen)} aria-expanded={legendOpen} aria-label={legendOpen ? 'Hide legend' : 'Show legend'}>
                {legendOpen ? '×' : 'i'}
              </button>
            )}
          </div>
          {!anim && <button type="button" className={css.barBtn} onClick={() => setLegendOpen(!legendOpen)} aria-label="Toggle legend" disabled={mv}>
            <LegendBar preset={preset} />
          </button>}
          {anim && <div className={css.legendBody}>
            <div data-testid="ride-active">{moving.active.toLocaleString('en-US')} rides in progress</div>
            {mv && colorBy === 'bike' && <div className={css.movieBikeLegend}>
              <span><i style={{ background: rgbCss(BIKE_COLORS.electric) }} /> electric</span>
              <span><i style={{ background: rgbCss(BIKE_COLORS.classic) }} /> classic</span>
            </div>}
            {legendOpen && !mv && <>
            <div className={css.clockSub}>Animation preview · {routeMode ? <><span data-testid="ride-routed">{moving.routed.toLocaleString('en-US')} active rides on inferred bike routes</span> · {routeData.nj ? 'JC/Hoboken catalog + NYC sample' : 'sample coverage'}; other rides use straight paths</> : 'inferred straight paths'} · round-trip loops · rides over 3h fade out</div>
            <div className={css.clockSub}>{colorBy === 'bike' ? 'light purple: electric · blue: classic · gray: unknown' : 'green: member · pink: casual · gray: unknown'}</div>
            {animation.month && (animation.month.invalidDuration > 0 || animation.month.unmapped > 0) && <div className={css.clockSub} data-testid="ride-exclusions">
              This month excludes {animation.month.invalidDuration.toLocaleString('en-US')} invalid-duration records and {animation.month.unmapped.toLocaleString('en-US')} rides with unmapped stations.
            </div>}
            {animation.month && !animation.month.final && <div className={css.clockSub}>Provisional month · late-ending rides may be added with the next data release.</div>}
            {!mv && <div className={css.group}>
              <label><input type="checkbox" checked={routeMode} onChange={(e) => setFlow(e.target.checked ? 'routes' : undefined)} /> Inferred bike routes</label>
              <label>Color <select className={css.select} aria-label="Ride color" value={colorBy} onChange={(e) => setColorRaw(e.target.value)}><option value="bike">Bike type</option><option value="user">User type</option></select></label>
              <label>Trail <select className={css.select} aria-label="Trail length" value={trail} onChange={(e) => setTrailRaw(Number(e.target.value))}>{[30, 60, 120, 300, 600].map((n) => <option key={n} value={n}>{n}s</option>)}</select></label>
              <Link to={`/timelapse?b=${bin}&d=${formatYmd(range[0])}-${formatYmd(range[1])}&t=${formatT(shownMs)}&len=${tl.length}&sp=${sp}&ll=${viewParam.encode(view) ?? ''}`}>Station counts</Link>
            </div>}
            </>}
          </div>}
          {!anim && legendOpen && (
            <div className={css.legendBody} data-testid="tl-legend">
              <Legend preset={preset} unit={unit} note={note} />
              <div className={css.clockSub}>
                {mv
                  ? <>{frame && !frame.missing && <>{fmt(frame.starts.reduce((a, b) => a + b, 0))} rides started</>}</>
                  : <>
                    {formatYmd(range[0])} – {formatYmd(range[1])} · {bin} bins · frame {i - iStart + 1} of {nFramesRange}
                    {peek !== null && <> · preview</>}
                    {frame && !frame.missing && <> · {fmt(frame.starts.reduce((a, b) => a + b, 0))} starts</>}
                  </>}
              </div>
              {!mv && frame && (
                <div className={css.chips}>
                  {frame.missing && <span className={css.chip}>no data: past the last published month</span>}
                  {frame.source === 'api' && <span className={css.chip}>source: rides-tl (/api/tl)</span>}
                  {frame.source === 'shard' && <span className={css.chip}>source: live pyramid shard (interim)</span>}
                  {frame.source === 'synth' && <span className={css.chip}>synthesized from monthly station totals (interim)</span>}
                  {frame.unmapped > 0 && <span className={css.chip}>{fmt(frame.unmapped)} rides at unmapped stations</span>}
                </div>
              )}
              {!mv && <Link to={`/timelapse?anim=1&b=1m&d=${formatYmd(shownMs)}-${formatYmd(shownMs)}&t=${formatT(shownMs)}&len=60&ll=${viewParam.encode(view) ?? ''}`}>Animate this day</Link>}
            </div>
          )}
        </div>
        {cap && <div className={css.caption}>{cap}</div>}
        {mv && <div className={css.movieStrip} data-testid="movie-strip">
          <div className={css.movieStripLabel}>Ride starts per {binUnit(bin)}</div>
          <svg viewBox="0 0 1000 48" preserveAspectRatio="none" aria-label="Ride starts timeline">
            <path data-testid="movie-sparkline" d={sparkPath(movieSeries, 1000, 48, 'edge')} fill="none" stroke="#7fb3ff" strokeWidth={2} vectorEffect="non-scaling-stroke" />
            <line data-testid="movie-playhead" x1={edgeX(shown - captureStart, movieSeries.length) * 1000} x2={edgeX(shown - captureStart, movieSeries.length) * 1000} y1={0} y2={48} stroke="#ffd24a" strokeWidth={3} vectorEffect="non-scaling-stroke" />
          </svg>
          <div className={css.movieStripDates}>
            <span>{clockLabel(bin, frameStartMs(bin, captureStart))}</span>
            <span>{clockLabel(bin, frameStartMs(bin, captureAfter))}</span>
          </div>
        </div>}
        {dragRect && (
          <div className={css.dragRect} style={{ left: dragRect.x0, top: dragRect.y0, width: dragRect.x1 - dragRect.x0, height: dragRect.y1 - dragRect.y0 }} />
        )}
        {!mv && !anim && canHover && table && frame && hover !== null && !dragRect && (
          <HoverCard
            x={hover.x}
            y={hover.y}
            w={pageRef.current?.clientWidth ?? 0}
            name={table.names[hover.i]}
            starts={frame.starts[hover.i]}
            ends={frame.ends[hover.i]}
            selected={pinIdx.includes(hover.i)}
            multi={multi}
          />
        )}
        {!mv && !anim && table && frame && (pinIdx.length > 0 || multi) && (
          <SelPanel
            src={src}
            bin={bin}
            table={table}
            idx={pinIdx}
            starts={frame.starts}
            ends={frame.ends}
            iStart={iStart}
            iEnd={iEnd}
            i={i}
            multi={multi}
            wide={wide}
            onRemove={(id) => applySel({ t: 'remove', id })}
            onClear={() => applySel({ t: 'clear' })}
            onDone={() => applySel({ t: 'done' })}
          />
        )}
        {!anim && !table && <div className={css.loading}>Loading stations…</div>}
        {!mv && (
          <div className={css.controls} ref={controlsRef} data-testid="tl-controls">
            {showSettings && <TlSettings tl={tl} animation={anim} />}
            <TlTransport tl={tl} wide={wide} settingsOpen={settingsOpen} onToggleSettings={() => setSettingsOpen(!settingsOpen)} />
          </div>
        )}
      </GLMap>
    </div>
  )
}

/** Selected stations: docked right (desktop) or a collapsible bottom sheet
 *  (phone), with a count, Clear, and Done while in multi-select mode. */
function SelPanel({ src, bin, table, idx, starts, ends, iStart, iEnd, i, multi, wide, onRemove, onClear, onDone }: {
  src: SourceMode
  bin: Bin
  table: StationTable
  idx: number[]
  starts: Float32Array
  ends: Float32Array
  iStart: number
  iEnd: number
  i: number
  multi: boolean
  wide: boolean
  onRemove: (id: string) => void
  onClear: () => void
  onDone: () => void
}) {
  const [collapsed, setCollapsed] = useState(false)
  const n = idx.length
  const single = n === 1 && !multi
  return (
    <div className={`${css.panel} ${collapsed && !wide ? css.panelCollapsed : ''}`} data-testid="tl-sel-panel">
      <div className={css.panelHead}>
        {!wide && (
          <button type="button" className={css.collapse} onClick={() => setCollapsed(!collapsed)} aria-expanded={!collapsed} aria-label={collapsed ? 'Expand selection' : 'Collapse selection'}>
            <Chevron dir={collapsed ? 'right' : 'left'} />
          </button>
        )}
        <span className={css.panelCount} data-testid="tl-sel-count">
          {n} selected
          {multi && <span className={css.modeTag}>multi-select</span>}
        </span>
        <span className={css.panelBtns}>
          {multi && <button type="button" className={`${css.btn} ${css.done}`} onClick={onDone} data-testid="tl-sel-done">Done</button>}
          {n > 0 && <button type="button" className={css.btn} onClick={onClear} data-testid="tl-sel-clear">Clear</button>}
        </span>
      </div>
      {multi && !collapsed && <div className={css.panelHint}>Tap stations to add / remove; long-press + drag to box-select.</div>}
      {!(collapsed && !wide) && (
        <div className={css.rows}>
          {idx.map((s) => (
            <SelRow
              key={table.ids[s]}
              src={src}
              bin={bin}
              id={table.ids[s]}
              name={table.names[s]}
              linkable={table.names[s] !== table.ids[s]}
              starts={starts[s]}
              ends={ends[s]}
              iStart={iStart}
              iEnd={iEnd}
              i={i}
              big={single}
              onRemove={() => onRemove(table.ids[s])}
            />
          ))}
        </div>
      )}
    </div>
  )
}

/** One selected station: current-frame stats + a sparkline of starts + ends
 *  over the range from whatever chunks are cached (gaps where they aren't);
 *  `big` (a lone selection): a taller sparkline and a station-page link. */
function SelRow({ src, bin, id, name, linkable, starts, ends, iStart, iEnd, i, big, onRemove }: {
  src: SourceMode
  bin: Bin
  id: string
  name: string
  linkable: boolean
  starts: number
  ends: number
  iStart: number
  iEnd: number
  i: number
  big: boolean
  onRemove: () => void
}) {
  const series = useTlStationSeries(src, bin, id, iStart, iEnd)
  const W = big ? 280 : 90
  const H = big ? 48 : 22
  const path = useMemo(() => sparkPath(series, W, H), [series, W, H])
  const n = series.length
  const x = n ? ((i - iStart + 0.5) / n) * W : 0
  const spark = (
    <svg className={big ? css.sparkBig : css.sparkMini} viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
      {path && <path d={path} fill="none" stroke={rgbCss(PIN_RGB)} strokeWidth={1.5} vectorEffect="non-scaling-stroke" />}
      <line x1={x} x2={x} y1={0} y2={H} stroke="rgba(255,255,255,0.6)" strokeWidth={1} vectorEffect="non-scaling-stroke" />
    </svg>
  )
  return (
    <div className={`${css.selRow} ${big ? css.selRowBig : ''}`} data-testid="tl-pin">
      <div className={css.selMain}>
        <span className={css.selName}>{name}</span>
        <span className={css.cardStat}><Stats starts={starts} ends={ends} /></span>
      </div>
      {!big && spark}
      <button type="button" className={css.unpin} onClick={onRemove} aria-label={`Remove ${name}`}>×</button>
      {big && spark}
      {big && linkable && <Link className={css.stationLink} to={`/s/${id}`}>Station page →</Link>}
    </div>
  )
}

