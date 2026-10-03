/**
 * `/stations?tm=1` (GL map only): the `/timelapse` machinery on the main
 * map (`specs/timelapse-map.md` "`/stations` time mode"). Station circles
 * come from `/api/tl` frames (`useTimelapse`, with the playhead in `tt`
 * since `/stations` uses `t` for its tile code), drawn by the shared
 * `timelapseLayers`; a dock above the rides panel holds the clock, legend,
 * transport + scrubber, and the ⚙ settings (always open on wide screens).
 * Selection is the page's (`sel=`, `lib/mapSelection`), so picks, the rides
 * panel and the "▶ timelapse" link keep working. Lazy-loaded: none of this
 * ships with the plain `/stations` map.
 */
import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import GLMap from './GLMap'
import { HoverCard, Legend, LegendBar, scaleNote, TlSettings, TlTransport } from './timelapse/controls'
import { pinIndices, timelapseLayers } from './timelapse/layers'
import { useTimelapse } from './timelapse/useTimelapse'
import { stationsInRect, useSelectionGestures, type Pt, type Rect, type SelAction } from '../lib/mapSelection'
import { useCanHover, useWide } from '../lib/useMediaQuery'
import { clockLabel, shortLabel, UNIT } from '../query/timelapseControls'
import { frameStartMs } from '../query/timelapseFrames'
import { radiusFactor } from '../query/timelapseSize'
import stationsCss from '../stations.module.css'
import tlCss from '../timelapse.module.css'
import css from './StationsTimeMode.module.css'
import type { Layer, PickingInfo } from '@deck.gl/core'
import type { MapboxOverlay } from '@deck.gl/mapbox'
import type { Map as MaplibreMapInstance } from 'maplibre-gl'

const { floor } = Math

export interface StationsTimeModeProps {
  /** The page's selection (`sel=`) + its reducer (`useSelection().apply`). */
  sel: readonly string[]
  multi: boolean
  onSelAction: (a: SelAction) => void
  center: [number, number]
  zoom: number
  onMove: (lat: number, lng: number, zoom: number) => void
  /** Height (px) of whatever is docked below (the rides panel); the dock sits above it. */
  bottom: number
  /** The dock's height (px), on every resize; 0 on unmount. */
  onHeight?: (px: number) => void
  onExit: () => void
}

export default function StationsTimeMode({ sel, multi, onSelAction, center, zoom, onMove, bottom, onHeight, onExit }: StationsTimeModeProps) {
  const tl = useTimelapse({ tKey: 'tt' })
  const { bin, table, frame, frames, preset, sz, shown, playing, scale, scaleFrozen, peek } = tl
  const wide = useWide()
  const canHover = useCanHover()
  const pinIdx = useMemo(() => pinIndices(table, sel), [table, sel])

  const [hover, setHover] = useState<{ i: number; x: number; y: number } | null>(null)
  const rf = radiusFactor(sz, zoom)
  const layers = useMemo<Layer[]>(() => (table && frame
    ? timelapseLayers({
      table,
      frame,
      preset,
      rf,
      pinIdx,
      pickable: true,
      onHover: canHover ? (info: PickingInfo) => setHover(info.index >= 0 ? { i: info.index, x: info.x, y: info.y } : null) : undefined,
    })
    : []), [table, frame, preset, pinIdx, canHover, rf])

  // Selection gestures, as on `/timelapse`: deck picks for taps, projected
  // positions for rectangles.
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
      const keep = (j: number) => table.names[j] !== table.ids[j] || frame.starts[j] + frame.ends[j] > 0
      const project = (lng: number, lat: number): [number, number] => {
        const p = m.project([lng, lat])
        return [p.x, p.y]
      }
      return stationsInRect(table.positions, project, r, keep).map((j) => table.ids[j])
    },
    apply: onSelAction,
  })

  const dockRef = useRef<HTMLDivElement>(null)
  const onHeightRef = useRef(onHeight)
  onHeightRef.current = onHeight
  useLayoutEffect(() => {
    const el = dockRef.current
    if (!el) return
    const report = () => onHeightRef.current?.(el.offsetHeight)
    report()
    const ro = new ResizeObserver(report)
    ro.observe(el)
    return () => { ro.disconnect(); onHeightRef.current?.(0) }
  }, [])

  const [legendOpen, setLegendOpen] = useState(false)
  const [settingsOpen, setSettingsOpen] = useState(false)
  const i = floor(shown)
  const iMs = frameStartMs(bin, i)
  const stalled = playing && !frames.ready

  return (
    <>
      <GLMap
        layers={layers}
        center={center}
        zoom={zoom}
        onMove={onMove}
        onOverlay={(o, m) => { overlayRef.current = o; setGestureMap(m) }}
        cursor={hover !== null ? 'pointer' : 'grab'}
        className={stationsCss.homeMap}
      >
        {dragRect && (
          <div className={stationsCss.dragRect} style={{ left: dragRect.x0, top: dragRect.y0, width: dragRect.x1 - dragRect.x0, height: dragRect.y1 - dragRect.y0 }} />
        )}
        {canHover && table && frame && hover !== null && !dragRect && (
          <HoverCard
            x={hover.x}
            y={hover.y}
            w={window.innerWidth}
            name={table.names[hover.i]}
            starts={frame.starts[hover.i]}
            ends={frame.ends[hover.i]}
            selected={pinIdx.includes(hover.i)}
            multi={multi}
          />
        )}
      </GLMap>
      <div ref={dockRef} className={css.dock} style={{ bottom }} data-testid="tm-dock" data-tl-frame={i}>
        <div className={tlCss.headRow}>
          <span className={tlCss.clockDate} data-testid="tl-clock">{wide ? clockLabel(bin, iMs) : shortLabel(bin, iMs)}</span>
          <button type="button" className={`${tlCss.barBtn} ${css.bar}`} onClick={() => setLegendOpen(!legendOpen)} aria-expanded={legendOpen} aria-label="Toggle legend">
            <LegendBar preset={preset} />
          </button>
          <div className={tlCss.badges}>
            {frame?.missing && <span className={`${tlCss.badge} ${tlCss.badgeWarn}`} data-testid="tl-no-data">no data</span>}
            {stalled && <span className={`${tlCss.badge} ${tlCss.badgeWarn}`}>buffering…</span>}
            {!frames.ready && !playing && !frames.error && <span className={tlCss.badge}>loading…</span>}
            {frames.error && <span className={`${tlCss.badge} ${tlCss.badgeError}`}>error: {frames.error.message}</span>}
            {!table && <span className={tlCss.badge}>loading stations…</span>}
          </div>
          <button type="button" className={css.exit} onClick={onExit} aria-label="Exit time mode" data-testid="tm-exit">×</button>
        </div>
        {legendOpen && (
          <div className={tlCss.legendBody} data-testid="tl-legend">
            <Legend preset={preset} unit={UNIT[bin]} note={scaleNote(scale, scaleFrozen, sz, zoom)} />
            {peek !== null && <div className={tlCss.clockSub}>preview</div>}
          </div>
        )}
        {(wide || settingsOpen) && <TlSettings tl={tl} />}
        <TlTransport tl={tl} wide={wide} settingsOpen={settingsOpen} onToggleSettings={() => setSettingsOpen(!settingsOpen)} />
      </div>
    </>
  )
}
