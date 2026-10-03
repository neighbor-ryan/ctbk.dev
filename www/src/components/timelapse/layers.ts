/**
 * Deck layers for one timelapse frame (`useTimelapse().frame`): the preset's
 * `ScatterplotLayer`s (`flow` / `act`: one layer, `split`: disk + ring) over
 * binary per-station attributes, plus a ring around each selected station.
 * Shared by `/timelapse` and the `/stations` time mode.
 */
import { ScatterplotLayer } from '@deck.gl/layers'
import { PIN_RGB } from './controls'
import { COOL, NEUTRAL, SIZE, SPLIT, WARM, type Preset, type StationTable } from '../../query/timelapseFrames'
import type { Timelapse } from './useTimelapse'
import type { Layer, PickingInfo } from '@deck.gl/core'

const { max } = Math

/** Additive blending (`act`): overlapping glows sum toward white. */
const ADDITIVE = {
  blend: true,
  blendColorOperation: 'add',
  blendColorSrcFactor: 'src-alpha',
  blendColorDstFactor: 'one',
  blendAlphaOperation: 'add',
  blendAlphaSrcFactor: 'one',
  blendAlphaDstFactor: 'one-minus-src-alpha',
} as const

export type TlFrame = NonNullable<Timelapse['frame']>

export function timelapseLayers({ table, frame, preset, rf, pinIdx, pickable, onHover }: {
  table: StationTable
  frame: TlFrame
  preset: Preset
  /** Radius factor (`radiusFactor(sz, zoom)`). */
  rf: number
  /** Table indices of the selected stations (ringed). */
  pinIdx: readonly number[]
  pickable: boolean
  onHover?: (info: PickingInfo) => void
}): Layer[] {
  const n = table.ids.length
  // Picking only; selection is `useSelectionGestures` → `pickObject`.
  const pick = { pickable, onHover }
  const sized = { radiusScale: rf, radiusMaxPixels: SIZE.rMax * rf }
  const position = { value: table.positions, size: 2 }
  const out: Layer[] = []
  if (frame.flow || frame.act) {
    const { radius, color } = (frame.flow ?? frame.act)!
    out.push(new ScatterplotLayer({
      id: `tl-${preset}`,
      data: { length: n, attributes: { getPosition: position, getRadius: { value: radius, size: 1 }, getFillColor: { value: color, size: 4 } } },
      radiusUnits: 'pixels',
      radiusMinPixels: 1,
      ...sized,
      parameters: frame.act ? ADDITIVE : undefined,
      ...pick,
    }))
  }
  if (frame.split) {
    const { rStart, rEnd } = frame.split
    // Idle stations keep a grey disk (the skeleton); active ones warm.
    const disk = new Uint8Array(n * 4)
    for (let i = 0; i < n; i++) {
      const idle = !(frame.starts[i] + frame.ends[i] > 0)
      const [r, g, b] = idle ? NEUTRAL : WARM
      disk.set([r, g, b, idle ? 70 : SPLIT.alpha], 4 * i)
    }
    out.push(
      new ScatterplotLayer({
        id: 'tl-split-starts',
        data: { length: n, attributes: { getPosition: position, getRadius: { value: rStart, size: 1 }, getFillColor: { value: disk, size: 4 } } },
        radiusUnits: 'pixels',
        ...sized,
        ...pick,
      }),
      new ScatterplotLayer({
        id: 'tl-split-ends',
        data: { length: n, attributes: { getPosition: position, getRadius: { value: rEnd, size: 1 } } },
        radiusUnits: 'pixels',
        ...sized,
        filled: false,
        stroked: true,
        lineWidthUnits: 'pixels',
        getLineWidth: SPLIT.ringWidth,
        getLineColor: [...COOL, 230],
        pickable: false,
      }),
    )
  }
  if (pinIdx.length) {
    const r = (i: number) => {
      if (frame.flow) return frame.flow.radius[i]
      if (frame.act) return frame.act.radius[i]
      return max(frame.split!.rStart[i], frame.split!.rEnd[i], SIZE.rIdle)
    }
    out.push(new ScatterplotLayer<number>({
      id: 'tl-pins',
      data: pinIdx as number[],
      getPosition: (i) => [table.positions[2 * i], table.positions[2 * i + 1]],
      getRadius: (i) => r(i) * rf + 3,
      radiusUnits: 'pixels',
      filled: false,
      stroked: true,
      lineWidthUnits: 'pixels',
      getLineWidth: 2,
      getLineColor: [...PIN_RGB, 255],
      updateTriggers: { getRadius: [frame, rf] },
      pickable: false,
    }))
  }
  return out
}

/** Table indices of `ids` (unknown ids dropped), in `ids` order. */
export function pinIndices(table: StationTable | null, ids: readonly string[]): number[] {
  if (!table) return []
  return ids.map((id) => table.index.get(id)).filter((i): i is number => i !== undefined)
}
