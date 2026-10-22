import { useLayoutEffect, useMemo, useRef, useState } from 'react'
import { edgeX, sparkPath } from './controls'
import { movieTimeTicks } from '../../query/movieTimeline'
import css from '../../timelapse.module.css'

export function MovieTimeline({ series, start, end, frame, unit }: { series: Float64Array; start: number; end: number; frame: number; unit: string }) {
  const root = useRef<HTMLDivElement>(null)
  const [width, setWidth] = useState(1000)
  useLayoutEffect(() => {
    const node = root.current!
    const resize = () => {
      const next = node.getBoundingClientRect().width
      // A temporarily hidden preview retains its last useful tick spacing.
      if (next > 0) setWidth(next)
    }
    resize()
    const observer = new ResizeObserver(resize)
    observer.observe(node)
    return () => observer.disconnect()
  }, [])
  const ticks = useMemo(() => movieTimeTicks(start, end, width), [start, end, width])
  return <div className={css.movieStrip} data-testid="movie-strip">
    <div ref={root}>
      <div className={css.movieStripLabel}>Ride starts per {unit}</div>
      <svg viewBox="0 0 1000 48" preserveAspectRatio="none" aria-label="Ride starts timeline">
        {ticks.map((tick) => <line key={tick.ms} x1={tick.position * 1000} x2={tick.position * 1000} y1={0} y2={48} stroke="#fff" strokeOpacity={tick.midnight ? .28 : .12} strokeWidth={1} vectorEffect="non-scaling-stroke" />)}
        <path data-testid="movie-sparkline" d={sparkPath(series, 1000, 48, 'edge')} fill="none" stroke="#7fb3ff" strokeWidth={2} vectorEffect="non-scaling-stroke" />
        <line data-testid="movie-playhead" x1={edgeX(frame, series.length) * 1000} x2={edgeX(frame, series.length) * 1000} y1={0} y2={48} stroke="#ffd24a" strokeWidth={3} vectorEffect="non-scaling-stroke" />
      </svg>
      <div className={css.movieStripAxis} aria-label="Simulation time">
        {ticks.map((tick, i) => <span key={tick.ms} className={css.movieStripTick} data-testid="movie-time-tick" data-time={new Date(tick.ms).toISOString()}
          style={{ left: `${tick.position * 100}%`, transform: i === 0 ? undefined : i === ticks.length - 1 ? 'translateX(-100%)' : 'translateX(-50%)' }}>
          <span>{tick.label}</span>{tick.date && <span className={css.movieStripDay}>{tick.date}</span>}
        </span>)}
      </div>
    </div>
  </div>
}
