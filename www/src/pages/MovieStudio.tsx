import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import { useAction } from 'use-kbd'
import { localDateTime, parseLocalDateTime, studioDefaults, studioQuery } from '../query/movieStudio'
import { formatLength, parseLength } from '../query/timelapseLinked'
import { useTheme } from '../contexts/ThemeContext'
import css from './MovieStudio.module.css'

type Plan = NonNullable<Window['__tl']>
type Job = { id: string; state: 'queued' | 'rendering' | 'complete' | 'failed' | 'cancelled'; frame: number; frames: number; error?: string; query: string }
async function json<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init)
  const value = await response.json()
  if (!response.ok) throw new Error(value.error ?? `Request failed (${response.status})`)
  return value as T
}

export default function MovieStudio() {
  const [params, setParams] = useSearchParams()
  const { actualTheme } = useTheme()
  const [initial] = useState(() => studioDefaults(params))
  const [query, setQuery] = useState(() => {
    const p = studioQuery(params, initial)
    p.set('theme', actualTheme)
    return p.toString()
  })
  const currentQuery = useRef(query)
  const [start, setStart] = useState(localDateTime(initial.start))
  const [end, setEnd] = useState(localDateTime(initial.end))
  const [length, setLength] = useState(formatLength(initial.seconds))
  const [fps, setFps] = useState(String(initial.fps))
  const [size, setSize] = useState(`${initial.width}x${initial.height}`)
  const [dimensions, setDimensions] = useState([initial.width, initial.height])
  const [error, setError] = useState<string | null>(null)
  const [plan, setPlan] = useState<Plan | null>(null)
  const [frame, setFrame] = useState(0)
  const [playing, setPlaying] = useState(false)
  const [loop, setLoop] = useState(false)
  const [busy, setBusy] = useState(false)
  const [jobId, setJobId] = useState<string | null>(params.get('job'))
  const iframe = useRef<HTMLIFrameElement>(null)
  const stage = useRef<HTMLDivElement>(null)
  const [scale, setScale] = useState(1)
  const seekChain = useRef(Promise.resolve())
  const currentFrame = useRef(0)
  const generation = useRef(0)
  const { data: capabilities } = useQuery({ queryKey: ['movie-export-capabilities'], queryFn: () => json<{ available: boolean }>('/__movies/capabilities'), retry: false, staleTime: Infinity })
  const job = useQuery({ queryKey: ['movie-export', jobId], queryFn: () => json<Job>(`/__movies/jobs/${jobId}`), enabled: !!jobId, refetchInterval: (q) => q.state.data && ['complete', 'failed', 'cancelled'].includes(q.state.data.state) ? false : 1500, retry: false })
  const exportMovie = useMutation({
    mutationFn: () => json<Job>('/__movies/jobs', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ query: currentQuery.current }) }),
    onSuccess: (j) => { setJobId(j.id); const p = new URLSearchParams(currentQuery.current); p.set('job', j.id); setParams(p, { replace: true }) },
  })
  const cancelExport = useMutation({ mutationFn: () => json<Job>(`/__movies/jobs/${jobId}`, { method: 'DELETE' }), onSuccess: () => { void job.refetch() } })
  const [width, height] = dimensions
  useLayoutEffect(() => {
    if (!stage.current) return
    const fit = () => {
      const box = stage.current!.getBoundingClientRect()
      setScale(Math.min(box.width / width, box.height / height, 1))
    }
    const observer = new ResizeObserver(fit)
    observer.observe(stage.current)
    fit()
    return () => observer.disconnect()
  }, [width, height])
  useEffect(() => {
    const receive = (event: MessageEvent) => {
      if (event.origin !== window.location.origin || event.source !== iframe.current?.contentWindow || event.data?.type !== 'ctbk-movie-view') return
      const p = new URLSearchParams(currentQuery.current)
      if (event.data.view) p.set('ll', event.data.view)
      else p.delete('ll')
      currentQuery.current = p.toString()
      if (jobId) p.set('job', jobId)
      setParams(p, { replace: true })
    }
    window.addEventListener('message', receive)
    return () => window.removeEventListener('message', receive)
  }, [setParams, jobId])
  useEffect(() => {
    let disposed = false
    const timer = setInterval(() => {
      const win = iframe.current?.contentWindow
      const api = win?.__tl
      if (!disposed && api && win.document.querySelector('[data-tl-frame]')) {
        setPlan(api)
        clearInterval(timer)
      }
    }, 200)
    return () => { disposed = true; clearInterval(timer) }
  }, [query])

  const seek = useCallback((value: number) => {
    if (!plan) return Promise.resolve()
    const target = Math.max(0, Math.min(plan.frames - 1, value))
    const token = generation.current
    const run = seekChain.current.then(async () => {
      if (token !== generation.current) return
      const pending = window.setTimeout(() => setBusy(true), 250)
      try {
        await plan.seek(target)
        if (token === generation.current) { currentFrame.current = target; setFrame(target) }
      } catch (e) {
        if (token === generation.current) { setPlaying(false); setError(e instanceof Error ? e.message : String(e)) }
      } finally { clearTimeout(pending); if (token === generation.current) setBusy(false) }
    })
    seekChain.current = run
    return run
  }, [plan])
  useEffect(() => {
    if (!playing || !plan) return
    let cancelled = false, timer = 0
    let from = currentFrame.current, began = performance.now()
    const tick = async () => {
      if (cancelled) return
      let target = from + Math.floor((performance.now() - began) * plan.fps / 1000)
      if (target >= plan.frames) {
        if (loop) { from = 0; began = performance.now(); target = 0 }
        else { await seek(plan.frames - 1); setPlaying(false); return }
      }
      if (target !== currentFrame.current) await seek(target)
      if (!cancelled) timer = window.setTimeout(() => { void tick() }, 1000 / plan.fps)
    }
    void tick()
    return () => { cancelled = true; clearTimeout(timer) }
  }, [playing, loop, plan, seek])
  const toggle = useCallback(() => {
    if (!plan) return
    if (!playing && currentFrame.current >= plan.frames - 1) { void seek(0).then(() => setPlaying(true)); return }
    setPlaying(!playing)
  }, [playing, plan, seek])
  const step = useCallback((delta: number) => { setPlaying(false); void seek(currentFrame.current + delta) }, [seek])
  useEffect(() => {
    const receiveKey = (event: MessageEvent) => {
      if (event.origin !== window.location.origin || event.source !== iframe.current?.contentWindow || event.data?.type !== 'ctbk-movie-key') return
      if (event.data.action === 'play') toggle()
      else if (event.data.action === 'prev') step(-1)
      else if (event.data.action === 'next') step(1)
    }
    window.addEventListener('message', receiveKey)
    return () => window.removeEventListener('message', receiveKey)
  }, [toggle, step])
  useAction('movie:play', { label: 'Play / pause movie preview', group: 'Movie', defaultBindings: ['space'], handler: toggle, enabled: !!plan })
  useAction('movie:prev', { label: 'Previous movie frame', group: 'Movie', defaultBindings: ['arrowleft'], handler: () => step(-1), enabled: !!plan })
  useAction('movie:next', { label: 'Next movie frame', group: 'Movie', defaultBindings: ['arrowright'], handler: () => step(1), enabled: !!plan })

  const apply = (event: React.FormEvent) => {
    event.preventDefault()
    try {
      const [w, h] = size.split('x').map(Number)
      const s = { start: parseLocalDateTime(start), end: parseLocalDateTime(end), seconds: parseLength(length) ?? NaN, fps: Number(fps), width: w, height: h }
      const p = studioQuery(new URLSearchParams(currentQuery.current), s)
      generation.current++
      seekChain.current = Promise.resolve()
      setBusy(false)
      setPlaying(false); setPlan(null); currentFrame.current = 0; setFrame(0); setError(null)
      setDimensions([w, h]); setLength(formatLength(s.seconds))
      currentQuery.current = p.toString(); setQuery(p.toString()); setParams(p, { replace: true }); setJobId(null)
    } catch (e) { setError(e instanceof Error ? e.message : String(e)) }
  }
  const previewQuery = new URLSearchParams(query)
  previewQuery.set('mv', '1'); previewQuery.set('pv', '1')
  const activeJob = job.data && ['queued', 'rendering'].includes(job.data.state)
  const applied = studioDefaults(new URLSearchParams(query))
  const dirty = start !== localDateTime(applied.start) || end !== localDateTime(applied.end) || parseLength(length) !== applied.seconds || Number(fps) !== applied.fps || size !== `${applied.width}x${applied.height}`
  const mapQuery = new URLSearchParams(currentQuery.current)
  for (const key of ['mlen', 'mw', 'mh', 'cfps', 'clip']) mapQuery.delete(key)
  return <div className={css.page}>
    <header className={css.heading}><div><h1>Movie preview</h1><p>Pan or zoom the frame. Export uses this exact view.</p></div><Link to={`/timelapse?${mapQuery}`}>Back to map</Link></header>
    <div className={css.workspace}>
      <div className={css.viewer}>
        <div className={css.stage} ref={stage}>
          <div className={css.frame} style={{ width: width * scale, height: height * scale }}>
            <iframe ref={iframe} title="Movie frame" src={`/timelapse?${previewQuery}`} width={width} height={height} style={{ transform: `scale(${scale})` }} />
          </div>
        </div>
        <div className={css.transport}>
          <button type="button" disabled={!plan} onClick={() => step(-1)} aria-label="Previous movie frame">‹</button>
          <button type="button" className={css.play} disabled={!plan} onClick={toggle} aria-label={playing ? 'Pause movie preview' : 'Play movie preview'}>{playing ? 'Ⅱ' : '▶'}</button>
          <button type="button" disabled={!plan} onClick={() => step(1)} aria-label="Next movie frame">›</button>
          <input type="range" aria-label="Movie playhead" min={0} max={(plan?.frames ?? 1) - 1} value={frame} disabled={!plan} onChange={(e) => { setPlaying(false); void seek(Number(e.target.value)) }} />
          <span className={css.time}>{formatLength(Math.floor(frame / (plan?.fps ?? 30)))} / {formatLength(plan?.seconds ?? applied.seconds)}</span>
          <label><input type="checkbox" checked={loop} onChange={(e) => setLoop(e.target.checked)} /> loop</label>
        </div>
        <div className={css.frameNote} aria-live="polite">{plan ? `${width} × ${height} · ${plan.fps} fps · frame ${frame + 1} of ${plan.frames}${busy ? ' · buffering' : ''}` : 'Loading movie frame…'}</div>
      </div>
      <aside className={css.sidebar}>
        <form onSubmit={apply}>
          <h2>Clip</h2>
          <label>Start (New York time)<input aria-label="Movie start" type="datetime-local" value={start} onChange={(e) => setStart(e.target.value)} required /></label>
          <label>End (exclusive)<input aria-label="Movie end" type="datetime-local" value={end} onChange={(e) => setEnd(e.target.value)} required /></label>
          <div className={css.pair}>
            <label>Length<input aria-label="Movie length" value={length} onChange={(e) => setLength(e.target.value)} /></label>
            <label>FPS<input aria-label="Movie FPS" type="number" min={1} max={60} value={fps} onChange={(e) => setFps(e.target.value)} /></label>
          </div>
          <label>Format<select aria-label="Movie format" value={size} onChange={(e) => setSize(e.target.value)}>
            <option value="1080x1350">Portrait · 1080 × 1350</option><option value="1080x1080">Square · 1080 × 1080</option><option value="1920x1080">Landscape · 1920 × 1080</option><option value="720x1280">Tall · 720 × 1280</option><option value="480x600">Small portrait · 480 × 600</option>
            {!['1080x1350', '1080x1080', '1920x1080', '720x1280', '480x600'].includes(size) && <option value={size}>{size.replace('x', ' × ')}</option>}
          </select></label>
          <button type="submit">Apply to preview</button>
          {error && <p role="alert" className={css.error}>{error}</p>}
        </form>
        <section className={css.export}>
          <h2>Export</h2>
          <p>{capabilities?.available ? 'Render an MP4 on this computer. You can keep editing while it renders.' : 'Open this page on localhost to render an MP4.'}</p>
          <button type="button" className={css.exportButton} disabled={!plan || dirty || !capabilities?.available || exportMovie.isPending || !!activeJob} onClick={() => exportMovie.mutate()}>{exportMovie.isPending ? 'Starting…' : 'Export MP4'}</button>
          {dirty && <p>Apply clip settings before exporting.</p>}
          {exportMovie.error && <p role="alert" className={css.error}>{exportMovie.error.message}</p>}
          {job.error && <p role="alert" className={css.error}>{job.error.message}</p>}
          {job.data && <div className={css.job} data-testid="movie-export-status">
            <span>{job.data.state === 'rendering' ? `Rendering ${job.data.frame} / ${job.data.frames} frames` : job.data.state === 'complete' ? 'MP4 ready' : job.data.state}</span>
            {activeJob && <><progress max={job.data.frames} value={job.data.frame} /><button type="button" onClick={() => cancelExport.mutate()} disabled={cancelExport.isPending}>Cancel export</button></>}
            {job.data.state === 'complete' && <a href={`/__movies/jobs/${job.data.id}/file?download=1`} download>Download MP4</a>}
            {job.data.error && <p role="alert" className={css.error}>{job.data.error}</p>}
            {cancelExport.error && <p role="alert" className={css.error}>{cancelExport.error.message}</p>}
          </div>}
        </section>
      </aside>
    </div>
  </div>
}
