import { afterEach, describe, expect, it, vi } from 'vitest'
import { captureStage } from './movieCapture'

afterEach(() => vi.useRealTimers())

describe('bounded movie capture stages', () => {
  it('returns completed work and removes its deadline', async () => {
    vi.useFakeTimers()
    await expect(captureStage('Frame 610: deck redraw', async () => 610, () => ({ ready: true }))).resolves.toBe(610)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('aborts a stalled readiness wait with frame-specific diagnostics', async () => {
    vi.useFakeTimers()
    let aborted = false
    const stage = captureStage('Frame 610: committed frame data', (signal) => new Promise<void>((_, reject) => {
      signal.addEventListener('abort', () => { aborted = true; reject(new Error('aborted')) }, { once: true })
    }), () => ({ requested: 976, rendered: 976, totalsReady: false }), 1000)
    const failure = expect(stage).rejects.toThrow('Frame 610: committed frame data timed out after 1000ms: {"requested":976,"rendered":976,"totalsReady":false}')
    await vi.advanceTimersByTimeAsync(1000)
    await failure
    expect(aborted).toBe(true)
    expect(vi.getTimerCount()).toBe(0)
  })
})
