/** Bound each capture stage and report the exact readiness state on failure. */
export async function captureStage<T>(
  label: string,
  run: (signal: AbortSignal) => Promise<T>,
  diagnostic: () => unknown,
  timeoutMs = 120_000,
): Promise<T> {
  const controller = new AbortController()
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      run(controller.signal),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`${label} timed out after ${timeoutMs}ms: ${JSON.stringify(diagnostic())}`))
          controller.abort()
        }, timeoutMs)
      }),
    ])
  } finally {
    clearTimeout(timer)
  }
}
