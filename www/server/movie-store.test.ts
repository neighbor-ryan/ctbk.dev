import { DatabaseSync } from 'node:sqlite'
import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { movieStore, type MovieDatabase } from './movie-store'
import { MOVIE_LIMITS, type StoredMovie } from './movie-jobs'

describe('atomic movie quotas on SQLite', () => {
  it('accounts for active per-user/global limits and independent UTC daily allowances', async () => {
    const db = new DatabaseSync(':memory:')
    db.exec(readFileSync(new URL('./movie-schema.sql', import.meta.url), 'utf8'))
    const adapter: MovieDatabase = { prepare(sql) { let values: unknown[] = []; return {
      bind(...args) { values = args; return this },
      async first<T>() { return (db.prepare(sql).get(...values as string[]) ?? null) as T | null },
      async all<T>() { return { results: db.prepare(sql).all(...values as string[]) as T[] } },
      async run() { return { meta: { changes: Number(db.prepare(sql).run(...values as string[]).changes) } } },
    } } }
    const store = movieStore(adapter)
    const job: StoredMovie = { id: '1', owner: 'viewer@example.test', state: 'queued', frame: 0, frames: 900, width: 1080, height: 1350, fps: 30, seconds: 30, query: 'preview', createdAt: '2026-10-20T12:00:00Z' }
    expect(await store.reserve(job, MOVIE_LIMITS)).toBe(true)
    expect(await store.reserve({ ...job, id: '2' }, MOVIE_LIMITS)).toBe(true)
    expect(await store.active()).toEqual([job, { ...job, id: '2' }])
    expect(await store.reserve({ ...job, id: '3' }, MOVIE_LIMITS)).toBe(false)
    expect(await store.get('1')).toEqual(job)
    await store.update({ ...job, state: 'complete' })
    expect(await store.reserve({ ...job, id: '3' }, MOVIE_LIMITS)).toBe(true)
    expect(await store.reserve({ ...job, id: 'other', owner: 'other@example.test' }, { ...MOVIE_LIMITS, activeTotal: 2 })).toBe(false)
    db.exec("UPDATE movie_jobs SET state = 'complete'")
    expect(await store.reserve({ ...job, id: '4' }, { ...MOVIE_LIMITS, dailyPerUser: 3 })).toBe(false)
    expect(await store.reserve({ ...job, id: '5', createdAt: '2026-10-21T00:00:00Z' }, { ...MOVIE_LIMITS, dailyPerUser: 3 })).toBe(true)
    await store.update({ ...job, id: '5', state: 'cancelled' })
    await store.update({ ...job, id: '5', state: 'rendering' })
    expect((await store.get('5'))?.state).toBe('cancelled')
    db.close()
  })
})
