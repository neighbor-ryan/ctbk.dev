import type { MovieStore, StoredMovie } from './movie-jobs'

/** Narrow D1 shape, so this module also runs against SQLite in tests. */
type Statement = {
  bind(...values: unknown[]): Statement
  first<T>(): Promise<T | null>
  all<T>(): Promise<{ results: T[] }>
  run(): Promise<{ meta: { changes?: number } }>
}
export type MovieDatabase = { prepare(sql: string): Statement }

export function movieStore(db: MovieDatabase): MovieStore {
  return {
    async reserve(job, limits) {
      const day = job.createdAt.slice(0, 10)
      const nextDay = new Date(Date.parse(day) + 86_400_000).toISOString()
      // One statement: parallel requests cannot both pass a read-then-write quota check.
      const result = await db.prepare(`INSERT INTO movie_jobs (id, owner, state, created_at, job)
        SELECT ?, ?, ?, ?, ? WHERE
        (SELECT COUNT(*) FROM movie_jobs WHERE owner = ? AND state IN ('queued', 'rendering')) < ? AND
        (SELECT COUNT(*) FROM movie_jobs WHERE owner = ? AND created_at >= ? AND created_at < ?) < ? AND
        (SELECT COUNT(*) FROM movie_jobs WHERE state IN ('queued', 'rendering')) < ?`)
        .bind(job.id, job.owner, job.state, job.createdAt, JSON.stringify(job), job.owner, limits.activePerUser,
          job.owner, day, nextDay, limits.dailyPerUser, limits.activeTotal).run()
      return result.meta.changes === 1
    },
    async get(id) {
      const row = await db.prepare('SELECT job FROM movie_jobs WHERE id = ?').bind(id).first<{ job: string }>()
      return row ? JSON.parse(row.job) as StoredMovie : null
    },
    async active() {
      const rows = await db.prepare("SELECT job FROM movie_jobs WHERE state IN ('queued', 'rendering') ORDER BY created_at LIMIT 20").all<{ job: string }>()
      return rows.results.map((row) => JSON.parse(row.job) as StoredMovie)
    },
    async update(job) {
      await db.prepare("UPDATE movie_jobs SET state = ?, job = ? WHERE id = ? AND (state IN ('queued', 'rendering') OR state = ?)").bind(job.state, JSON.stringify(job), job.id, job.state).run()
    },
  }
}
