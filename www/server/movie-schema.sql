CREATE TABLE IF NOT EXISTS movie_jobs (
    id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'rendering', 'complete', 'failed', 'cancelled')),
    created_at TEXT NOT NULL,
    job TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS movie_jobs_owner_created ON movie_jobs (owner, created_at);
CREATE INDEX IF NOT EXISTS movie_jobs_state ON movie_jobs (state);
