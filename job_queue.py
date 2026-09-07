"""
=========================================================================
 job_queue.py - Phase B PostgreSQL-backed Job Queue
=========================================================================
"""

import os
import random
import socket
import threading
import time
import uuid
import logging
from typing import Optional

import psycopg2
import psycopg2.extensions
import psycopg2.errors
from psycopg2 import OperationalError

logger = logging.getLogger(__name__)

LEASE_SECONDS         = int(os.environ.get("LEASE_SECONDS", "300"))
HEARTBEAT_INTERVAL    = int(os.environ.get("HEARTBEAT_INTERVAL", "120"))
MAX_JOB_RETRIES       = int(os.environ.get("MAX_JOB_RETRIES", "3"))
QUEUE_RETENTION_DAYS  = int(os.environ.get("QUEUE_RETENTION_DAYS", "7"))
BASE_BACKOFF_MINUTES  = int(os.environ.get("BASE_BACKOFF_MINUTES", "15"))
HEARTBEAT_MAX_RETRIES = int(os.environ.get("HEARTBEAT_MAX_RETRIES", "3"))


def _make_worker_id():
    hostname = socket.gethostname()
    pid = os.getpid()
    token = uuid.uuid4().hex[:8]
    return f"{hostname}-{pid}-{token}"


WORKER_ID = _make_worker_id()

CRAWL_JOBS_DDL = """
CREATE TABLE IF NOT EXISTS crawl_jobs (
    id               BIGSERIAL PRIMARY KEY,
    site_name        TEXT NOT NULL,
    url              TEXT NOT NULL,
    job_type         TEXT NOT NULL,
    priority         INT DEFAULT 100,
    status           TEXT DEFAULT 'pending',
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    claimed_at       TIMESTAMPTZ,
    claimed_by       TEXT,
    lease_expires_at TIMESTAMPTZ,
    retry_count      INT DEFAULT 0,
    next_retry_at    TIMESTAMPTZ DEFAULT NOW(),
    last_error       TEXT
);
"""

CRAWL_JOBS_INDEXES = [
    """
    CREATE UNIQUE INDEX IF NOT EXISTS unique_active_job
    ON crawl_jobs (site_name, url, job_type)
    WHERE status NOT IN ('completed', 'dead');
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_crawl_jobs_poll
    ON crawl_jobs (priority, next_retry_at)
    WHERE status IN ('pending', 'retry_wait');
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status_updated
    ON crawl_jobs (status, updated_at);
    """,
]


def create_crawl_jobs_table(conn):
    cur = conn.cursor()
    try:
        cur.execute(CRAWL_JOBS_DDL)
        for idx_sql in CRAWL_JOBS_INDEXES:
            cur.execute(idx_sql)
        conn.commit()
    finally:
        cur.close()


def enqueue_job(conn, site_name, url, job_type="extract_movie", priority=100):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO crawl_jobs (site_name, url, job_type, priority)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (site_name, url, job_type)
                WHERE status NOT IN ('completed', 'dead')
            DO NOTHING
            RETURNING id;
            """,
            (site_name, url, job_type, priority),
        )
        result = cur.fetchone()
        conn.commit()
        return result is not None
    finally:
        cur.close()


def enqueue_jobs_bulk(conn, jobs, job_type="extract_movie", priority=100):
    if not jobs:
        return 0
    inserted = 0
    cur = conn.cursor()
    try:
        for site_name, url in jobs:
            cur.execute(
                """
                INSERT INTO crawl_jobs (site_name, url, job_type, priority)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (site_name, url, job_type)
                    WHERE status NOT IN ('completed', 'dead')
                DO NOTHING
                RETURNING id;
                """,
                (site_name, url, job_type, priority),
            )
            if cur.fetchone():
                inserted += 1
        conn.commit()
        return inserted
    finally:
        cur.close()


def enqueue_forced_reprocess(conn, site_name, url, job_type="extract_movie", priority=50):
    return enqueue_job(conn, site_name, url, job_type, priority)


def claim_next_job(conn, site_name=None, worker_id=None):
    if worker_id is None:
        worker_id = WORKER_ID
    if site_name:
        site_clause = "AND site_name = %s"
        params = [worker_id, LEASE_SECONDS, site_name]
    else:
        site_clause = ""
        params = [worker_id, LEASE_SECONDS]

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE crawl_jobs
            SET status           = 'processing',
                claimed_at       = NOW(),
                claimed_by       = %s,
                lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                updated_at       = NOW()
            WHERE id = (
                SELECT id
                FROM crawl_jobs
                WHERE status IN ('pending', 'retry_wait')
                  AND next_retry_at <= NOW()
                  {site_clause}
                ORDER BY priority ASC, next_retry_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING
                id, site_name, url, job_type, priority,
                retry_count, claimed_by, lease_expires_at;
            """,
            params,
        )
        row = cur.fetchone()
        conn.commit()
        if not row:
            return None
        return {
            "id": row[0], "site_name": row[1], "url": row[2],
            "job_type": row[3], "priority": row[4], "retry_count": row[5],
            "claimed_by": row[6], "lease_expires_at": row[7],
        }
    finally:
        cur.close()


def renew_leases(conn, job_ids, worker_id):
    if not job_ids:
        return set()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE crawl_jobs
            SET lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                updated_at       = NOW()
            WHERE id              = ANY(%s)
              AND claimed_by      = %s
              AND status          = 'processing'
              AND lease_expires_at > NOW()
            RETURNING id;
            """,
            (LEASE_SECONDS, job_ids, worker_id),
        )
        renewed = {row[0] for row in cur.fetchall()}
        conn.commit()
        return renewed
    finally:
        cur.close()


def release_job(conn, job_id, worker_id):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE crawl_jobs
            SET status           = 'retry_wait',
                next_retry_at    = NOW() + INTERVAL '30 seconds',
                claimed_by       = NULL,
                claimed_at       = NULL,
                lease_expires_at = NULL,
                updated_at       = NOW()
            WHERE id              = %s
              AND claimed_by      = %s
              AND status          = 'processing'
              AND lease_expires_at > NOW()
            RETURNING id;
            """,
            (job_id, worker_id),
        )
        result = cur.fetchone()
        conn.commit()
        return result is not None
    finally:
        cur.close()


def validate_ownership_for_write(cur, job_id, worker_id):
    try:
        cur.execute(
            """
            SELECT id
            FROM crawl_jobs
            WHERE id              = %s
              AND claimed_by      = %s
              AND status          = 'processing'
              AND lease_expires_at > NOW()
            FOR UPDATE NOWAIT;
            """,
            (job_id, worker_id),
        )
        return cur.fetchone() is not None
    except psycopg2.errors.LockNotAvailable:
        return False


def ack_job(cur, job_id, worker_id):
    cur.execute(
        """
        UPDATE crawl_jobs
        SET status           = 'completed',
            claimed_by       = NULL,
            lease_expires_at = NULL,
            updated_at       = NOW()
        WHERE id              = %s
          AND claimed_by      = %s
          AND status          = 'processing'
          AND lease_expires_at > NOW()
        RETURNING id;
        """,
        (job_id, worker_id),
    )
    return cur.fetchone() is not None


PERMANENT_ERROR_SIGNALS = (
    "404", "not found", "dmca", "removed", "taken down",
    "page not found", "410",
)


def is_permanent_failure(error_message):
    msg_lower = str(error_message).lower()
    return any(sig in msg_lower for sig in PERMANENT_ERROR_SIGNALS)


def fail_job(conn, job_id, worker_id, error_message):
    permanent = is_permanent_failure(error_message)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE crawl_jobs
            SET retry_count      = retry_count + 1,
                last_error       = %s,
                claimed_by       = NULL,
                claimed_at       = NULL,
                lease_expires_at = NULL,
                updated_at       = NOW(),
                status           = CASE
                    WHEN %s OR (retry_count + 1) >= %s THEN 'dead'
                    ELSE 'retry_wait'
                END,
                next_retry_at    = CASE
                    WHEN %s OR (retry_count + 1) >= %s THEN NULL
                    ELSE NOW() + (
                        GREATEST(60,
                            (POWER(2, retry_count + 1) * %s) + %s
                        ) * INTERVAL '1 second'
                    )
                END
            WHERE id           = %s
              AND claimed_by   = %s
              AND status       = 'processing'
            RETURNING status;
            """,
            (
                str(error_message)[:2000],
                permanent, MAX_JOB_RETRIES,
                permanent, MAX_JOB_RETRIES,
                BASE_BACKOFF_MINUTES * 60,
                random.randint(-60, 60),
                job_id, worker_id,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else "dead"
    finally:
        cur.close()


def recover_expired_leases(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE crawl_jobs
            SET status           = CASE
                                       WHEN (retry_count + 1) >= %s THEN 'dead'
                                       ELSE 'retry_wait'
                                   END,
                retry_count      = retry_count + 1,
                next_retry_at    = CASE
                                       WHEN (retry_count + 1) >= %s THEN NULL
                                       ELSE NOW() + INTERVAL '15 minutes'
                                   END,
                last_error       = 'Lease expired: worker crash/hang',
                claimed_by       = NULL,
                claimed_at       = NULL,
                lease_expires_at = NULL,
                updated_at       = NOW()
            WHERE status           = 'processing'
              AND lease_expires_at < NOW()
            RETURNING id;
            """,
            (MAX_JOB_RETRIES, MAX_JOB_RETRIES),
        )
        recovered_ids = [r[0] for r in cur.fetchall()]
        conn.commit()
        if recovered_ids:
            logger.info("Sweeper recovered %d expired jobs: %s", len(recovered_ids), recovered_ids)
        return len(recovered_ids)
    finally:
        cur.close()


def cleanup_old_jobs(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            """
            DELETE FROM crawl_jobs
            WHERE status    IN ('completed', 'dead')
              AND updated_at < NOW() - (%s * INTERVAL '1 day')
            RETURNING id;
            """,
            (QUEUE_RETENTION_DAYS,),
        )
        deleted = cur.rowcount
        conn.commit()
        if deleted:
            logger.info("Cleanup removed %d old queue jobs.", deleted)
        return deleted
    finally:
        cur.close()


def get_queue_stats(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT status, COUNT(*) FROM crawl_jobs GROUP BY status;")
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        cur.close()


class HeartbeatManager:
    """
    One thread per worker process.
    Tracks all currently active job IDs and batch-renews their leases
    every HEARTBEAT_INTERVAL seconds using a DEDICATED DB connection.

    Correctness: renew_leases() enforces lease_expires_at > NOW().
    If renewal returns a subset of registered IDs, the missing IDs
    have lost ownership and are added to _lost so workers can abort.
    """

    def __init__(self, get_conn_fn, release_conn_fn, worker_id=None):
        self._get_conn     = get_conn_fn
        self._release_conn = release_conn_fn
        self._worker_id    = worker_id or WORKER_ID
        self._lock         = threading.Lock()
        self._active       = set()
        self._lost         = set()
        self._stop_event   = threading.Event()
        self._thread       = None
        self._conn         = None

    def start(self):
        self._conn = self._get_conn()
        self._thread = threading.Thread(
            target=self._loop, name="HeartbeatManager", daemon=True
        )
        self._thread.start()
        logger.info(
            "HeartbeatManager started (worker_id=%s, interval=%ds, lease=%ds)",
            self._worker_id, HEARTBEAT_INTERVAL, LEASE_SECONDS,
        )

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=HEARTBEAT_INTERVAL + 10)
        if self._conn:
            try:
                self._release_conn(self._conn)
            except Exception:
                pass
            self._conn = None
        logger.info("HeartbeatManager stopped.")

    def register(self, job_id):
        with self._lock:
            self._active.add(job_id)
            self._lost.discard(job_id)

    def unregister(self, job_id):
        with self._lock:
            self._active.discard(job_id)
            self._lost.discard(job_id)

    def is_lost(self, job_id):
        with self._lock:
            return job_id in self._lost

    def _loop(self):
        while not self._stop_event.wait(HEARTBEAT_INTERVAL):
            with self._lock:
                job_ids = list(self._active)
            if not job_ids:
                continue
            self._renew_with_retry(job_ids)

    def _renew_with_retry(self, job_ids):
        for attempt in range(HEARTBEAT_MAX_RETRIES + 1):
            try:
                renewed = renew_leases(self._conn, job_ids, self._worker_id)
                lost = set(job_ids) - renewed
                if lost:
                    with self._lock:
                        self._lost.update(lost)
                        self._active -= lost
                    logger.warning(
                        "HeartbeatManager: lost ownership of jobs %s (sweeper reclaimed).",
                        lost,
                    )
                return
            except OperationalError as exc:
                logger.warning(
                    "Heartbeat DB error (attempt %d/%d): %s",
                    attempt + 1, HEARTBEAT_MAX_RETRIES + 1, exc,
                )
                if attempt < HEARTBEAT_MAX_RETRIES:
                    time.sleep(30)
                    try:
                        self._release_conn(self._conn)
                        self._conn = self._get_conn()
                    except Exception:
                        pass
                else:
                    logger.error(
                        "HeartbeatManager: exhausted retries. Marking all active jobs lost."
                    )
                    with self._lock:
                        self._lost.update(self._active)
                        self._active.clear()
