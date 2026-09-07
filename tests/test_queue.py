"""
tests/test_queue.py - Phase B Integration Tests
Run with: pytest tests/test_queue.py -v
Requires: DATABASE_URL env var pointing to the Supabase PostgreSQL instance.
"""
import os
import random
import threading
import time
import pytest
import psycopg2

# Add parent to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from job_queue import (
    create_crawl_jobs_table,
    enqueue_job,
    enqueue_jobs_bulk,
    claim_next_job,
    renew_leases,
    release_job,
    validate_ownership_for_write,
    ack_job,
    fail_job,
    recover_expired_leases,
    cleanup_old_jobs,
    get_queue_stats,
    WORKER_ID,
    LEASE_SECONDS,
    MAX_JOB_RETRIES,
)

DATABASE_URL = os.environ.get("DATABASE_URL")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="DATABASE_URL not set")

TEST_SITE = "test_site"
TEST_JOB_TYPE = "test_job"


@pytest.fixture(scope="session")
def db_conn():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    conn.autocommit = True
    create_crawl_jobs_table(conn)
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def cleanup(db_conn):
    """Remove test jobs before and after each test."""
    cur = db_conn.cursor()
    cur.execute("DELETE FROM crawl_jobs WHERE site_name = %s;", (TEST_SITE,))
    yield
    cur.execute("DELETE FROM crawl_jobs WHERE site_name = %s;", (TEST_SITE,))
    cur.close()


def make_url():
    return f"https://test.example.com/movie/{random.randint(100000, 999999)}"


# =======================================================================
# Test 1: Expired lease heartbeat -> must return 0 rows
# =======================================================================
def test_1_expired_lease_heartbeat(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    # Force lease into the past
    cur = db_conn.cursor()
    cur.execute(
        "UPDATE crawl_jobs SET lease_expires_at = NOW() - INTERVAL '10 minutes' WHERE id = %s;",
        (job["id"],),
    )
    cur.close()

    renewed = renew_leases(db_conn, [job["id"]], job["claimed_by"])
    assert job["id"] not in renewed, "Expired lease must not be renewed"
    print("PASS: Test 1 - expired lease heartbeat correctly returns 0 rows")


# =======================================================================
# Test 2: Expired lease final fencing -> write must fail
# =======================================================================
def test_2_expired_lease_fencing(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    # Force lease into the past
    cur = db_conn.cursor()
    cur.execute(
        "UPDATE crawl_jobs SET lease_expires_at = NOW() - INTERVAL '10 minutes' WHERE id = %s;",
        (job["id"],),
    )
    cur.close()

    # Attempt fencing check
    write_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    write_conn.autocommit = False
    try:
        wc = write_conn.cursor()
        owns = validate_ownership_for_write(wc, job["id"], job["claimed_by"])
        assert not owns, "Fencing check must fail for expired lease"
        write_conn.rollback()
    finally:
        write_conn.close()
    print("PASS: Test 2 - expired lease fencing correctly fails")


# =======================================================================
# Test 3: Sweeper reclaims expired job
# =======================================================================
def test_3_sweeper_reclaims_expired_job(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    # Force lease into the past
    cur = db_conn.cursor()
    cur.execute(
        "UPDATE crawl_jobs SET lease_expires_at = NOW() - INTERVAL '10 minutes' WHERE id = %s;",
        (job["id"],),
    )
    cur.close()

    # Run sweeper
    recovered = recover_expired_leases(db_conn)
    assert recovered >= 1

    # Heartbeat should now fail
    renewed = renew_leases(db_conn, [job["id"]], job["claimed_by"])
    assert job["id"] not in renewed, "Worker must not regain ownership after sweeper"

    # Job should be retry_wait or dead
    cur2 = db_conn.cursor()
    cur2.execute("SELECT status, claimed_by FROM crawl_jobs WHERE id = %s;", (job["id"],))
    row = cur2.fetchone()
    cur2.close()
    assert row[0] in ("retry_wait", "dead"), f"Expected retry_wait/dead, got {row[0]}"
    assert row[1] is None, "claimed_by must be cleared by sweeper"
    print(f"PASS: Test 3 - sweeper reclaimed job (status={row[0]})")


# =======================================================================
# Test 4: Valid lease - heartbeat and fencing both succeed
# =======================================================================
def test_4_valid_lease_succeeds(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    # Heartbeat should succeed
    renewed = renew_leases(db_conn, [job["id"]], job["claimed_by"])
    assert job["id"] in renewed, "Valid lease heartbeat must succeed"

    # Fencing should succeed
    write_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    write_conn.autocommit = False
    try:
        wc = write_conn.cursor()
        owns = validate_ownership_for_write(wc, job["id"], job["claimed_by"])
        assert owns, "Valid lease fencing must succeed"
        acked = ack_job(wc, job["id"], job["claimed_by"])
        assert acked, "ACK must succeed with valid lease"
        write_conn.commit()
    finally:
        write_conn.close()
    print("PASS: Test 4 - valid lease - heartbeat and fencing both succeed")


# =======================================================================
# Test 5: Worker A loses lease, B claims, A tries to commit -> must fail
# =======================================================================
def test_5_zombie_worker_cannot_commit(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)

    # Worker A claims
    job_a = claim_next_job(db_conn, TEST_SITE, worker_id="worker_A")
    assert job_a is not None

    # Force lease expiry for Worker A
    cur = db_conn.cursor()
    cur.execute(
        "UPDATE crawl_jobs SET lease_expires_at = NOW() - INTERVAL '10 minutes' WHERE id = %s;",
        (job_a["id"],),
    )
    cur.close()

    # Sweeper reclaims
    recover_expired_leases(db_conn)

    # Worker B claims
    job_b = claim_next_job(db_conn, TEST_SITE, worker_id="worker_B")
    assert job_b is not None
    assert job_b["id"] == job_a["id"], "Must be the same job"
    assert job_b["claimed_by"] == "worker_B"

    # Worker A tries to heartbeat -> must fail
    renewed = renew_leases(db_conn, [job_a["id"]], "worker_A")
    assert job_a["id"] not in renewed, "Worker A must NOT be able to renew after B claimed"

    # Worker A tries to commit -> fencing must reject it
    write_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    write_conn.autocommit = False
    try:
        wc = write_conn.cursor()
        owns = validate_ownership_for_write(wc, job_a["id"], "worker_A")
        assert not owns, "Worker A fencing must fail after B claimed"
        write_conn.rollback()
    finally:
        write_conn.close()
    print("PASS: Test 5 - zombie worker (A) correctly blocked after B claimed")


# =======================================================================
# Test 6: Simultaneous claim - only one worker wins
# =======================================================================
def test_6_simultaneous_claim(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)

    results = []

    def try_claim(worker_name):
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        conn.autocommit = True
        try:
            job = claim_next_job(conn, TEST_SITE, worker_id=worker_name)
            results.append((worker_name, job))
        finally:
            conn.close()

    threads = [threading.Thread(target=try_claim, args=(f"w{i}",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed = [r for r in results if r[1] is not None]
    assert len(claimed) == 1, f"Only 1 worker must claim the job, got {len(claimed)}"
    print(f"PASS: Test 6 - only {claimed[0][0]} claimed the job among 5 concurrent workers")


# =======================================================================
# Test 7: Transaction rollback on exception
# =======================================================================
def test_7_transaction_rollback(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    write_conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    write_conn.autocommit = False
    try:
        wc = write_conn.cursor()
        owns = validate_ownership_for_write(wc, job["id"], job["claimed_by"])
        assert owns
        # Simulate crash mid-write
        raise RuntimeError("Simulated crash during write")
    except RuntimeError:
        write_conn.rollback()
    finally:
        write_conn.close()

    # Job must still be 'processing' (not completed/corrupted)
    cur = db_conn.cursor()
    cur.execute("SELECT status FROM crawl_jobs WHERE id = %s;", (job["id"],))
    status = cur.fetchone()[0]
    cur.close()
    assert status == "processing", f"After rollback, job must remain 'processing', got {status}"
    print("PASS: Test 7 - transaction rolled back correctly, job still processing")


# =======================================================================
# Test 8: Duplicate enqueue -> only one active job
# =======================================================================
def test_10_duplicate_discovery(db_conn):
    url = make_url()

    results = []

    def try_enqueue():
        conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
        conn.autocommit = True
        try:
            inserted = enqueue_job(conn, TEST_SITE, url, TEST_JOB_TYPE)
            results.append(inserted)
        finally:
            conn.close()

    threads = [threading.Thread(target=try_enqueue) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    cur = db_conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM crawl_jobs WHERE site_name = %s AND url = %s;",
        (TEST_SITE, url),
    )
    count = cur.fetchone()[0]
    cur.close()

    assert count == 1, f"Duplicate enqueue must create only 1 active job, got {count}"
    print(f"PASS: Test 10 - duplicate discovery correctly deduplicated ({sum(results)} inserted / {count} in DB)")


# =======================================================================
# Test 9: fail_job increments retry_count and transitions status
# =======================================================================
def test_retry_count_increments(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    new_status = fail_job(db_conn, job["id"], job["claimed_by"], "Temporary error")
    assert new_status == "retry_wait", f"Expected retry_wait, got {new_status}"

    cur = db_conn.cursor()
    cur.execute(
        "SELECT retry_count, claimed_by, status FROM crawl_jobs WHERE id = %s;",
        (job["id"],),
    )
    row = cur.fetchone()
    cur.close()
    assert row[0] == 1, f"retry_count must be 1 after first failure, got {row[0]}"
    assert row[1] is None, "claimed_by must be cleared after fail"
    print(f"PASS: Test 9 - retry_count={row[0]}, status={row[2]}, claimed_by cleared")


# =======================================================================
# Test: Permanent failure -> dead immediately
# =======================================================================
def test_permanent_failure_goes_dead(db_conn):
    url = make_url()
    enqueue_job(db_conn, TEST_SITE, url, TEST_JOB_TYPE)
    job = claim_next_job(db_conn, TEST_SITE)
    assert job is not None

    new_status = fail_job(db_conn, job["id"], job["claimed_by"], "404 not found")
    assert new_status == "dead", f"Expected dead for 404, got {new_status}"
    print("PASS: Permanent failure (404) goes directly to dead")
