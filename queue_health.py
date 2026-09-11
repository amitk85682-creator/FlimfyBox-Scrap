#!/usr/bin/env python3
"""
queue_health.py - Phase C Step 1: Queue Health, Recovery & Observability
Lightweight, read-only operational health reporter for the scraper cluster.
"""

import os
import json
import argparse
from datetime import datetime, timezone
import psycopg2

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(db_url)

def fetch_health_metrics():
    """Fetches read-only queue metrics from the database."""
    conn = get_db_connection()
    metrics = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall": {
            "pending": 0, "processing": 0, "retry_wait": 0,
            "completed": 0, "dead": 0
        },
        "per_site": {},
        "ages": {
            "oldest_pending_minutes": None,
            "oldest_retry_wait_minutes": None,
            "oldest_processing_minutes": None,
            "oldest_processing_lease_expires_minutes_ago": None,
            "expired_processing_leases_count": 0
        },
        "recovery": {
            "high_retry_jobs_total": 0,
            "high_retry_jobs_per_site": {}
        },
        "crawl_health": {}
    }

    try:
        conn.set_session(readonly=True)
        cur = conn.cursor()

        # 1. Overall Queue Status
        cur.execute("SELECT status, COUNT(*) FROM crawl_jobs GROUP BY status;")
        for status, count in cur.fetchall():
            metrics["overall"][status] = count

        # 2. Per-Site Backlog
        cur.execute("SELECT site_name, status, COUNT(*) FROM crawl_jobs GROUP BY site_name, status;")
        for site, status, count in cur.fetchall():
            if site not in metrics["per_site"]:
                metrics["per_site"][site] = {"pending": 0, "processing": 0, "retry_wait": 0, "completed": 0, "dead": 0}
            metrics["per_site"][site][status] = count

        # 3. Age Metrics & Lease Expiry
        cur.execute("""
            SELECT 
                (SELECT EXTRACT(EPOCH FROM (NOW() - created_at))/60 FROM crawl_jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1) as oldest_pending,
                (SELECT EXTRACT(EPOCH FROM (NOW() - created_at))/60 FROM crawl_jobs WHERE status = 'retry_wait' ORDER BY created_at ASC LIMIT 1) as oldest_retry,
                (SELECT EXTRACT(EPOCH FROM (NOW() - created_at))/60 FROM crawl_jobs WHERE status = 'processing' ORDER BY created_at ASC LIMIT 1) as oldest_processing,
                (SELECT EXTRACT(EPOCH FROM (NOW() - lease_expires_at))/60 FROM crawl_jobs WHERE status = 'processing' ORDER BY lease_expires_at ASC LIMIT 1) as oldest_lease_expiry_ago,
                (SELECT COUNT(*) FROM crawl_jobs WHERE status = 'processing' AND lease_expires_at < NOW()) as expired_leases
        """)
        row = cur.fetchone()
        if row:
            metrics["ages"]["oldest_pending_minutes"] = round(row[0], 2) if row[0] is not None else None
            metrics["ages"]["oldest_retry_wait_minutes"] = round(row[1], 2) if row[1] is not None else None
            metrics["ages"]["oldest_processing_minutes"] = round(row[2], 2) if row[2] is not None else None
            metrics["ages"]["oldest_processing_lease_expires_minutes_ago"] = round(row[3], 2) if row[3] is not None else None
            metrics["ages"]["expired_processing_leases_count"] = row[4]

        # 4. Recovery Indicators (High Retry Jobs >= 2)
        cur.execute("SELECT site_name, COUNT(*) FROM crawl_jobs WHERE retry_count >= 2 GROUP BY site_name;")
        total_high = 0
        for site, count in cur.fetchall():
            metrics["recovery"]["high_retry_jobs_per_site"][site] = count
            total_high += count
        metrics["recovery"]["high_retry_jobs_total"] = total_high

        # 5. Crawl/Discovery Health (from crawl_runs)
        cur.execute("""
            SELECT site_name, run_mode, started_at, status, urls_discovered, urls_processed, urls_failed, duration_secs
            FROM (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY site_name, run_mode ORDER BY started_at DESC) as rn 
                FROM crawl_runs
            ) t
            WHERE rn = 1
        """)
        for site, mode, start, status, disc, proc, fail, duration in cur.fetchall():
            if site not in metrics["crawl_health"]:
                metrics["crawl_health"][site] = {}
            metrics["crawl_health"][site][mode] = {
                "started_at": start.isoformat() if start else None,
                "status": status,
                "urls_discovered": disc,
                "urls_processed": proc,
                "urls_failed": fail,
                "duration_secs": round(duration, 2) if duration else None
            }
            
        # 6. Velocity / Throughput (Last 24 Hours)
        cur.execute("""
            SELECT 
                site_name,
                COUNT(*) FILTER (WHERE status = 'completed' AND updated_at >= NOW() - INTERVAL '24 hours') as completed_24h,
                COUNT(*) FILTER (WHERE status = 'dead' AND updated_at >= NOW() - INTERVAL '24 hours') as dead_24h,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours') as new_24h,
                AVG(EXTRACT(EPOCH FROM (updated_at - claimed_at))) FILTER (WHERE status = 'completed' AND claimed_at IS NOT NULL AND updated_at >= NOW() - INTERVAL '24 hours') as avg_duration_secs
            FROM crawl_jobs 
            WHERE updated_at >= NOW() - INTERVAL '24 hours' OR created_at >= NOW() - INTERVAL '24 hours'
            GROUP BY site_name;
        """)
        
        metrics["velocity"] = {
            "overall": {
                "completed_per_hour": 0,
                "failed_per_hour": 0,
                "avg_job_duration_secs": None,
                "backlog_growth_24h": 0,
                "estimated_drain_hours": None
            },
            "per_site": {}
        }
        
        total_completed = 0
        total_dead = 0
        total_new = 0
        sum_duration = 0
        dur_count = 0
        
        for site, comp_24h, dead_24h, new_24h, avg_dur in cur.fetchall():
            comp_24h = comp_24h or 0
            dead_24h = dead_24h or 0
            new_24h = new_24h or 0
            
            total_completed += comp_24h
            total_dead += dead_24h
            total_new += new_24h
            if avg_dur:
                sum_duration += avg_dur * comp_24h
                dur_count += comp_24h
                
            metrics["velocity"]["per_site"][site] = {
                "completed_per_hour": round(comp_24h / 24.0, 2),
                "failed_per_hour": round(dead_24h / 24.0, 2),
                "avg_job_duration_secs": round(avg_dur, 2) if avg_dur else None,
                "backlog_growth_24h": new_24h - (comp_24h + dead_24h)
            }
            
        metrics["velocity"]["overall"]["completed_per_hour"] = round(total_completed / 24.0, 2)
        metrics["velocity"]["overall"]["failed_per_hour"] = round(total_dead / 24.0, 2)
        metrics["velocity"]["overall"]["backlog_growth_24h"] = total_new - (total_completed + total_dead)
        if dur_count > 0:
            metrics["velocity"]["overall"]["avg_job_duration_secs"] = round(sum_duration / dur_count, 2)
            
        total_backlog = metrics["overall"]["pending"] + metrics["overall"]["retry_wait"]
        if total_completed > 0:
            metrics["velocity"]["overall"]["estimated_drain_hours"] = round(total_backlog / (total_completed / 24.0), 2)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return metrics

def print_human_readable(metrics):
    print("========================================")
    print("             QUEUE HEALTH               ")
    print("========================================")
    print(f"Timestamp: {metrics['timestamp']}")
    print("\n[ Overall Status ]")
    for k, v in metrics["overall"].items():
        print(f"  {k:12}: {v}")
        
    print("\n[ Velocity (Last 24h) ]")
    for k, v in metrics["velocity"]["overall"].items():
        print(f"  {k:22}: {v}")

    print("\n[ Per-Site Status & Velocity ]")
    for site, stats in metrics["per_site"].items():
        s = " ".join(f"{k}={v}" for k, v in stats.items() if v > 0 or k == 'pending')
        print(f"  {site:12}: {s}")
        if site in metrics["velocity"]["per_site"]:
            v_stats = metrics["velocity"]["per_site"][site]
            v_s = " ".join(f"{k}={v}" for k, v in v_stats.items() if v is not None)
            print(f"  {'':12}  > {v_s}")

    print("\n[ Age Metrics ]")
    for k, v in metrics["ages"].items():
        val_str = f"{v} min" if v is not None and "count" not in k else str(v)
        print(f"  {k:45}: {val_str}")

    print("\n[ Recovery Indicators ]")
    print(f"  High Retry Jobs (>=2): {metrics['recovery']['high_retry_jobs_total']}")
    if metrics["recovery"]["high_retry_jobs_per_site"]:
        for site, count in metrics["recovery"]["high_retry_jobs_per_site"].items():
            print(f"    {site:10}: {count}")

    print("\n[ Latest Crawl Runs ]")
    for site, modes in metrics["crawl_health"].items():
        print(f"  {site}:")
        for mode, data in modes.items():
            print(f"    - {mode:8}: {data['status']:7} | processed={data['urls_processed']} | failed={data['urls_failed']} | discovered={data['urls_discovered']} | dur={data['duration_secs']}s")
    print("========================================")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch operational queue health metrics.")
    parser.add_argument("--json", action="store_true", help="Output in JSON format")
    args = parser.parse_args()

    try:
        metrics = fetch_health_metrics()
        if args.json:
            print(json.dumps(metrics, indent=2))
        else:
            print_human_readable(metrics)
    except Exception as e:
        if args.json:
            print(json.dumps({"error": str(e)}))
        else:
            print(f"ERROR: {e}")
        exit(1)
