#!/usr/bin/env python3
"""
=========================================================================
 main.py — Universal Multi-Site Scraping Engine
=========================================================================
 The single orchestrator for ALL site plugins. This file handles:
   • CLI argument parsing  (--site, --mode, --bot_id, --total_bots)
   • Dynamic plugin loading via importlib
   • Playwright browser lifecycle with anti-bot stealth
   • TMDB API enrichment   (movies + TV series with season data)
   • PostgreSQL upserts    (SELECT-first + INSERT/UPDATE, crash-safe)
   • Matrix mode           (historical bulk scraping, work-split across bots)
   • Watchdog mode         (daily top-N sync with smart deduplication)

 Usage:
   python main.py --site filmyzilla --mode matrix --bot_id 1 --total_bots 5
   python main.py --site hdhub4u   --mode watchdog
=========================================================================
"""

import asyncio
import logging
import os
import re
import sys
import time
import json
import argparse
import importlib
import urllib.parse
import threading

import hashlib
import signal
import requests
import psycopg2
from psycopg2 import pool as psycopg2_pool
from psycopg2.extras import execute_values
import nest_asyncio
from playwright.async_api import async_playwright

import job_queue
from job_queue import (
    HeartbeatManager,
    WORKER_ID,
    claim_next_job,
    enqueue_jobs_bulk,
    enqueue_forced_reprocess,
    validate_ownership_for_write,
    ack_job,
    fail_job,
    release_job,
    recover_expired_leases,
    cleanup_old_jobs,
    get_queue_stats,
)

nest_asyncio.apply()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


# =====================================================================
# CONFIGURATION — All secrets via environment variables
# =====================================================================
DATABASE_URL  = os.environ.get("DATABASE_URL", "")
TMDB_API_KEY  = os.environ.get("TMDB_API_KEY", "")
FORCE_REFRESH = os.environ.get("FORCE_REFRESH", "false").lower() == "true"

# GitHub Actions has a 6-hour limit; stop gracefully well before that
MAX_RUN_TIME_SECONDS = (5 * 3600) + (45 * 60)   # 5 h 45 min

# Max simultaneous movie-scraping coroutines
CONCURRENCY_LIMIT = 10

# How many URLs to fire with asyncio.gather at once in matrix mode
BATCH_SIZE = 50

# Shared User-Agent string
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── Connection pool size ─────────────────────────────────────────────
# Per-process pool. Default=3 is safe for the current GitHub Actions
# matrix of up to 15 parallel processes (3 sites × 5 bots).
#
# Connection budget (worst case, GitHub Actions matrix mode):
#   15 processes × DB_POOL_SIZE(3) = 45 scraper connections
#   + ~15 Supabase infrastructure connections
#   = ~60  (matches Supavisor hard limit of 60)
#
# For a single VPS worker: set DB_POOL_SIZE=10 or higher.
# NEVER increase this without also reducing matrix concurrency.
DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "3"))

# =====================================================================
# SITE CONFIGURATION
# =====================================================================
DEFAULT_SITE_CONFIG = {
    "enabled": True,
    "max_active": 2,
}

SITE_CONFIG = {
    "filmyzilla": {
        "max_active": 2,
    },
    "hdhub4u": {
        "max_active": 2,
    },
    "mkvcinemas": {
        "enabled": False,
        "max_active": 2,
    }
}

def get_site_config(site_name):
    """Retrieve site-aware configuration with safe defaults."""
    config = DEFAULT_SITE_CONFIG.copy()
    config.update(SITE_CONFIG.get(site_name, {}))
    # Ensure safe concurrency bounds based on DB pool size (reserve 2 for heartbeat/fetching)
    max_safe_active = max(1, DB_POOL_SIZE - 2)
    config["max_active"] = max(1, min(config["max_active"], max_safe_active))
    return config

# =====================================================================
# DATABASE CONNECTION POOL
# =====================================================================
_db_pool = None


def _get_pool():
    """
    Return the process-level ThreadedConnectionPool, creating it on first
    call.  Each Python process (GitHub Actions bot, VPS worker) has its
    own pool of size DB_POOL_SIZE.
    """
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        if not DATABASE_URL:
            raise EnvironmentError(
                "DATABASE_URL environment variable is not set. "
                "Set it to your Supabase/PostgreSQL connection string."
            )
        _db_pool = psycopg2_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=DB_POOL_SIZE,
            dsn=DATABASE_URL,
            connect_timeout=10,
        )
    return _db_pool


def get_db_connection():
    """Borrow a connection from the process-level pool."""
    return _get_pool().getconn()


def release_db_connection(conn):
    """Return a borrowed connection to the pool (does NOT close it)."""
    try:
        _get_pool().putconn(conn)
    except Exception:
        # Pool may have been closed during shutdown — just discard.
        try:
            conn.close()
        except Exception:
            pass


def shutdown_db_pool():
    """Close all pooled connections.  Call once at process exit."""
    global _db_pool
    if _db_pool and not _db_pool.closed:
        _db_pool.closeall()
    _db_pool = None


def check_movie_in_db(url):
    """Return True if this movie page URL already exists in the DB."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT url FROM movies WHERE url = %s;", (url,))
        result = cur.fetchone()
        cur.close()
        return bool(result)
    except Exception as e:
        print(f"   ⚠️ DB Check Error: {e}", flush=True)
        return False
    finally:
        if conn:
            release_db_connection(conn)


def get_existing_file_urls(movie_url):
    """
    For watchdog smart-verify: return the movie's DB id, title,
    and set of all existing direct download URLs.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, title FROM movies WHERE url = %s LIMIT 1",
            (movie_url,),
        )
        row = cur.fetchone()
        if not row:
            cur.close()
            return None, None, set()

        movie_id, db_title = row
        cur.execute(
            "SELECT url FROM movie_files WHERE movie_id = %s",
            (movie_id,),
        )
        existing_urls = {r[0] for r in cur.fetchall() if r[0]}
        cur.close()
        return movie_id, db_title, existing_urls
    except Exception as e:
        print(f"   ⚠️ DB Verify Error: {e}", flush=True)
        return None, None, set()
    finally:
        if conn:
            release_db_connection(conn)


# =====================================================================
# SCRAPER STATE, PROGRESS & BULK URL HELPERS
# =====================================================================
def initialize_db():
    """
    Auto-create scraper infrastructure tables on startup.
    Uses CREATE TABLE IF NOT EXISTS — safe to run on every boot.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Track which URLs have been scraped + their link fingerprint
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraped_urls (
                url         TEXT PRIMARY KEY,
                site_name   TEXT NOT NULL,
                scraped_at  TIMESTAMPTZ DEFAULT NOW(),
                link_hash   TEXT,
                skip_reason TEXT DEFAULT 'ok'
            );
        """)

        # Per-bot run progress (crash resume + monitoring dashboard)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS scraper_state (
                id           BIGSERIAL PRIMARY KEY,
                site_name    TEXT NOT NULL,
                bot_id       INT  NOT NULL DEFAULT 1,
                total_bots   INT  NOT NULL DEFAULT 1,
                run_mode     TEXT NOT NULL DEFAULT 'matrix',
                sitemap_hash TEXT,
                last_url_idx INT  DEFAULT 0,
                urls_total   INT  DEFAULT 0,
                urls_done    INT  DEFAULT 0,
                started_at   TIMESTAMPTZ DEFAULT NOW(),
                updated_at   TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (site_name, bot_id, total_bots, run_mode)
            );
        """)

        # Crawl run observability — one row per scraper invocation
        cur.execute("""
            CREATE TABLE IF NOT EXISTS crawl_runs (
                id              BIGSERIAL PRIMARY KEY,
                site_name       TEXT NOT NULL,
                run_mode        TEXT NOT NULL,
                bot_id          INT  DEFAULT 1,
                total_bots      INT  DEFAULT 1,
                started_at      TIMESTAMPTZ DEFAULT NOW(),
                finished_at     TIMESTAMPTZ,
                status          TEXT DEFAULT 'running',
                urls_discovered INT  DEFAULT 0,
                urls_processed  INT  DEFAULT 0,
                urls_inserted   INT  DEFAULT 0,
                urls_updated    INT  DEFAULT 0,
                urls_skipped    INT  DEFAULT 0,
                urls_failed     INT  DEFAULT 0,
                duration_secs   FLOAT,
                error_message   TEXT
            );
        """)

        conn.commit()
        cur.close()

        # ── Phase B: crawl_jobs queue table ──────────────────────────
        # CREATE TABLE IF NOT EXISTS + indexes — safe to run on every boot
        job_queue.create_crawl_jobs_table(conn)

        print(
            "✅ DB initialized — scraped_urls + scraper_state + crawl_runs + crawl_jobs ready.",
            flush=True,
        )
    except Exception as e:
        print(f"⚠️ DB init warning (tables may already exist): {e}", flush=True)
    finally:
        if conn:
            release_db_connection(conn)


def compute_link_hash(bypassed_links):
    """
    MD5 fingerprint of all final direct download URLs.
    Used to detect whether download links changed between scraper runs.
    Returns None if there are no URLs.
    """
    all_urls = sorted(
        dl["url"]
        for bl in bypassed_links
        for dl in bl.get("direct_links", [])
        if dl.get("url")
    )
    if not all_urls:
        return None
    return hashlib.md5("|".join(all_urls).encode()).hexdigest()


def compute_sitemap_hash(urls):
    """
    MD5 of the complete sorted URL list.
    If the sitemap changes between runs this hash changes → fresh start.
    """
    return hashlib.md5("|".join(sorted(urls)).encode()).hexdigest()


def get_already_scraped_urls_bulk(site_name, url_list, chunk_size=10_000):
    """
    Single-round-trip check: which URLs have already been scraped?

    Queries in chunks of `chunk_size` to handle very large URL lists
    (50 lakh+) without hitting psycopg2 parameter limits.

    Returns:
        dict {url -> link_hash}   (link_hash is None for no-link URLs)
    """
    result = {}
    if not url_list:
        return result
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        for i in range(0, len(url_list), chunk_size):
            chunk = url_list[i : i + chunk_size]
            cur.execute(
                "SELECT url, link_hash FROM scraped_urls "
                "WHERE site_name = %s AND url = ANY(%s)",
                (site_name, chunk),
            )
            for row in cur.fetchall():
                result[row[0]] = row[1]
        cur.close()
    except Exception as e:
        print(f"⚠️ Bulk URL check error: {e}", flush=True)
    finally:
        if conn:
            release_db_connection(conn)
    return result


def mark_url_scraped(url, site_name, link_hash=None, skip_reason="ok"):
    """
    Upsert a URL into scraped_urls after processing.
      skip_reason = 'ok'       -> successfully saved to DB
      skip_reason = 'no_links' -> page had no download links
      skip_reason = 'dead'     -> page returned 404 / load error
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO scraped_urls
                (url, site_name, link_hash, skip_reason, scraped_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (url) DO UPDATE SET
                link_hash   = COALESCE(EXCLUDED.link_hash, scraped_urls.link_hash),
                skip_reason = EXCLUDED.skip_reason,
                scraped_at  = NOW()
            """,
            (url, site_name, link_hash, skip_reason),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"⚠️ mark_url_scraped error: {e}", flush=True)
    finally:
        if conn:
            release_db_connection(conn)


def save_progress(
    site_name, bot_id, total_bots, mode, urls_done, urls_total, sitemap_hash
):
    """Upsert current scraping progress into scraper_state (monitoring + resume)."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO scraper_state
                (site_name, bot_id, total_bots, run_mode,
                 sitemap_hash, last_url_idx, urls_total, urls_done, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (site_name, bot_id, total_bots, run_mode) DO UPDATE SET
                sitemap_hash = EXCLUDED.sitemap_hash,
                last_url_idx = EXCLUDED.last_url_idx,
                urls_total   = EXCLUDED.urls_total,
                urls_done    = EXCLUDED.urls_done,
                updated_at   = NOW()
            """,
            (
                site_name, bot_id, total_bots, mode,
                sitemap_hash, urls_done, urls_total, urls_done,
            ),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"⚠️ save_progress error: {e}", flush=True)
    finally:
        if conn:
            release_db_connection(conn)


def load_progress(site_name, bot_id, total_bots, mode, sitemap_hash):
    """
    Load previous scraping state for crash-resume awareness.

    Returns urls_done from the last run if the sitemap hash still matches,
    else 0 (sitemap changed -> full fresh pass, bulk pre-filter handles skips).
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT urls_done, sitemap_hash, updated_at
            FROM scraper_state
            WHERE site_name=%s AND bot_id=%s AND total_bots=%s AND run_mode=%s
            LIMIT 1
            """,
            (site_name, bot_id, total_bots, mode),
        )
        row = cur.fetchone()
        cur.close()
        if not row:
            return 0
        saved_hash = row[1]
        if saved_hash != sitemap_hash:
            print(
                "Sitemap changed (hash mismatch) — "
                "scraper_state reset for this run.",
                flush=True,
            )
            return 0
        prev_done = row[0] or 0
        if prev_done > 0:
            print(
                f"Previous run found: {prev_done} URLs already processed "
                f"(as of {row[2]}). Bulk pre-filter will skip them.",
                flush=True,
            )
        return prev_done
    except Exception as e:
        print(f"⚠️ load_progress error: {e}", flush=True)
        return 0
    finally:
        if conn:
            release_db_connection(conn)


# =====================================================================
# CRAWL RUN TRACKING — Observability helpers
# =====================================================================
def create_crawl_run(site_name, run_mode, bot_id=1, total_bots=1):
    """Insert a new crawl_runs row and return its id."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO crawl_runs (site_name, run_mode, bot_id, total_bots)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (site_name, run_mode, bot_id, total_bots),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return run_id
    except Exception as e:
        print(f"⚠️ create_crawl_run error: {e}", flush=True)
        return None
    finally:
        if conn:
            release_db_connection(conn)


def finish_crawl_run(run_id, status, counters, error_message=None):
    """
    Finalize a crawl_runs row.
    counters = dict with keys: discovered, processed, inserted,
                               updated, skipped, failed
    """
    if run_id is None:
        return
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE crawl_runs SET
                finished_at     = NOW(),
                status          = %s,
                urls_discovered = %s,
                urls_processed  = %s,
                urls_inserted   = %s,
                urls_updated    = %s,
                urls_skipped    = %s,
                urls_failed     = %s,
                duration_secs   = EXTRACT(EPOCH FROM (NOW() - started_at)),
                error_message   = %s
            WHERE id = %s;
            """,
            (
                status,
                counters.get("discovered", 0),
                counters.get("processed", 0),
                counters.get("inserted", 0),
                counters.get("updated", 0),
                counters.get("skipped", 0),
                counters.get("failed", 0),
                error_message,
                run_id,
            ),
        )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"⚠️ finish_crawl_run error: {e}", flush=True)
    finally:
        if conn:
            release_db_connection(conn)


# =====================================================================
# TITLE CLEANING & METADATA EXTRACTION
# =====================================================================
def fix_movie_details(scraped_data, movie_url=None):
    """
    Clean the raw scraped title and extract structured metadata.

    Enriches scraped_data in-place with:
        Search_Query   – cleaned title for TMDB search
        Year           – release year or 'N/A'
        Type           – 'Movies' or 'Web Series'
        Default_Season – season number (int) for web series, else None
    """
    raw_title = scraped_data.get("Raw_Title", "").replace("🎬", "").strip()
    # Strip private-use unicode characters (some sites embed these)
    raw_title = re.sub(r"[\uE000-\uF8FF]", "", raw_title).strip()

    search_query = "UNKNOWN_TITLE"
    year = "N/A"
    media_type = "Movies"
    season_number = None

    if raw_title and raw_title != "N/A":
        # Step 1: Title is usually everything before the first bracket
        title_parts = re.split(r"\(|\[", raw_title)
        search_query = title_parts[0].strip()

        # Step 2: Parse bracketed segments for year / season
        brackets_content = re.findall(r"\((.*?)\)|\[(.*?)\]", raw_title)
        bracket_texts = [
            item for sublist in brackets_content for item in sublist if item
        ]

        for text in bracket_texts:
            text_lower = text.lower().strip()
            if re.match(r"^\d{4}$", text_lower):
                year = text_lower
                media_type = "Movies"
            elif "season" in text_lower or re.match(r"^s\d+", text_lower):
                media_type = "Web Series"
                s_match = re.search(r"(?i)(?:season|s)\s*(\d+)", text_lower)
                if s_match:
                    season_number = int(s_match.group(1))

        # Step 3: Detect season in the main title string
        if media_type != "Web Series":
            s_match = re.search(r"(?i)\bseason\s*(\d+)", raw_title)
            if s_match:
                media_type = "Web Series"
                season_number = int(s_match.group(1))
                search_query = re.sub(
                    r"(?i)\bseason\s*\d+.*", "", search_query
                ).strip()
            elif re.search(r"(?i)\bepisode\b", raw_title):
                media_type = "Web Series"

    # Step 4: Honour page-level type hint (e.g. Creator field found)
    page_type = scraped_data.get("Type", "")
    if page_type in ("TV Series", "Web Series") and media_type == "Movies":
        media_type = "Web Series"

    # Step 5: Fallback — extract title from URL slug
    if (not search_query or search_query == "UNKNOWN_TITLE") and movie_url:
        try:
            slug = movie_url.rstrip("/").split("/")[-1]
            if "season" in slug.lower() or "episode" in slug.lower():
                media_type = "Web Series"
                if season_number is None:
                    season_number = 1

            s_match_url = re.search(r"(?i)season-(\d+)", slug)
            if s_match_url:
                season_number = int(s_match_url.group(1))

            junk_words = {
                "hindi", "english", "dual", "audio", "dubbed", "uncut",
                "hdrip", "webrip", "bluray", "web", "dl", "esubs", "esub",
                "480p", "720p", "1080p", "4k", "x264", "x265", "hevc",
                "aac", "mb", "gb", "full", "movie", "hd", "pre", "dvdrip",
                "brrip", "hdtc", "camrip", "south", "bollywood",
                "hollywood", "series", "season", "complete", "all",
                "episodes", "download", "free", "filmyzilla", "hdhub4u",
                "mkvcinemas",
            }
            parts = slug.split("-")
            clean_parts = []
            for p in parts:
                if re.match(r"^\d{4}$", p):
                    year = p
                    if media_type != "Web Series":
                        media_type = "Movies"
                    break
                if re.match(r"^\d+[mg]b?$", p, re.IGNORECASE):
                    break
                if p.lower() not in junk_words and len(p) > 1:
                    clean_parts.append(p)
            if clean_parts:
                search_query = " ".join(clean_parts).strip()
        except Exception:
            pass

    # Step 6: Final junk removal from search query
    if search_query and search_query != "UNKNOWN_TITLE":
        junk_re = (
            r"(?i)\b(uncut|hindi|dual\s*audio|dubbed|480p|720p|1080p|"
            r"hdrip|webrip|web-dl|x264|hevc|esubs?|mb|gb|brrip|dvdrip|"
            r"hdtc|camrip|x265|aac|download|free)\b"
        )
        search_query = re.sub(junk_re, "", search_query).strip()
        search_query = re.sub(r"\b(19|20)\d{2}\b", "", search_query).strip()
        search_query = re.sub(r"[\(\)\[\]\-]+", " ", search_query).strip()
        search_query = re.sub(r"\s+", " ", search_query).strip()

    if not search_query:
        search_query = "UNKNOWN_TITLE"

    if media_type == "Web Series" and season_number is None:
        season_number = 1

    # Write enriched fields back
    scraped_data["Search_Query"] = search_query
    scraped_data["Year"] = year
    scraped_data["Type"] = media_type
    scraped_data["Default_Season"] = season_number

    season_info = f" | Season: {season_number}" if media_type == "Web Series" else ""
    print(
        f"   ✅ Cleaned: '{search_query}' (Year: {year}) "
        f"| Type: '{media_type}'{season_info}",
        flush=True,
    )
    return scraped_data


# =====================================================================
# TMDB API ENRICHMENT
# =====================================================================
def get_tmdb_details(fixed_data):
    """
    Search TMDB for the cleaned title and fetch rich metadata:
      – Basic info (title, poster, release date)
      – Genre, rating, cast, IMDb ID
      – Season/episode data for TV series
    Returns a metadata dict, or None on failure.
    """
    if not TMDB_API_KEY:
        print("   ⚠️ TMDB_API_KEY not set. Skipping enrichment.", flush=True)
        return None

    search_query = fixed_data.get("Search_Query", "")
    if not search_query or search_query == "UNKNOWN_TITLE":
        return None

    year_hint = fixed_data.get("Year", "N/A")
    type_hint = "tv" if fixed_data.get("Type") == "Web Series" else "movie"

    print(
        f"   🌐 TMDB lookup: '{search_query}' (type: {type_hint})...",
        flush=True,
    )

    try:
        # ── Search ────────────────────────────────────────────────────
        base = "https://api.themoviedb.org/3"
        q = urllib.parse.quote(search_query)
        search_url = f"{base}/search/{type_hint}?api_key={TMDB_API_KEY}&query={q}"
        if year_hint and year_hint != "N/A":
            yr_param = "first_air_date_year" if type_hint == "tv" else "year"
            search_url += f"&{yr_param}={year_hint}"

        results = requests.get(search_url, timeout=10).json().get("results", [])

        # Retry without year filter
        if not results and year_hint != "N/A":
            fb_url = f"{base}/search/{type_hint}?api_key={TMDB_API_KEY}&query={q}"
            results = requests.get(fb_url, timeout=10).json().get("results", [])

        # Retry with the alternate type (movie ↔ tv)
        if not results:
            alt = "movie" if type_hint == "tv" else "tv"
            alt_url = f"{base}/search/{alt}?api_key={TMDB_API_KEY}&query={q}"
            alt_res = requests.get(alt_url, timeout=10).json().get("results", [])
            if alt_res:
                results = alt_res
                type_hint = alt

        if not results:
            print(f"   ⚠️ TMDB: No results for '{search_query}'", flush=True)
            return None

        best = results[0]
        tmdb_id = best.get("id")

        # ── Details ───────────────────────────────────────────────────
        details = requests.get(
            f"{base}/{type_hint}/{tmdb_id}?api_key={TMDB_API_KEY}", timeout=10
        ).json()

        genres = [g["name"] for g in details.get("genres", [])]
        genre_str = ", ".join(genres) if genres else "N/A"
        plot = details.get("overview", "N/A")
        rating = (
            str(round(details.get("vote_average", 0), 1))
            if details.get("vote_average")
            else "N/A"
        )

        # ── Credits ──────────────────────────────────────────────────
        credits_data = requests.get(
            f"{base}/{type_hint}/{tmdb_id}/credits?api_key={TMDB_API_KEY}",
            timeout=10,
        ).json()
        cast_list = [c["name"] for c in credits_data.get("cast", [])[:5]]
        cast_str = ", ".join(cast_list) if cast_list else "N/A"

        # ── External IDs ─────────────────────────────────────────────
        ext_ids = requests.get(
            f"{base}/{type_hint}/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}",
            timeout=10,
        ).json()
        imdb_id = ext_ids.get("imdb_id", "N/A")

        # ── Season Data (TV only) ────────────────────────────────────
        seasons_data = {}
        if type_hint == "tv":
            for s in details.get("seasons", []):
                s_num = str(s.get("season_number", ""))
                if not s_num or s_num == "0":
                    continue

                s_air = str(s.get("air_date", ""))
                s_year = (
                    int(s_air[:4])
                    if len(s_air) >= 4 and s_air[:4].isdigit()
                    else 0
                )
                s_poster = (
                    f"https://image.tmdb.org/t/p/original{s.get('poster_path')}"
                    if s.get("poster_path")
                    else None
                )

                episodes_info = {}
                try:
                    sd = requests.get(
                        f"{base}/tv/{tmdb_id}/season/{s_num}"
                        f"?api_key={TMDB_API_KEY}",
                        timeout=5,
                    ).json()
                    for ep in sd.get("episodes", []):
                        ep_num = str(ep.get("episode_number"))
                        episodes_info[ep_num] = {
                            "air_date": ep.get("air_date", "")
                        }
                except Exception:
                    pass

                seasons_data[s_num] = {
                    "year": s_year,
                    "poster": s_poster,
                    "air_date": s_air,
                    "episode_count": s.get("episode_count", 0),
                    "episodes": episodes_info,
                }

        poster_path = best.get("poster_path")
        return {
            "Title": best.get("name") or best.get("title"),
            "Release": best.get("first_air_date")
            or best.get("release_date", "N/A"),
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "Genre": genre_str,
            "Description": plot,
            "TMDb_Rating": rating,
            "Cast": cast_str,
            "seasons_data": seasons_data,
            "Poster": (
                f"https://image.tmdb.org/t/p/original{poster_path}"
                if poster_path
                else "N/A"
            ),
            "is_tv": type_hint == "tv",
        }

    except Exception as e:
        print(f"   ❌ TMDB Error: {e}", flush=True)
        return None


# =====================================================================
# =====================================================================
# DATABASE UPSERT — Movies + Movie Files
# =====================================================================
def save_movie_to_db(data_dict):
    """
    Upsert a movie record and all its download-file records.

    Uses SELECT-first logic so it works whether or not a UNIQUE
    constraint on `title` exists.  The INSERT also includes
    ON CONFLICT (title) DO UPDATE as a safety net against race
    conditions when multiple bots process the same title.

    Expected keys in data_dict:
        url, raw_title, clean_title, Type, Default_Season, Year,
        IMDb, tmdb_data, Genre, Stars, Language, Description,
        bypassed_links: [{quality, size, direct_links: [{server_name, url}]}]
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Prevent infinite hangs if a row is locked by another dangling bot
        cur.execute("SET statement_timeout = 30000;")

        tmdb = data_dict.get("tmdb_data") or {}

        # ── Determine final title ────────────────────────────────────
        title = tmdb.get("Title") or data_dict.get("clean_title")
        if title:
            junk = (
                r"(?i)\b(uncut|hindi|dual\s*audio|dubbed|480p|720p|1080p|"
                r"hdrip|webrip|web-dl|x264|hevc|esubs?|mb|gb|brrip|"
                r"dvdrip|hdtc|camrip|x265|aac)\b"
            )
            title = re.sub(junk, "", title).strip()
            title = re.sub(r"\b(19|20)\d{2}\b", "", title).strip()
            title = re.sub(r"[\(\)\[\]\-]+", " ", title).strip()
            title = re.sub(r"\s+", " ", title).strip()

        if not title:
            print("   Warning: No valid title. Skipping DB save.", flush=True)
            return

        # ── Merge page-scraped + TMDB metadata ───────────────────────
        year = (
            tmdb.get("Release", "")[:4]
            if tmdb.get("Release")
            else data_dict.get("Year", "N/A")
        )
        poster = (
            tmdb.get("Poster")
            if tmdb.get("Poster") and tmdb.get("Poster") != "N/A"
            else ""
        )

        page_genre  = data_dict.get("Genre", "N/A")
        page_rating = data_dict.get("IMDb", "N/A")
        page_cast   = data_dict.get("Stars", "N/A")
        page_lang   = data_dict.get("Language", "N/A")
        page_desc   = data_dict.get("Description", "N/A")

        # Page data takes priority, TMDB as fallback
        genre_str  = page_genre  if page_genre  != "N/A" else tmdb.get("Genre", "N/A")
        rating_str = page_rating if page_rating != "N/A" else tmdb.get("TMDb_Rating", "N/A")
        cast_str   = page_cast   if page_cast   != "N/A" else tmdb.get("Cast", "N/A")
        plot_str   = page_desc   if page_desc   != "N/A" else tmdb.get("Description", "N/A")
        lang_str   = page_lang   if page_lang   != "N/A" else "Hindi"

        imdb_id_real = tmdb.get("imdb_id")
        seasons_json = tmdb.get("seasons_data", {})

        if data_dict.get("Type") == "Hot Web Series":
            final_category = "Hot Web Series"
        else:
            final_category = (
                "Web Series"
                if data_dict.get("Type") == "Web Series" or tmdb.get("is_tv")
                else "Movies"
            )

        try:
            year_val = int(year)
        except (ValueError, TypeError):
            year_val = None

        # ── UPSERT: movies table ─────────────────────────────────────
        cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
        row = cur.fetchone()

        if row:
            movie_id = row[0]
            cur.execute(
                """
                UPDATE movies SET
                    url          = %s,
                    poster_url   = COALESCE(NULLIF(poster_url, ''), %s),
                    seasons_data = %s
                WHERE id = %s
                """,
                (data_dict["url"], poster, json.dumps(seasons_json), movie_id),
            )
        else:
            # Try INSERT with ON CONFLICT safety net.  If the DB has no
            # UNIQUE constraint on title the ON CONFLICT clause is simply
            # never triggered and the plain INSERT succeeds.
            try:
                cur.execute(
                    """
                    INSERT INTO movies
                        (url, title, poster_url, year, genre, description,
                         rating, language, "cast", imdb_id, seasons_data, category)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (title) DO UPDATE SET
                        url          = EXCLUDED.url,
                        poster_url   = COALESCE(NULLIF(movies.poster_url,''), EXCLUDED.poster_url),
                        seasons_data = EXCLUDED.seasons_data
                    RETURNING id;
                    """,
                    (
                        data_dict["url"], title, poster, year_val,
                        genre_str, plot_str, rating_str, lang_str,
                        cast_str, imdb_id_real, json.dumps(seasons_json),
                        final_category,
                    ),
                )
                result = cur.fetchone()
                movie_id = result[0] if result else None
            except psycopg2.errors.UndefinedObject:
                # ON CONFLICT target doesn't exist — fall back to plain INSERT
                conn.rollback()
                cur.execute(
                    """
                    INSERT INTO movies
                        (url, title, poster_url, year, genre, description,
                         rating, language, "cast", imdb_id, seasons_data, category)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                    """,
                    (
                        data_dict["url"], title, poster, year_val,
                        genre_str, plot_str, rating_str, lang_str,
                        cast_str, imdb_id_real, json.dumps(seasons_json),
                        final_category,
                    ),
                )
                movie_id = cur.fetchone()[0]

        if not movie_id:
            conn.commit()
            cur.close()
            return

        # ── UPSERT: movie_files table (batch) ────────────────────────
        # All quality/episode/language detection logic is unchanged.
        # Records are collected first, then written in a single batch
        # INSERT ... ON CONFLICT using the existing unique constraint:
        #   unique_movie_quality_server (movie_id, quality, server_name, extra_info)
        default_season = data_dict.get("Default_Season") or 1
        bypassed_links = data_dict.get("bypassed_links", [])

        file_records = []  # (movie_id, quality, srv_name, srv_url, file_size, languages, ep_str)

        for link_group in bypassed_links:
            raw_quality  = link_group.get("quality", "Unknown")
            file_size    = link_group.get("size", "")
            direct_links = link_group.get("direct_links", [])

            for server in direct_links:
                srv_name_raw = server.get("server_name", "Download Server")
                srv_url      = server.get("url", "").strip()
                if not srv_url:
                    continue

                # ── Smart quality / episode / language detection ──────
                # (logic unchanged from original)
                decoded_url = urllib.parse.unquote(srv_url)
                fn_match = re.search(
                    r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE
                )
                actual_filename = fn_match.group(1) if fn_match else decoded_url
                combined_text = f"{actual_filename} {raw_quality}"

                # Episode detection
                is_combined = "[COMBINED]" in raw_quality
                js_ep_match = re.search(r"\[(E\d{1,3})\]", raw_quality)
                js_ep_str   = js_ep_match.group(1) if js_ep_match else ""

                ep_str = ""
                s_e_match = re.search(
                    r"(?i)\bS(\d{1,2})[\s._-]*E(\d{1,3})\b", actual_filename
                )
                if s_e_match:
                    ep_str = (
                        f"S{int(s_e_match.group(1)):02d}"
                        f"E{int(s_e_match.group(2)):02d}"
                    )
                elif js_ep_str:
                    ep_str = f"S{default_season:02d}{js_ep_str}"
                elif is_combined or re.search(
                    r"(?i)\b(batch|full season|complete|all episodes|pack|zip)\b",
                    combined_text,
                ):
                    ep_str = f"S{default_season:02d} Combined"

                if final_category == "Movies":
                    ep_str = ""

                # Quality tag
                quality = "HD"
                q_match = re.search(
                    r"\b(2160p|1080p|720p|480p|360p|4K)\b",
                    combined_text, re.IGNORECASE,
                )
                if q_match:
                    quality = q_match.group(1).lower()

                src_match = re.search(
                    r"\b(WEB-DL|WEBRip|BluRay|HDRip|HDTC|HDTS|CAMRip)\b",
                    combined_text, re.IGNORECASE,
                )
                if src_match:
                    quality += f" {src_match.group(1).upper()}"

                if quality == "HD":
                    q_fb = re.search(
                        r"\b(2160p|1080p|720p|480p|360p|4K)\b",
                        raw_quality, re.IGNORECASE,
                    )
                    if q_fb:
                        quality = q_fb.group(1).lower()

                # Language
                lang_keywords = [
                    "Hindi", "English", "Tamil", "Telugu",
                    "Malayalam", "Dual Audio", "Multi",
                ]
                langs = [
                    lk.title()
                    for lk in lang_keywords
                    if re.search(r"\b" + lk + r"\b", combined_text, re.IGNORECASE)
                ]
                languages = ", ".join(sorted(set(langs))) if langs else lang_str

                # File size
                if not file_size or file_size.lower() in ("", "n/a", "unknown"):
                    sz_m = re.search(
                        r"(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))", combined_text
                    )
                    file_size = (
                        sz_m.group(1).strip().upper().replace(" ", "") if sz_m else ""
                    )

                # Server name
                m_srv = re.search(r"(?i)download\s*\[(.+?)\]", srv_name_raw or "")
                srv_name = (
                    m_srv.group(1).strip() if m_srv else (srv_name_raw or "").strip()
                )
                if not srv_name:
                    srv_name = "Download Server"

                file_records.append(
                    (movie_id, quality, srv_name, srv_url, file_size, languages, ep_str)
                )

        # ── Batch upsert all file records in one round-trip ──────────
        # Uses the existing DB constraint:
        #   unique_movie_quality_server (movie_id, quality, server_name, extra_info)
        if file_records:
            # Deduplicate within the batch (Postgres ON CONFLICT cannot update the same row twice)
            unique_records = {}
            for rec in file_records:
                # rec = (movie_id, quality, srv_name, srv_url, file_size, languages, ep_str)
                key = (rec[0], rec[1], rec[2], rec[6])
                unique_records[key] = rec
            
            deduped_records = list(unique_records.values())

            execute_values(
                cur,
                """
                INSERT INTO movie_files
                    (movie_id, quality, server_name, url,
                     file_size, languages, extra_info, source)
                VALUES %s
                ON CONFLICT (movie_id, quality, server_name, extra_info) DO UPDATE SET
                    url       = EXCLUDED.url,
                    file_size = EXCLUDED.file_size,
                    languages = EXCLUDED.languages,
                    source    = 'scraped'
                """,
                deduped_records,
                template="(%s, %s, %s, %s, %s, %s, %s, 'scraped')",
            )

        conn.commit()
        cur.close()
        print(
            f"   DB Sync Complete: '{title}' ({len(file_records)} file records)",
            flush=True,
        )

    except Exception as e:
        print(f"   DB Save Error: {e}", flush=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            release_db_connection(conn)


def fenced_save_movie_to_db(data_dict, job_id: int, worker_id: str,
                             heartbeat_manager=None) -> bool:
    """
    Phase B version of save_movie_to_db — identical business logic but
    wraps the final DB writes inside a transaction with ownership fencing.

    Transaction sequence:
      1. BEGIN
      2. SELECT crawl_jobs ... FOR UPDATE NOWAIT  (ownership fencing)
      3. movies upsert
      4. movie_files batch upsert
      5. ack_job (mark completed inside same transaction)
      6. COMMIT

    If ownership fencing fails at step 2 (lease expired or stolen by sweeper),
    the transaction is rolled back and NO application data is written.

    Returns True on success, False if ownership was lost.
    """
    if heartbeat_manager and heartbeat_manager.is_lost(job_id):
        print(
            f"   ⚠️ Ownership lost (heartbeat detected) for job {job_id}. "
            "Aborting write.",
            flush=True,
        )
        return False

    conn = None
    try:
        conn = get_db_connection()
        # Disable autocommit so we control the transaction boundary
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 30000;")

        # ── Step 1: Ownership fencing ─────────────────────────────────
        # Lock the crawl_jobs row. Fails instantly if:
        #   - another worker holds the lock (NOWAIT)
        #   - claimed_by != worker_id
        #   - lease_expires_at <= NOW()
        if not validate_ownership_for_write(cur, job_id, worker_id):
            conn.rollback()
            print(
                f"   ⚠️ Fencing check failed for job {job_id}: "
                "lease expired or ownership lost. Aborting write.",
                flush=True,
            )
            return False

        # ── Steps 2-4: Identical business logic from save_movie_to_db ─
        tmdb = data_dict.get("tmdb_data") or {}

        title = tmdb.get("Title") or data_dict.get("clean_title")
        if title:
            junk = (
                r"(?i)\b(uncut|hindi|dual\s*audio|dubbed|480p|720p|1080p|"
                r"hdrip|webrip|web-dl|x264|hevc|esubs?|mb|gb|brrip|"
                r"dvdrip|hdtc|camrip|x265|aac)\b"
            )
            title = re.sub(junk, "", title).strip()
            title = re.sub(r"\b(19|20)\d{2}\b", "", title).strip()
            title = re.sub(r"[\(\)\[\]\-]+", " ", title).strip()
            title = re.sub(r"\s+", " ", title).strip()

        if not title:
            print("   Warning: No valid title. Skipping DB save.", flush=True)
            conn.rollback()
            return False

        year = (
            tmdb.get("Release", "")[:4]
            if tmdb.get("Release")
            else data_dict.get("Year", "N/A")
        )
        poster = (
            tmdb.get("Poster")
            if tmdb.get("Poster") and tmdb.get("Poster") != "N/A"
            else ""
        )
        page_genre  = data_dict.get("Genre", "N/A")
        page_rating = data_dict.get("IMDb", "N/A")
        page_cast   = data_dict.get("Stars", "N/A")
        page_lang   = data_dict.get("Language", "N/A")
        page_desc   = data_dict.get("Description", "N/A")

        genre_str  = page_genre  if page_genre  != "N/A" else tmdb.get("Genre", "N/A")
        rating_str = page_rating if page_rating != "N/A" else tmdb.get("TMDb_Rating", "N/A")
        cast_str   = page_cast   if page_cast   != "N/A" else tmdb.get("Cast", "N/A")
        plot_str   = page_desc   if page_desc   != "N/A" else tmdb.get("Description", "N/A")
        lang_str   = page_lang   if page_lang   != "N/A" else "Hindi"

        imdb_id_real = tmdb.get("imdb_id")
        seasons_json = tmdb.get("seasons_data", {})

        if data_dict.get("Type") == "Hot Web Series":
            final_category = "Hot Web Series"
        else:
            final_category = (
                "Web Series"
                if data_dict.get("Type") == "Web Series" or tmdb.get("is_tv")
                else "Movies"
            )

        try:
            year_val = int(year)
        except (ValueError, TypeError):
            year_val = None

        # movies upsert
        cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
        row = cur.fetchone()
        if row:
            movie_id = row[0]
            cur.execute(
                """
                UPDATE movies SET
                    url          = %s,
                    poster_url   = COALESCE(NULLIF(poster_url, ''), %s),
                    seasons_data = %s
                WHERE id = %s
                """,
                (data_dict["url"], poster, json.dumps(seasons_json), movie_id),
            )
        else:
            try:
                cur.execute(
                    """
                    INSERT INTO movies
                        (url, title, poster_url, year, genre, description,
                         rating, language, "cast", imdb_id, seasons_data, category)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (title) DO UPDATE SET
                        url          = EXCLUDED.url,
                        poster_url   = COALESCE(NULLIF(movies.poster_url,''), EXCLUDED.poster_url),
                        seasons_data = EXCLUDED.seasons_data
                    RETURNING id;
                    """,
                    (
                        data_dict["url"], title, poster, year_val,
                        genre_str, plot_str, rating_str, lang_str,
                        cast_str, imdb_id_real, json.dumps(seasons_json),
                        final_category,
                    ),
                )
                result = cur.fetchone()
                movie_id = result[0] if result else None
            except psycopg2.errors.UndefinedObject:
                conn.rollback()
                # Re-validate after rollback
                if not validate_ownership_for_write(cur, job_id, worker_id):
                    return False
                cur.execute(
                    """
                    INSERT INTO movies
                        (url, title, poster_url, year, genre, description,
                         rating, language, "cast", imdb_id, seasons_data, category)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id;
                    """,
                    (
                        data_dict["url"], title, poster, year_val,
                        genre_str, plot_str, rating_str, lang_str,
                        cast_str, imdb_id_real, json.dumps(seasons_json),
                        final_category,
                    ),
                )
                movie_id = cur.fetchone()[0]

        if not movie_id:
            conn.rollback()
            return False

        # movie_files batch upsert
        default_season  = data_dict.get("Default_Season") or 1
        bypassed_links  = data_dict.get("bypassed_links", [])
        file_records    = []

        for link_group in bypassed_links:
            raw_quality  = link_group.get("quality", "Unknown")
            file_size    = link_group.get("size", "")
            direct_links = link_group.get("direct_links", [])

            for server in direct_links:
                srv_name_raw = server.get("server_name", "Download Server")
                srv_url      = server.get("url", "").strip()
                if not srv_url:
                    continue

                decoded_url    = urllib.parse.unquote(srv_url)
                fn_match       = re.search(r'filename=["\']?(.*?)["\'\'&]', decoded_url, re.IGNORECASE)
                actual_filename = fn_match.group(1) if fn_match else decoded_url
                combined_text  = f"{actual_filename} {raw_quality}"

                is_combined = "[COMBINED]" in raw_quality
                js_ep_match = re.search(r"\[(E\d{1,3})\]", raw_quality)
                js_ep_str   = js_ep_match.group(1) if js_ep_match else ""

                ep_str = ""
                s_e_match = re.search(r"(?i)\bS(\d{1,2})[\s._-]*E(\d{1,3})\b", actual_filename)
                if s_e_match:
                    ep_str = f"S{int(s_e_match.group(1)):02d}E{int(s_e_match.group(2)):02d}"
                elif js_ep_str:
                    ep_str = f"S{default_season:02d}{js_ep_str}"
                elif is_combined or re.search(
                    r"(?i)\b(batch|full season|complete|all episodes|pack|zip)\b", combined_text
                ):
                    ep_str = f"S{default_season:02d} Combined"

                if final_category == "Movies":
                    ep_str = ""

                quality = "HD"
                q_match = re.search(r"\b(2160p|1080p|720p|480p|360p|4K)\b", combined_text, re.IGNORECASE)
                if q_match:
                    quality = q_match.group(1).lower()

                src_match = re.search(r"\b(WEB-DL|WEBRip|BluRay|HDRip|HDTC|HDTS|CAMRip)\b", combined_text, re.IGNORECASE)
                if src_match:
                    quality += f" {src_match.group(1).upper()}"

                if quality == "HD":
                    q_fb = re.search(r"\b(2160p|1080p|720p|480p|360p|4K)\b", raw_quality, re.IGNORECASE)
                    if q_fb:
                        quality = q_fb.group(1).lower()

                lang_keywords = ["Hindi", "English", "Tamil", "Telugu", "Malayalam", "Dual Audio", "Multi"]
                langs = [lk.title() for lk in lang_keywords
                         if re.search(r"\b" + lk + r"\b", combined_text, re.IGNORECASE)]
                languages = ", ".join(sorted(set(langs))) if langs else lang_str

                if not file_size or file_size.lower() in ("", "n/a", "unknown"):
                    sz_m = re.search(r"(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))", combined_text)
                    file_size = sz_m.group(1).strip().upper().replace(" ", "") if sz_m else ""

                m_srv = re.search(r"(?i)download\s*\[(.+?)\]", srv_name_raw or "")
                srv_name = m_srv.group(1).strip() if m_srv else (srv_name_raw or "").strip()
                if not srv_name:
                    srv_name = "Download Server"

                file_records.append(
                    (movie_id, quality, srv_name, srv_url, file_size, languages, ep_str)
                )

        if file_records:
            unique_records = {}
            for rec in file_records:
                key = (rec[0], rec[1], rec[2], rec[6])
                unique_records[key] = rec
            deduped_records = list(unique_records.values())

            execute_values(
                cur,
                """
                INSERT INTO movie_files
                    (movie_id, quality, server_name, url,
                     file_size, languages, extra_info, source)
                VALUES %s
                ON CONFLICT (movie_id, quality, server_name, extra_info) DO UPDATE SET
                    url       = EXCLUDED.url,
                    file_size = EXCLUDED.file_size,
                    languages = EXCLUDED.languages,
                    source    = 'scraped'
                """,
                deduped_records,
                template="(%s, %s, %s, %s, %s, %s, %s, 'scraped')",
            )

        # ── Step 5: Acknowledge job inside same transaction ───────────
        if not ack_job(cur, job_id, worker_id):
            conn.rollback()
            print(
                f"   ⚠️ ACK failed for job {job_id}: lease expired during write. "
                "Rolling back.",
                flush=True,
            )
            return False

        # ── Step 6: Commit ────────────────────────────────────────────
        conn.commit()
        cur.close()
        print(
            f"   ✅ DB Sync Complete (fenced): '{title}' "
            f"({len(file_records)} file records)",
            flush=True,
        )
        return True

    except Exception as e:
        print(f"   DB Save Error (fenced): {e}", flush=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            try:
                conn.autocommit = True
            except Exception:
                pass
            release_db_connection(conn)


# =====================================================================
# MODE: DISCOVERY  (Phase B — enqueue URLs into crawl_jobs)
# =====================================================================
async def run_discovery_mode(plugin, bot_id=1, total_bots=1,
                              is_watchdog=False, priority=None):
    """
    Discovery-only run: discover URLs and push them into crawl_jobs.
    Does NOT process / extract any movie pages.

    Watchdog discovery: top WATCHDOG_LIMIT URLs, priority=10
    Matrix discovery:   full sitemap split across bots, priority=100
    """
    if priority is None:
        priority = 10 if is_watchdog else 100

    mode_label = "watchdog-discovery" if is_watchdog else "matrix-discovery"
    print("=" * 60, flush=True)
    print(
        f"DISCOVERY MODE | Site: {plugin.SITE_NAME} | "
        f"{'Watchdog' if is_watchdog else f'Matrix Bot #{bot_id}/{total_bots}'} "
        f"| Priority: {priority}",
        flush=True,
    )
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    run_id = create_crawl_run(plugin.SITE_NAME, mode_label, bot_id, total_bots)
    counters = {"discovered": 0, "processed": 0, "inserted": 0,
                "updated": 0, "skipped": 0, "failed": 0}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox", "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = await browser.new_context(user_agent=USER_AGENT)

            if is_watchdog:
                all_urls = await plugin.get_all_urls(ctx, watchdog_mode=True)
                urls = all_urls[: plugin.WATCHDOG_LIMIT]
            else:
                all_urls = await plugin.get_all_urls(ctx)
                total = len(all_urls)
                chunk_size = max(1, total // total_bots)
                start_idx  = (bot_id - 1) * chunk_size
                end_idx    = total if bot_id == total_bots else start_idx + chunk_size
                urls = all_urls[start_idx:end_idx]

            await ctx.close()
            await browser.close()

        if not urls:
            print("No URLs discovered. Exiting.", flush=True)
            finish_crawl_run(run_id, "success", counters)
            return

        counters["discovered"] = len(urls)
        print(f"Discovered {len(urls)} URLs. Checking scraped_urls...", flush=True)

        # Filter out already-scraped URLs (unless FORCE_REFRESH)
        if not FORCE_REFRESH:
            already_scraped = get_already_scraped_urls_bulk(plugin.SITE_NAME, urls)
            new_urls = [u for u in urls if u not in already_scraped]
            counters["skipped"] = len(already_scraped)
            print(
                f"  {len(already_scraped)} already in scraped_urls → skipping.\n"
                f"  {len(new_urls)} new URLs → enqueueing.",
                flush=True,
            )
        else:
            new_urls = urls
            print(
                f"  FORCE_REFRESH=true — enqueueing all {len(new_urls)} URLs.",
                flush=True,
            )

        # Enqueue into crawl_jobs
        conn = get_db_connection()
        try:
            jobs = [(plugin.SITE_NAME, url) for url in new_urls]
            inserted = enqueue_jobs_bulk(conn, jobs, priority=priority)
            counters["inserted"] = inserted
            print(
                f"  Enqueued {inserted} new jobs (priority={priority}).",
                flush=True,
            )
        finally:
            release_db_connection(conn)

        finish_crawl_run(run_id, "success", counters)
        print(f"\nDiscovery complete.", flush=True)

    except Exception as exc:
        finish_crawl_run(run_id, "failed", counters, error_message=str(exc))
        raise


# =====================================================================
# MODE: WORKER  (Phase B — drain crawl_jobs queue)
# =====================================================================
async def run_worker_mode(plugin, max_jobs: int = 0):
    """
    Continuous worker: claim jobs from crawl_jobs, process them with
    the existing extraction/bypass/TMDB pipeline, then commit via the
    fenced transaction.

    max_jobs: if > 0, stop after processing this many jobs (for GHA
              bounded execution). If 0, run until the time limit.

    Connection budget: DB_POOL_SIZE >= MAX_ACTIVE_JOBS_PER_WORKER + 2
    (1 dedicated for HeartbeatManager, 1 buffer, rest for scraping).
    """
    site_conf = get_site_config(plugin.SITE_NAME)
    MAX_ACTIVE = site_conf["max_active"]

    print("=" * 60, flush=True)
    print(
        f"WORKER MODE | Site: {plugin.SITE_NAME} | Worker: {WORKER_ID} "
        f"| Max active: {MAX_ACTIVE} | Pool: {DB_POOL_SIZE}",
        flush=True,
    )
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    start_time = time.time()
    idle_timeout = int(os.environ.get("WORKER_IDLE_TIMEOUT", 60 if max_jobs > 0 else 0))
    idle_start = time.time()

    run_id = create_crawl_run(plugin.SITE_NAME, "worker")
    counters = {"discovered": 0, "processed": 0, "inserted": 0,
                "updated": 0, "skipped": 0, "failed": 0}
    last_crawl_run_update = time.time()

    # Graceful shutdown support
    _shutdown = threading.Event()
    _active_jobs: dict = {}   # job_id -> asyncio.Task

    def _handle_sigterm(sig, frame):
        print("\n⚠️ SIGTERM received — stopping gracefully.", flush=True)
        _shutdown.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    # Start multiplexed heartbeat manager
    hb = HeartbeatManager(get_db_connection, release_db_connection)
    hb.start()

    # Recover any stale jobs from previous workers and clean up old jobs
    conn = get_db_connection()
    try:
        recovered = recover_expired_leases(conn)
        if recovered:
            print(f"Sweeper: recovered {recovered} stale jobs from previous runs.", flush=True)
            
        try:
            cleaned = cleanup_old_jobs(conn)
            if cleaned:
                print(f"Cleanup: removed {cleaned} old queue jobs.", flush=True)
        except Exception as e:
            print(f"⚠️ Cleanup failed safely: {e}", flush=True)
            
        stats = get_queue_stats(conn)
        print(f"Queue stats: {stats}", flush=True)
    finally:
        release_db_connection(conn)

    sem = asyncio.Semaphore(MAX_ACTIVE)
    jobs_done = 0

    async def process_one_job(job: dict):
        nonlocal jobs_done
        job_id   = job["id"]
        url      = job["url"]
        site     = job["site_name"]
        w_id     = job["claimed_by"]

        hb.register(job_id)
        print(f"\n📥 Worker claimed job {job_id}: {url}", flush=True)

        conn_mark = None
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox", "--disable-setuid-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-blink-features=AutomationControlled",
                    ],
                )
                main_ctx = await browser.new_context(user_agent=USER_AGENT)
                await main_ctx.route(
                    "**/*",
                    lambda route: (
                        route.abort()
                        if route.request.resource_type in ["image", "media", "font"]
                        else route.continue_()
                    ),
                )
                page = await main_ctx.new_page()

                try:
                    await page.goto(url, timeout=60000, wait_until="domcontentloaded")
                    scraped_data = await plugin.extract_movie_data(page)
                except Exception as exc:
                    await page.close() if not page.is_closed() else None
                    await main_ctx.close()
                    await browser.close()
                    err_msg = str(exc)
                    conn_f = get_db_connection()
                    try:
                        new_status = fail_job(conn_f, job_id, w_id, err_msg)
                    finally:
                        release_db_connection(conn_f)
                    print(f"   ❌ Page load/extract error for job {job_id}: {exc} → {new_status}", flush=True)
                    counters["failed"] += 1
                    return
                finally:
                    if not page.is_closed():
                        await page.close()

                if not scraped_data:
                    await main_ctx.close()
                    await browser.close()
                    conn_f = get_db_connection()
                    try:
                        fail_job(conn_f, job_id, w_id, "Plugin returned no data")
                    finally:
                        release_db_connection(conn_f)
                    conn_mark = get_db_connection()
                    try:
                        mark_url_scraped(url, site, None, "no_links")
                    finally:
                        release_db_connection(conn_mark)
                        conn_mark = None
                    counters["skipped"] += 1
                    return

                raw_links = scraped_data.pop("raw_download_links", [])
                if not raw_links:
                    await main_ctx.close()
                    await browser.close()
                    conn_f = get_db_connection()
                    try:
                        fail_job(conn_f, job_id, w_id, "No download links found")
                    finally:
                        release_db_connection(conn_f)
                    await asyncio.to_thread(mark_url_scraped, url, site, None, "no_links")
                    counters["skipped"] += 1
                    return

                # Bypass
                try:
                    bypassed_links = await plugin.bypass_links(main_ctx, browser, raw_links)
                except Exception as exc:
                    bypassed_links = []

                bypassed_links = [b for b in bypassed_links if b.get("direct_links")]
                await main_ctx.close()
                await browser.close()

                if not bypassed_links:
                    conn_f = get_db_connection()
                    try:
                        fail_job(conn_f, job_id, w_id, "No valid links after bypass")
                    finally:
                        release_db_connection(conn_f)
                    await asyncio.to_thread(mark_url_scraped, url, site, None, "no_links")
                    counters["skipped"] += 1
                    return

            # Hash check
            new_link_hash = compute_link_hash(bypassed_links)

            # TMDB enrichment
            fixed_data = fix_movie_details(scraped_data, movie_url=url)
            if scraped_data.get("is_adult_bypass"):
                tmdb_data = {
                    "Title": fixed_data.get("Raw_Title", ""),
                    "Poster": fixed_data.get("Poster", ""),
                    "Genre": fixed_data.get("Genre", "Hot Web Series"),
                    "Cast": fixed_data.get("Stars", "N/A"),
                    "Description": fixed_data.get("Description", "N/A"),
                    "TMDb_Rating": "N/A", "is_tv": True, "seasons_data": {}
                }
                fixed_data["Type"] = "Hot Web Series"
            else:
                tmdb_data = await asyncio.to_thread(get_tmdb_details, fixed_data)

            # Check heartbeat before fenced write
            if hb.is_lost(job_id):
                print(f"   ⚠️ Ownership lost (heartbeat) before write — aborting job {job_id}.", flush=True)
                counters["failed"] += 1
                return

            db_payload = {
                "url": url,
                "raw_title": fixed_data.get("Raw_Title", ""),
                "clean_title": fixed_data.get("Search_Query", ""),
                "Type": fixed_data.get("Type", "Movies"),
                "Default_Season": fixed_data.get("Default_Season"),
                "Year": fixed_data.get("Year", "N/A"),
                "IMDb": fixed_data.get("IMDb", "N/A"),
                "tmdb_data": tmdb_data,
                "Genre": fixed_data.get("Genre", "N/A"),
                "Stars": fixed_data.get("Stars", "N/A"),
                "Language": fixed_data.get("Language", "N/A"),
                "Description": fixed_data.get("Description", "N/A"),
                "bypassed_links": bypassed_links,
            }

            # Fenced DB write — includes ACK inside transaction
            success = await asyncio.to_thread(
                fenced_save_movie_to_db, db_payload, job_id, w_id, hb
            )

            if success:
                await asyncio.to_thread(
                    mark_url_scraped, url, site, new_link_hash, "ok"
                )
                counters["inserted"] += 1
                jobs_done += 1
            else:
                counters["failed"] += 1

        except Exception as exc:
            print(f"   ❌ Unhandled worker error for job {job_id}: {exc}", flush=True)
            conn_f = get_db_connection()
            try:
                fail_job(conn_f, job_id, w_id, str(exc))
            finally:
                release_db_connection(conn_f)
            counters["failed"] += 1
        finally:
            counters["processed"] += 1
            hb.unregister(job_id)

    # ── Main worker polling loop ──────────────────────────────────────
    try:
        while not _shutdown.is_set():
            if time.time() - start_time > MAX_RUN_TIME_SECONDS:
                print("\n⏰ Time limit reached — stopping gracefully.", flush=True)
                break

            if max_jobs > 0 and jobs_done >= max_jobs:
                print(f"\n✅ Reached max_jobs={max_jobs}. Stopping.", flush=True)
                break

            # Periodic crawl_runs update (every 30 min)
            if time.time() - last_crawl_run_update > 1800:
                # In-place update for long-running worker session
                conn_u = get_db_connection()
                try:
                    c = conn_u.cursor()
                    c.execute(
                        """
                        UPDATE crawl_runs SET
                            urls_processed = %s,
                            urls_inserted  = %s,
                            urls_failed    = %s
                        WHERE id = %s;
                        """,
                        (counters["processed"], counters["inserted"],
                         counters["failed"], run_id),
                    )
                    conn_u.commit()
                    c.close()
                finally:
                    release_db_connection(conn_u)
                last_crawl_run_update = time.time()

            # Wait for a free slot
            await sem.acquire()
            
            conn_c = get_db_connection()
            try:
                job = claim_next_job(
                    conn_c, site_name=plugin.SITE_NAME, worker_id=WORKER_ID
                )
            finally:
                release_db_connection(conn_c)

            if job is None:
                # Queue is empty — wait before polling again
                sem.release()
                
                if _active_jobs:
                    idle_start = time.time()  # Reset idle timer if jobs are still processing
                elif idle_timeout > 0 and (time.time() - idle_start > idle_timeout):
                    print(f"\n⏳ Queue empty continuously for {idle_timeout}s. Exiting worker.", flush=True)
                    break

                print("   Queue empty. Waiting 10s...", flush=True)
                await asyncio.sleep(10)
                continue

            # Successfully claimed a job
            idle_start = time.time()

            # Create the task
            t = asyncio.ensure_future(process_one_job(job))
            
            # When the task completes, release the semaphore and remove from active tracking
            def _on_job_done(task, j_id=job["id"]):
                _active_jobs.pop(j_id, None)
                sem.release()
                
            t.add_done_callback(_on_job_done)
            _active_jobs[job["id"]] = t

    finally:
        # Let active tasks drain (up to 2 minutes)
        print("\n🛑 Worker shutting down — waiting for active jobs...", flush=True)
        for _ in range(24):   # 24 × 5s = 2 minutes
            if not _active_jobs:
                break
            await asyncio.sleep(5)

        hb.stop()
        finish_crawl_run(run_id, "success", counters)
        print(
            f"\nWorker finished. Processed={counters['processed']}, "
            f"Inserted={counters['inserted']}, Failed={counters['failed']}",
            flush=True,
        )


async def scrape_and_save_movie(
    movie_url, plugin, browser, main_context, sem,
    is_watchdog=False, site_name="", existing_link_hash=None,
):
    """
    Full pipeline for one movie URL:
      1. Navigate  →  plugin.extract_movie_data()
      2. Bypass    →  plugin.bypass_links()
      3. Hash check → skip if link fingerprint unchanged (matrix + watchdog)
      4. TMDB      →  enrich with metadata
      5. Save      →  upsert into PostgreSQL
      6. Mark      →  update scraped_urls with new link fingerprint

    Pre-filtering (bulk scraped_urls check) is done by the caller
    (run_matrix_mode / run_watchdog_mode) before this function is invoked.
    """
    async with sem:
        print(f"\n🎬 PROCESSING: {movie_url}", flush=True)

        page = await main_context.new_page()
        try:
            await page.goto(
                movie_url, timeout=60000, wait_until="domcontentloaded"
            )

            # ── Step 1: Extract via plugin ───────────────────────────
            scraped_data = await plugin.extract_movie_data(page)
        except Exception as e:
            print(f"   ❌ Page load / extract error: {e}", flush=True)
            await asyncio.to_thread(
                mark_url_scraped, movie_url, site_name, None, "dead"
            )
            return
        finally:
            if not page.is_closed():
                await page.close()

        if not scraped_data:
            print(
                f"   ⚠️ SKIP: Plugin returned no data for {movie_url}",
                flush=True,
            )
            await asyncio.to_thread(
                mark_url_scraped, movie_url, site_name, None, "no_links"
            )
            return

        raw_links = scraped_data.pop("raw_download_links", [])
        if not raw_links:
            print(
                f"   ⚠️ SKIP: No download links on {movie_url}", flush=True
            )
            await asyncio.to_thread(
                mark_url_scraped, movie_url, site_name, None, "no_links"
            )
            return

        # ── Step 2: Bypass via plugin ────────────────────────────────
        try:
            bypassed_links = await plugin.bypass_links(
                main_context, browser, raw_links
            )
        except Exception as e:
            print(f"   ❌ Bypass error: {e}", flush=True)
            bypassed_links = []

        bypassed_links = [b for b in bypassed_links if b.get("direct_links")]

        if not bypassed_links:
            print(
                f"   ⚠️ SKIP: No valid links after bypass for {movie_url}",
                flush=True,
            )
            await asyncio.to_thread(
                mark_url_scraped, movie_url, site_name, None, "no_links"
            )
            return

        # ── Step 3: Hash-based smart-verify ─────────────────────────
        # Works for both watchdog (daily) and matrix force-refresh runs.
        new_link_hash = compute_link_hash(bypassed_links)
        if existing_link_hash and new_link_hash == existing_link_hash:
            print(
                f"   ✅ HASH MATCH: Links unchanged for {movie_url}. "
                f"Skipping save.",
                flush=True,
            )
            # Refresh scraped_at so we know this URL was actively verified
            await asyncio.to_thread(
                mark_url_scraped, movie_url, site_name, new_link_hash, "ok"
            )
            return

        # ── Step 4: TMDB enrichment ──────────────────────────────────
        fixed_data = fix_movie_details(scraped_data, movie_url=movie_url)
        
        if scraped_data.get("is_adult_bypass"):
            print("   🔞 Adult Bypass Flag detected: Skipping TMDB enrichment.", flush=True)
            tmdb_data = {
                "Title": fixed_data.get("Raw_Title", ""),
                "Poster": fixed_data.get("Poster", ""),
                "Genre": fixed_data.get("Genre", "Hot Web Series"),
                "Cast": fixed_data.get("Stars", "N/A"),
                "Description": fixed_data.get("Description", "N/A"),
                "TMDb_Rating": "N/A",
                "is_tv": True,
                "seasons_data": {}
            }
            fixed_data["Type"] = "Hot Web Series"
        else:
            tmdb_data = await asyncio.to_thread(get_tmdb_details, fixed_data)

        # ── Step 5: DB upsert ────────────────────────────────────────
        db_payload = {
            "url": movie_url,
            "raw_title": fixed_data.get("Raw_Title", ""),
            "clean_title": fixed_data.get("Search_Query", ""),
            "Type": fixed_data.get("Type", "Movies"),
            "Default_Season": fixed_data.get("Default_Season"),
            "Year": fixed_data.get("Year", "N/A"),
            "IMDb": fixed_data.get("IMDb", "N/A"),
            "tmdb_data": tmdb_data,
            "Genre": fixed_data.get("Genre", "N/A"),
            "Stars": fixed_data.get("Stars", "N/A"),
            "Language": fixed_data.get("Language", "N/A"),
            "Description": fixed_data.get("Description", "N/A"),
            "bypassed_links": bypassed_links,
        }

        await asyncio.to_thread(save_movie_to_db, db_payload)

        # ── Step 6: Mark URL as scraped with link fingerprint ────────
        await asyncio.to_thread(
            mark_url_scraped, movie_url, site_name, new_link_hash, "ok"
        )


# =====================================================================
# MODE: MATRIX  (Historical Bulk Scraping)
# =====================================================================
async def run_matrix_mode(plugin, bot_id, total_bots):
    """Scrape ALL URLs from the plugin, split across N bots."""
    print("=" * 60, flush=True)
    print(
        f"MATRIX MODE | Site: {plugin.SITE_NAME} "
        f"| Bot #{bot_id}/{total_bots}",
        flush=True,
    )
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    if FORCE_REFRESH:
        print("FORCE_REFRESH=true — all URLs will be re-scraped.", flush=True)
    print("=" * 60, flush=True)

    start_time = time.time()

    # Create a crawl_runs record for observability
    run_id = create_crawl_run(plugin.SITE_NAME, "matrix", bot_id, total_bots)
    run_counters = {"discovered": 0, "processed": 0, "inserted": 0,
                   "updated": 0, "skipped": 0, "failed": 0}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            # ── Phase 1: URL Discovery ────────────────────────────────────────────────────────────────────────────────────────────
            print("\nPhase 1: Discovering URLs...", flush=True)
            discovery_ctx = await browser.new_context(user_agent=USER_AGENT)
            all_urls = await plugin.get_all_urls(discovery_ctx)
            await discovery_ctx.close()

            if not all_urls:
                print("No URLs discovered. Exiting.", flush=True)
                await browser.close()
                finish_crawl_run(run_id, "success", run_counters)
                return

            # ── Phase 2: Sitemap hash + workload split ──────────────────────────────────────────────────────────────────────
            total = len(all_urls)
            sitemap_hash = compute_sitemap_hash(all_urls)
            run_counters["discovered"] = total
            print(
                f"\nSitemap fingerprint: {sitemap_hash[:8]}... ({total} total URLs)",
                flush=True,
            )

            chunk_size = max(1, total // total_bots)
            start_idx = (bot_id - 1) * chunk_size
            end_idx = total if bot_id == total_bots else start_idx + chunk_size
            my_urls = all_urls[start_idx:end_idx]

            # ── Phase 3: Load previous progress (for monitoring) ────────────────────────────────────────────────────────────────────
            prev_done = load_progress(
                plugin.SITE_NAME, bot_id, total_bots, "matrix", sitemap_hash
            )

            # ── Phase 4: Bulk pre-filter (single round-trip per 10K URLs) ──────────────────────────────────────────────
            if FORCE_REFRESH:
                urls_to_scrape = my_urls
                already_scraped = {}
                print(
                    f"\nBot #{bot_id}: {len(urls_to_scrape)} URLs "
                    f"(force-refresh — bulk pre-filter skipped)",
                    flush=True,
                )
            else:
                print(
                    f"\nPhase 4: Bulk pre-filter "
                    f"({len(my_urls)} URLs against scraped_urls)...",
                    flush=True,
                )
                already_scraped = get_already_scraped_urls_bulk(
                    plugin.SITE_NAME, my_urls
                )
                urls_to_scrape = [u for u in my_urls if u not in already_scraped]
                run_counters["skipped"] = len(already_scraped)
                print(
                    f"   {len(already_scraped)} already scraped -> skipping.\n"
                    f"   {len(urls_to_scrape)} new URLs queued for scraping.",
                    flush=True,
                )

            if not urls_to_scrape:
                print(
                    f"Bot #{bot_id}: Nothing new to scrape. All done!",
                    flush=True,
                )
                await browser.close()
                finish_crawl_run(run_id, "success", run_counters)
                return

            print(
                f"Bot #{bot_id}: range [{start_idx}:{end_idx}] | "
                f"{len(urls_to_scrape)} to scrape",
                flush=True,
            )

            # ── Phase 5: Scraping ─────────────────────────────────────────────────────────────────────────────────────────────────
            main_ctx = await browser.new_context(user_agent=USER_AGENT)
            await main_ctx.route(
                "**/*",
                lambda route: (
                    route.abort()
                    if route.request.resource_type in ["image", "media", "font"]
                    else route.continue_()
                ),
            )
            sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

            done_in_run = 0
            for i in range(0, len(urls_to_scrape), BATCH_SIZE):
                if time.time() - start_time > MAX_RUN_TIME_SECONDS:
                    print("Time limit reached. Stopping gracefully.", flush=True)
                    break

                batch = urls_to_scrape[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                total_batches = (len(urls_to_scrape) + BATCH_SIZE - 1) // BATCH_SIZE
                print(
                    f"\nBatch {batch_num}/{total_batches} ({len(batch)} URLs)...",
                    flush=True,
                )

                tasks = [
                    scrape_and_save_movie(
                        url, plugin, browser, main_ctx, sem,
                        is_watchdog=False,
                        site_name=plugin.SITE_NAME,
                    )
                    for url in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Log any unhandled exceptions from the gather
                for url, result in zip(batch, results):
                    if isinstance(result, Exception):
                        print(f"   Unhandled: {url} -> {result}", flush=True)
                        run_counters["failed"] += 1

                done_in_run += len(batch)
                run_counters["processed"] = done_in_run

                # ── Save progress after every batch (crash-resume marker) ───────────────────────────────────────
                save_progress(
                    plugin.SITE_NAME, bot_id, total_bots, "matrix",
                    prev_done + done_in_run,
                    len(my_urls),
                    sitemap_hash,
                )

            await main_ctx.close()
            await browser.close()

        elapsed = time.time() - start_time
        print(
            f"\nMatrix Bot #{bot_id} finished in {elapsed / 60:.1f} min!",
            flush=True,
        )
        finish_crawl_run(run_id, "success", run_counters)

    except Exception as exc:
        finish_crawl_run(run_id, "failed", run_counters, error_message=str(exc))
        raise


# =====================================================================
# MODE: WATCHDOG  (Daily Quick Sync)
# =====================================================================
async def run_watchdog_mode(plugin):
    """Scrape only the top N most-recent URLs for daily updates."""
    print("=" * 60, flush=True)
    print(
        f"WATCHDOG MODE | Site: {plugin.SITE_NAME} "
        f"| Limit: {plugin.WATCHDOG_LIMIT}",
        flush=True,
    )
    print(f"Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    if FORCE_REFRESH:
        print("FORCE_REFRESH=true — link hash check disabled.", flush=True)
    print("=" * 60, flush=True)

    start_time = time.time()

    # Create a crawl_runs record for observability
    run_id = create_crawl_run(plugin.SITE_NAME, "watchdog")
    run_counters = {"discovered": 0, "processed": 0, "inserted": 0,
                    "updated": 0, "skipped": 0, "failed": 0}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                ],
            )

            # -- Phase 1: URL Discovery --
            print("\nDiscovering latest URLs...", flush=True)
            discovery_ctx = await browser.new_context(user_agent=USER_AGENT)
            all_urls = await plugin.get_all_urls(discovery_ctx, watchdog_mode=True)
            await discovery_ctx.close()

            if not all_urls:
                print("No URLs discovered. Exiting.", flush=True)
                await browser.close()
                finish_crawl_run(run_id, "success", run_counters)
                return

            # -- Phase 2: Slice top N --
            watchdog_urls = all_urls[: plugin.WATCHDOG_LIMIT]
            run_counters["discovered"] = len(watchdog_urls)
            print(
                f"Watchdog scanning top {len(watchdog_urls)} URLs...\n",
                flush=True,
            )

            # -- Phase 3: Bulk pre-fetch stored link hashes --
            # Each URL's stored link_hash lets us skip unchanged content
            # without re-saving to the DB (full scrape still happens to
            # compute the new hash, but save is skipped on match).
            already_scraped = {}
            if not FORCE_REFRESH:
                already_scraped = get_already_scraped_urls_bulk(
                    plugin.SITE_NAME, watchdog_urls
                )
                run_counters["skipped"] = len(already_scraped)
                print(
                    f"{len(already_scraped)} URLs have stored link hashes "
                    f"(will skip DB save if unchanged).",
                    flush=True,
                )

            # -- Phase 4: Concurrent scraping with hash-based verify --
            main_ctx = await browser.new_context(user_agent=USER_AGENT)
            await main_ctx.route(
                "**/*",
                lambda route: (
                    route.abort()
                    if route.request.resource_type in ["image", "media", "font"]
                    else route.continue_()
                ),
            )
            sem = asyncio.Semaphore(CONCURRENCY_LIMIT)

            done_in_run = 0
            for i in range(0, len(watchdog_urls), BATCH_SIZE):
                if time.time() - start_time > MAX_RUN_TIME_SECONDS:
                    print("Time limit reached.", flush=True)
                    break

                batch = watchdog_urls[i : i + BATCH_SIZE]
                batch_num = i // BATCH_SIZE + 1
                total_batches = (len(watchdog_urls) + BATCH_SIZE - 1) // BATCH_SIZE
                print(
                    f"\nBatch {batch_num}/{total_batches} "
                    f"({len(batch)} URLs) processing concurrently...",
                    flush=True,
                )

                tasks = [
                    scrape_and_save_movie(
                        url, plugin, browser, main_ctx, sem,
                        is_watchdog=True,
                        site_name=plugin.SITE_NAME,
                        existing_link_hash=already_scraped.get(url),
                    )
                    for url in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for url, result in zip(batch, results):
                    if isinstance(result, Exception):
                        print(
                            f"   Unhandled error for {url}: {result}",
                            flush=True,
                        )
                        run_counters["failed"] += 1

                done_in_run += len(batch)
                run_counters["processed"] = done_in_run

            await main_ctx.close()
            await browser.close()

        elapsed = time.time() - start_time
        print(
            f"\nWatchdog complete in {elapsed / 60:.1f} min! DB synced.",
            flush=True,
        )
        finish_crawl_run(run_id, "success", run_counters)

    except Exception as exc:
        finish_crawl_run(run_id, "failed", run_counters, error_message=str(exc))
        raise



# ENTRY POINT & CLI ARGUMENT PARSER
# =====================================================================
def main():
    parser = argparse.ArgumentParser(
        description="🚀 Universal Multi-Site Scraping Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --site filmyzilla --mode matrix  --bot_id 1 --total_bots 5
  python main.py --site hdhub4u    --mode watchdog
  python main.py --site mkvcinemas --mode matrix  --bot_id 3 --total_bots 10
  python main.py --site filmyzilla --mode discovery           # Phase B: seed queue
  python main.py --site filmyzilla --mode worker              # Phase B: drain queue
        """,
    )
    parser.add_argument(
        "--site",
        type=str,
        required=True,
        help="Site plugin name (e.g. filmyzilla, hdhub4u, mkvcinemas)",
    )
    parser.add_argument(
        "--mode",
        type=str,
        required=True,
        choices=["matrix", "watchdog", "discovery", "worker"],
        help=(
            "'matrix': bulk historical scrape (Phase A); "
            "'watchdog': daily top-N sync (Phase A); "
            "'discovery': Phase B — discover URLs and push to queue; "
            "'worker': Phase B — drain queue and process URLs"
        ),
    )
    parser.add_argument(
        "--bot_id",
        type=int,
        default=1,
        help="Bot ID for matrix work-splitting (default: 1)",
    )
    parser.add_argument(
        "--total_bots",
        type=int,
        default=1,
        help="Total bots for matrix mode (default: 1)",
    )

    args = parser.parse_args()

    # ── Validate ─────────────────────────────────────────────────────
    if args.mode == "matrix" and args.bot_id > args.total_bots:
        print(
            f"❌ Error: bot_id ({args.bot_id}) cannot exceed "
            f"total_bots ({args.total_bots})"
        )
        sys.exit(1)

    # discovery mode with matrix bot splitting also needs validation
    if args.mode == "discovery" and args.bot_id > args.total_bots:
        print(
            f"❌ Error: bot_id ({args.bot_id}) cannot exceed "
            f"total_bots ({args.total_bots})"
        )
        sys.exit(1)

    # ── Dynamic Plugin Loading ───────────────────────────────────────
    print(f"🔌 Loading plugin: sites/{args.site}.py", flush=True)
    try:
        module = importlib.import_module(f"sites.{args.site}")
        plugin = module.SitePlugin()
        print(
            f"✅ Plugin loaded: {plugin.SITE_NAME} ({plugin.TARGET_WEBSITE})",
            flush=True,
        )
    except ModuleNotFoundError:
        print(
            f"❌ Plugin 'sites/{args.site}.py' not found. "
            f"Check the /sites/ directory."
        )
        sys.exit(1)
    except AttributeError:
        print(
            f"❌ Plugin 'sites/{args.site}.py' does not expose "
            f"a 'SitePlugin' class."
        )
        sys.exit(1)

    # ── Environment checks ───────────────────────────────────────────
    if not DATABASE_URL:
        print(
            "⚠️  WARNING: DATABASE_URL not set. DB operations will fail.",
            flush=True,
        )
    if not TMDB_API_KEY:
        print(
            "⚠️  WARNING: TMDB_API_KEY not set. TMDB enrichment disabled.",
            flush=True,
        )

    # ── Check Site Config ────────────────────────────────────────────
    site_conf = get_site_config(plugin.SITE_NAME)
    if not site_conf.get("enabled", True):
        print(f"⏸️  Site '{plugin.SITE_NAME}' is disabled in configuration. Exiting gracefully.", flush=True)
        return

    # ── Initialize DB (auto-create tables if not exist) ──────────────
    initialize_db()

    # ── Dispatch ─────────────────────────────────────────────────────
    try:
        if args.mode == "matrix":
            asyncio.run(run_matrix_mode(plugin, args.bot_id, args.total_bots))
        elif args.mode == "watchdog":
            asyncio.run(run_watchdog_mode(plugin))
        elif args.mode == "discovery":
            # Phase B: discovery-only — push URLs into crawl_jobs
            # is_watchdog=True slices top WATCHDOG_LIMIT; False uses full sitemap
            is_wd = os.environ.get("DISCOVERY_WATCHDOG", "false").lower() == "true"
            asyncio.run(
                run_discovery_mode(
                    plugin,
                    bot_id=args.bot_id,
                    total_bots=args.total_bots,
                    is_watchdog=is_wd,
                )
            )
        elif args.mode == "worker":
            # Phase B: worker — drain crawl_jobs
            # MAX_JOBS env var for GHA bounded execution
            max_jobs = int(os.environ.get("MAX_WORKER_JOBS", "0"))
            asyncio.run(run_worker_mode(plugin, max_jobs=max_jobs))
    finally:
        shutdown_db_pool()


if __name__ == "__main__":
    main()
