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
import os
import re
import sys
import time
import json
import argparse
import importlib
import urllib.parse

import requests
import psycopg2
import nest_asyncio
from playwright.async_api import async_playwright

nest_asyncio.apply()


# =====================================================================
# CONFIGURATION — All secrets via environment variables
# =====================================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "")
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "")

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


# =====================================================================
# DATABASE HELPERS
# =====================================================================
def get_db_connection():
    """Create a new PostgreSQL connection using DATABASE_URL env var."""
    if not DATABASE_URL:
        raise EnvironmentError(
            "❌ DATABASE_URL environment variable is not set. "
            "Set it to your Supabase/PostgreSQL connection string."
        )
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)


def check_movie_in_db(url):
    """Return True if this movie page URL already exists in the DB."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT url FROM movies WHERE url = %s;", (url,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return bool(result)
    except Exception as e:
        print(f"   ⚠️ DB Check Error: {e}", flush=True)
        return False


def get_existing_file_urls(movie_url):
    """
    For watchdog smart-verify: return the movie's DB id, title,
    and set of all existing direct download URLs.
    """
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
            conn.close()
            return None, None, set()

        movie_id, db_title = row
        cur.execute(
            "SELECT url FROM movie_files WHERE movie_id = %s",
            (movie_id,),
        )
        existing_urls = {r[0] for r in cur.fetchall() if r[0]}
        cur.close()
        conn.close()
        return movie_id, db_title, existing_urls
    except Exception as e:
        print(f"   ⚠️ DB Verify Error: {e}", flush=True)
        return None, None, set()


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
            print("   ⚠️ No valid title. Skipping DB save.", flush=True)
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

        page_genre = data_dict.get("Genre", "N/A")
        page_rating = data_dict.get("IMDb", "N/A")
        page_cast = data_dict.get("Stars", "N/A")
        page_lang = data_dict.get("Language", "N/A")
        page_desc = data_dict.get("Description", "N/A")

        # Page data takes priority, TMDB as fallback
        genre_str = page_genre if page_genre != "N/A" else tmdb.get("Genre", "N/A")
        rating_str = page_rating if page_rating != "N/A" else tmdb.get("TMDb_Rating", "N/A")
        cast_str = page_cast if page_cast != "N/A" else tmdb.get("Cast", "N/A")
        plot_str = page_desc if page_desc != "N/A" else tmdb.get("Description", "N/A")
        lang_str = page_lang if page_lang != "N/A" else "Hindi"

        imdb_id_real = tmdb.get("imdb_id")
        seasons_json = tmdb.get("seasons_data", {})
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
                    url        = %s,
                    poster_url = COALESCE(NULLIF(poster_url, ''), %s),
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
            conn.close()
            return

        # ── UPSERT: movie_files table ────────────────────────────────
        default_season = data_dict.get("Default_Season") or 1
        bypassed_links = data_dict.get("bypassed_links", [])

        for link_group in bypassed_links:
            raw_quality = link_group.get("quality", "Unknown")
            file_size = link_group.get("size", "")
            direct_links = link_group.get("direct_links", [])

            for server in direct_links:
                srv_name_raw = server.get("server_name", "Download Server")
                srv_url = server.get("url", "").strip()
                if not srv_url:
                    continue

                # ── Smart quality / episode / language detection ──────
                decoded_url = urllib.parse.unquote(srv_url)
                fn_match = re.search(
                    r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE
                )
                actual_filename = fn_match.group(1) if fn_match else decoded_url
                combined_text = f"{actual_filename} {raw_quality}"

                # Episode detection
                is_combined = "[COMBINED]" in raw_quality
                js_ep_match = re.search(r"\[(E\d{1,3})\]", raw_quality)
                js_ep_str = js_ep_match.group(1) if js_ep_match else ""

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
                    combined_text,
                    re.IGNORECASE,
                )
                if q_match:
                    quality = q_match.group(1).lower()

                src_match = re.search(
                    r"\b(WEB-DL|WEBRip|BluRay|HDRip|HDTC|HDTS|CAMRip)\b",
                    combined_text,
                    re.IGNORECASE,
                )
                if src_match:
                    quality += f" {src_match.group(1).upper()}"

                if quality == "HD":
                    q_fb = re.search(
                        r"\b(2160p|1080p|720p|480p|360p|4K)\b",
                        raw_quality,
                        re.IGNORECASE,
                    )
                    if q_fb:
                        quality = q_fb.group(1).lower()

                # Language
                lang_keywords = [
                    "Hindi", "English", "Tamil", "Telugu",
                    "Malayalam", "Dual Audio", "Multi",
                ]
                langs = []
                for lk in lang_keywords:
                    if re.search(r"\b" + lk + r"\b", combined_text, re.IGNORECASE):
                        langs.append(lk.title())
                languages = ", ".join(sorted(set(langs))) if langs else lang_str

                # File size
                if not file_size or file_size.lower() in ("", "n/a", "unknown"):
                    sz_m = re.search(
                        r"(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))", combined_text
                    )
                    file_size = (
                        sz_m.group(1).strip().upper().replace(" ", "")
                        if sz_m
                        else ""
                    )

                # Server name
                m_srv = re.search(
                    r"(?i)download\s*\[(.+?)\]", srv_name_raw or ""
                )
                srv_name = (
                    m_srv.group(1).strip() if m_srv else (srv_name_raw or "").strip()
                )
                if not srv_name:
                    srv_name = "Download Server"

                # ── UPSERT: movie_files record ───────────────────────
                cur.execute(
                    "SELECT id FROM movie_files "
                    "WHERE movie_id=%s AND quality=%s AND server_name=%s AND extra_info=%s",
                    (movie_id, quality, srv_name, ep_str),
                )

                if cur.fetchone():
                    cur.execute(
                        """
                        UPDATE movie_files
                        SET url=%s, file_size=%s, languages=%s, source='scraped'
                        WHERE movie_id=%s AND quality=%s AND server_name=%s AND extra_info=%s
                        """,
                        (
                            srv_url, file_size, languages,
                            movie_id, quality, srv_name, ep_str,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO movie_files
                            (movie_id, quality, server_name, url,
                             file_size, languages, extra_info, source)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,'scraped')
                        """,
                        (
                            movie_id, quality, srv_name, srv_url,
                            file_size, languages, ep_str,
                        ),
                    )

        conn.commit()
        cur.close()
        conn.close()
        print(f"   💾 DB Sync Complete: '{title}'", flush=True)

    except Exception as e:
        print(f"   ❌ DB Save Error: {e}", flush=True)
        if conn:
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass


# =====================================================================
# CORE: PROCESS A SINGLE MOVIE URL
# =====================================================================
async def scrape_and_save_movie(
    movie_url, plugin, browser, main_context, sem, is_watchdog=False
):
    """
    Full pipeline for one movie URL:
      1. DB check  →  skip if already stored (matrix mode only)
      2. Navigate  →  plugin.extract_movie_data()
      3. Bypass    →  plugin.bypass_links()
      4. Verify    →  skip if links unchanged (watchdog only)
      5. TMDB      →  enrich with metadata
      6. Save      →  upsert into PostgreSQL
    """
    async with sem:
        # ── Step 1: Matrix skip ──────────────────────────────────────
        if not is_watchdog and check_movie_in_db(movie_url):
            print(f"⏩ SKIP (in DB): {movie_url}", flush=True)
            return

        print(f"\n🎬 PROCESSING: {movie_url}", flush=True)

        # Watchdog: pre-fetch existing data for smart verification
        existing_movie_id = None
        existing_db_title = None
        existing_file_urls = set()
        if is_watchdog:
            existing_movie_id, existing_db_title, existing_file_urls = (
                get_existing_file_urls(movie_url)
            )

        page = await main_context.new_page()
        try:
            await page.goto(
                movie_url, timeout=60000, wait_until="domcontentloaded"
            )

            # ── Step 2: Extract via plugin ───────────────────────────
            scraped_data = await plugin.extract_movie_data(page)
        except Exception as e:
            print(f"   ❌ Page load / extract error: {e}", flush=True)
            return
        finally:
            if not page.is_closed():
                await page.close()

        if not scraped_data:
            print(f"   ⚠️ SKIP: Plugin returned no data for {movie_url}", flush=True)
            return

        raw_links = scraped_data.pop("raw_download_links", [])
        if not raw_links:
            print(f"   ⚠️ SKIP: No download links on {movie_url}", flush=True)
            return

        # ── Step 3: Bypass via plugin ────────────────────────────────
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
            return

        # ── Step 4: Watchdog smart-verify ────────────────────────────
        if is_watchdog and existing_movie_id:
            all_new_urls = set()
            for bl in bypassed_links:
                for dl in bl.get("direct_links", []):
                    if dl.get("url"):
                        all_new_urls.add(dl["url"])

            if all_new_urls and all_new_urls.issubset(existing_file_urls):
                print(
                    f"   ✅ VERIFY: No changes for '{existing_db_title}'. Skipping.",
                    flush=True,
                )
                return

        # ── Step 5: TMDB enrichment ──────────────────────────────────
        fixed_data = fix_movie_details(scraped_data, movie_url=movie_url)
        tmdb_data = await asyncio.to_thread(get_tmdb_details, fixed_data)

        # ── Step 6: DB upsert ────────────────────────────────────────
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


# =====================================================================
# MODE: MATRIX  (Historical Bulk Scraping)
# =====================================================================
async def run_matrix_mode(plugin, bot_id, total_bots):
    """Scrape ALL URLs from the plugin, split across N bots."""
    print("=" * 60, flush=True)
    print(
        f"🚀 MATRIX MODE | Site: {plugin.SITE_NAME} "
        f"| Bot #{bot_id}/{total_bots}",
        flush=True,
    )
    print(f"⏰ Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    start_time = time.time()

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

        # ── Phase 1: URL Discovery ───────────────────────────────────
        print("\n📥 Phase 1: Discovering URLs...", flush=True)
        discovery_ctx = await browser.new_context(user_agent=USER_AGENT)
        all_urls = await plugin.get_all_urls(discovery_ctx)
        await discovery_ctx.close()

        if not all_urls:
            print("❌ No URLs discovered. Exiting.", flush=True)
            await browser.close()
            return

        # ── Phase 2: Split workload ──────────────────────────────────
        total = len(all_urls)
        chunk_size = max(1, total // total_bots)
        start_idx = (bot_id - 1) * chunk_size
        end_idx = total if bot_id == total_bots else start_idx + chunk_size
        my_urls = all_urls[start_idx:end_idx]

        print(
            f"📋 Bot #{bot_id}: Assigned {len(my_urls)} of {total} URLs "
            f"(range [{start_idx}:{end_idx}])",
            flush=True,
        )

        # ── Phase 3: Scraping ────────────────────────────────────────
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

        for i in range(0, len(my_urls), BATCH_SIZE):
            if time.time() - start_time > MAX_RUN_TIME_SECONDS:
                print("⏳ Time limit reached. Stopping gracefully.", flush=True)
                break

            batch = my_urls[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(my_urls) + BATCH_SIZE - 1) // BATCH_SIZE
            print(
                f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} URLs)...",
                flush=True,
            )

            tasks = [
                scrape_and_save_movie(
                    url, plugin, browser, main_ctx, sem, is_watchdog=False
                )
                for url in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Log any unhandled exceptions from the gather
            for url, result in zip(batch, results):
                if isinstance(result, Exception):
                    print(f"   ❌ Unhandled: {url} → {result}", flush=True)

        await main_ctx.close()
        await browser.close()

    elapsed = time.time() - start_time
    print(
        f"\n✅ Matrix Bot #{bot_id} finished in {elapsed / 60:.1f} min! 🎉",
        flush=True,
    )


# =====================================================================
# MODE: WATCHDOG  (Daily Quick Sync)
# =====================================================================
async def run_watchdog_mode(plugin):
    """Scrape only the top N most-recent URLs for daily updates."""
    print("=" * 60, flush=True)
    print(
        f"🐕 WATCHDOG MODE | Site: {plugin.SITE_NAME} "
        f"| Limit: {plugin.WATCHDOG_LIMIT}",
        flush=True,
    )
    print(f"⏰ Started: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)

    start_time = time.time()

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

        # ── Phase 1: URL Discovery ───────────────────────────────────
        print("\n📥 Discovering latest URLs...", flush=True)
        discovery_ctx = await browser.new_context(user_agent=USER_AGENT)
        all_urls = await plugin.get_all_urls(discovery_ctx, watchdog_mode=True)
        await discovery_ctx.close()

        if not all_urls:
            print("❌ No URLs discovered. Exiting.", flush=True)
            await browser.close()
            return

        # ── Phase 2: Slice top N ─────────────────────────────────────
        watchdog_urls = all_urls[: plugin.WATCHDOG_LIMIT]
        print(
            f"📋 Watchdog scanning top {len(watchdog_urls)} URLs...\n",
            flush=True,
        )

        # ── Phase 3: Sequential scraping with smart verify ───────────
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

        for url in watchdog_urls:
            if time.time() - start_time > MAX_RUN_TIME_SECONDS:
                print("⏳ Time limit reached.", flush=True)
                break
            try:
                await scrape_and_save_movie(
                    url, plugin, browser, main_ctx, sem, is_watchdog=True
                )
            except Exception as e:
                print(f"   ❌ Unhandled error for {url}: {e}", flush=True)
                continue

        await main_ctx.close()
        await browser.close()

    elapsed = time.time() - start_time
    print(
        f"\n✅ Watchdog complete in {elapsed / 60:.1f} min! DB synced. 🎉",
        flush=True,
    )


# =====================================================================
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
        choices=["matrix", "watchdog"],
        help="'matrix' for bulk historical, 'watchdog' for daily sync",
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

    # ── Dispatch ─────────────────────────────────────────────────────
    if args.mode == "matrix":
        asyncio.run(run_matrix_mode(plugin, args.bot_id, args.total_bots))
    elif args.mode == "watchdog":
        asyncio.run(run_watchdog_mode(plugin))


if __name__ == "__main__":
    main()
