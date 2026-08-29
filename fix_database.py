"""
fix_database.py — FlimfyBox DB Data Repair Script
==================================================
Ye script scraper bot ki galtiyan theek karta hai:

PROBLEM 1 (movie_files):
  - quality column mein "EPiSODE 1", "Season 1 Episode 2" jaisi values hain
    jabki hona chahiye "480p", "720p" etc.
  - episode info extra_info mein jaani chahiye (e.g. "S04E01")
  - languages column blank hai — URL se parse karenge

PROBLEM 2 (movies):
  - genre, rating, description, category, language, cast blank/N/A hain
  - TMDB API se fetch karke fill karenge

Run: python fix_database.py
     python fix_database.py --dry-run   (sirf print karo, DB change nahi)
     python fix_database.py --movie-id 70   (ek specific movie)
"""

import re
import os
import sys
import json
import time
import hashlib
import argparse
import requests
import psycopg2

# =====================================================================
# CONFIG
# =====================================================================
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres.vzixjxeppvpxrhntaidb:l0aDck2NUeD4Jws5@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres"
)
TMDB_API_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
HTTP_TIMEOUT = 8

# =====================================================================
# DB HELPER
# =====================================================================
def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


# =====================================================================
# PART 1 — movie_files FIX
# quality column se episode parse karo, languages URL se nikalo
# =====================================================================

QUALITY_PATTERN = re.compile(r'(?i)\b(4k|2160p|1080p|720p|576p|480p|360p|camrip|hdtc|hd)\b')
SIZE_PATTERN    = re.compile(r'(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))')

LANG_URL_MAP = {
    'dual':      'Hindi-English',
    'multi':     'Multi',
    'hindi':     'Hindi',
    'english':   'English',
    'tamil':     'Tamil',
    'telugu':    'Telugu',
    'malayalam': 'Malayalam',
    'kannada':   'Kannada',
    'punjabi':   'Punjabi',
    'marathi':   'Marathi',
    'bengali':   'Bengali',
}


def parse_episode_from_text(text):
    """
    'EPiSODE 1' / 'Season 4 Episode 1' / 'S04E01' se
    (season_num_or_None, ep_num_or_None, formatted_str) return karo.
    """
    if not text:
        return None, None, ""

    m = re.search(r'(?i)s(\d{1,2})\s*e(\d{1,3})', text)
    if m:
        s, e = int(m.group(1)), int(m.group(2))
        return s, e, f"S{s:02d}E{e:02d}"

    m = re.search(r'(?i)season\s*(\d+)\s*(?:episode|ep|epi)?\s*(\d+)', text)
    if m:
        s, e = int(m.group(1)), int(m.group(2))
        return s, e, f"S{s:02d}E{e:02d}"

    m = re.search(r'(?i)(?:episode|ep|epi|episod[eo]?)\s*(\d+)', text)
    if m:
        e = int(m.group(1))
        return None, e, f"E{e:02d}"

    return None, None, ""


def parse_quality_from_text(text):
    """'EPiSODE 1 480p WEB-DL' → '480p'"""
    if not text: return ''
    m = QUALITY_PATTERN.search(text)
    if m:
        # Pura 40 chars lene se URL extension (mkv) aa jata hai
        # Sirf quality format aur next word agar clean hai toh lete hain
        q_base = m.group(1)
        tail = text[m.end():m.end()+15].strip()
        tail_word = re.split(r'[^a-zA-Z0-9-]', tail)[0]
        if tail_word.upper() in ('HEVC', 'WEB-DL', '10BIT', 'BLURAY', 'HDRIP'):
            return f"{q_base} {tail_word}"
        return q_base
    return ''


def parse_lang_from_url(url):
    if not url:
        return ""
    u = url.lower()
    if 'dual' in u:
        return 'Hindi-English'
    if 'multi' in u:
        return 'Multi'
    found = []
    for kw, name in LANG_URL_MAP.items():
        if kw in u and name not in found:
            found.append(name)
    return '-'.join(found) if found else ""


def parse_size_from_url(url):
    if not url:
        return ""
    m = SIZE_PATTERN.search(url)
    if m:
        return m.group(1).strip().upper().replace(' ', '')
    return ""


def fix_movie_files(conn, dry_run=False, movie_id_filter=None):
    cur = conn.cursor()

    if movie_id_filter:
        cur.execute("""
            SELECT id, movie_id, quality, url, file_size, languages, extra_info
            FROM movie_files WHERE movie_id = %s ORDER BY id
        """, (movie_id_filter,))
    else:
        cur.execute("""
            SELECT id, movie_id, quality, url, file_size, languages, extra_info
            FROM movie_files WHERE source = 'scraped' ORDER BY movie_id, id
        """)

    rows = cur.fetchall()
    print(f"\n=== STEP 2: movie_files fix ({len(rows)} rows) ===")
    fixed = 0

    for row in rows:
        (fid, mid, quality, url, file_size, languages, extra_info) = row
        quality_orig   = quality    or ""
        url_str        = url        or ""
        new_quality    = quality_orig
        new_extra_info = extra_info or ""
        new_languages  = languages  or ""
        new_file_size  = file_size  or ""
        changed = False

        # 1. Episode info quality column se extra_info mein
        _, _, ep_str = parse_episode_from_text(quality_orig)
        if ep_str and not new_extra_info:
            new_extra_info = ep_str
            # quality se episode strip karke asli quality lo
            real_q = parse_quality_from_text(quality_orig)
            if not real_q:
                real_q = parse_quality_from_text(url_str.split('/')[-1])
            new_quality = real_q if real_q else "Unknown"
            changed = True

        # 2. Languages blank → URL se
        if not new_languages or new_languages.lower() in ('n/a', 'unknown', 'none', ''):
            lang = parse_lang_from_url(url_str)
            if lang:
                new_languages = lang
                changed = True

        # 3. file_size blank → URL se
        if not new_file_size or new_file_size.lower() in ('n/a', 'unknown', ''):
            sz = parse_size_from_url(url_str)
            if sz:
                new_file_size = sz
                changed = True

        if changed:
            fixed += 1
            print(f"  [id={fid}] movie_id={mid}")
            if new_quality    != quality_orig:       print(f"    quality:    '{quality_orig}' → '{new_quality}'")
            if new_extra_info != (extra_info or ""): print(f"    extra_info: '{extra_info}' → '{new_extra_info}'")
            if new_languages  != (languages  or ""): print(f"    languages:  '{languages}' → '{new_languages}'")
            if new_file_size  != (file_size  or ""): print(f"    file_size:  '{file_size}' → '{new_file_size}'")

            if not dry_run:
                try:
                    cur.execute("""
                        UPDATE movie_files
                        SET quality = %s, extra_info = %s, languages = %s, file_size = %s
                        WHERE id = %s
                    """, (new_quality, new_extra_info, new_languages, new_file_size, fid))
                except psycopg2.errors.UniqueViolation:
                    conn.rollback()
                    # Agar duplicate aata hai, purana record delete kar do kyunki naya aa chuka hai
                    cur.execute("DELETE FROM movie_files WHERE id = %s", (fid,))
                    conn.commit()
                    print(f"    ⚠️ Duplicate conflict! Deleted old record id={fid}.")

    if not dry_run:
        conn.commit()
    print(f"  ✅ {fixed} rows fixed")
    cur.close()


# =====================================================================
# PART 2 — movies METADATA FIX (TMDB se fill karo)
# =====================================================================

def tmdb_search(title, year=None, is_tv=False):
    kind = 'tv' if is_tv else 'movie'
    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_API_KEY}&query={requests.utils.quote(title)}"
    try:
        results = requests.get(url, timeout=HTTP_TIMEOUT).json().get('results', [])
        for res in results:
            if res.get('media_type') != kind:
                continue
            d = res.get('release_date') or res.get('first_air_date', '')
            if year and d and d.startswith(str(year)):
                return res
        for res in results:
            if res.get('media_type') == kind:
                return res
        return results[0] if results else None
    except Exception as e:
        print(f"    ⚠️ TMDB search error: {e}")
        return None


def tmdb_get_details(tmdb_id, is_tv=False):
    kind = 'tv' if is_tv else 'movie'
    url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}?api_key={TMDB_API_KEY}&append_to_response=credits,videos"
    try:
        return requests.get(url, timeout=HTTP_TIMEOUT).json()
    except Exception as e:
        print(f"    ⚠️ TMDB detail error: {e}")
        return {}


LANG_CODE_MAP = {
    'hi':'Hindi','en':'English','ta':'Tamil','te':'Telugu',
    'ml':'Malayalam','kn':'Kannada','pa':'Punjabi','mr':'Marathi',
    'bn':'Bengali','ja':'Japanese','ko':'Korean','zh':'Chinese',
    'fr':'French','es':'Spanish','de':'German','it':'Italian',
}


def fix_movies_metadata(conn, dry_run=False, movie_id_filter=None):
    cur = conn.cursor()

    if movie_id_filter:
        cur.execute("""
            SELECT id, title, year, genre, rating, description,
                   category, language, "cast", trailer_key, imdb_id
            FROM movies WHERE id = %s
        """, (movie_id_filter,))
    else:
        cur.execute("""
            SELECT id, title, year, genre, rating, description,
                   category, language, "cast", trailer_key, imdb_id
            FROM movies
            WHERE (genre        IS NULL OR genre        IN ('', 'N/A', 'Unknown'))
               OR (rating       IS NULL OR rating       IN ('', 'N/A', 'Unknown'))
               OR (description  IS NULL OR description  IN ('', 'N/A', 'Unknown'))
            ORDER BY id
        """)

    rows = cur.fetchall()
    print(f"\n=== STEP 3: movies metadata fix ({len(rows)} rows) ===")
    fixed = 0

    for row in rows:
        (mid, title, year, genre, rating, description,
         category, language, cast, trailer_key, imdb_id) = row

        print(f"\n  [{mid}] '{title}' (year={year})")

        tmdb_details = {}
        is_tv = False

        # TMDB ID exist kare aur valid ho (< 900M = real TMDB ID)
        if imdb_id and str(imdb_id).isdigit() and int(imdb_id) < 900_000_000:
            tmdb_details = tmdb_get_details(imdb_id, is_tv=False)
            if not tmdb_details.get('id'):
                tmdb_details = tmdb_get_details(imdb_id, is_tv=True)
                is_tv = bool(tmdb_details.get('id'))
        elif title and title not in ('N/A', 'Unknown', ''):
            res = tmdb_search(title, year=year, is_tv=False)
            if not res:
                res = tmdb_search(title, year=year, is_tv=True)
                is_tv = True if res else False
            if res:
                tmdb_details = tmdb_get_details(res.get('id'), is_tv=is_tv)

        if not tmdb_details.get('id'):
            print(f"    ⚠️ TMDB match nahi mila — skip")
            continue

        # Build fields
        genres_list = tmdb_details.get('genres', [])
        new_genre   = ', '.join(g['name'] for g in genres_list if g.get('name')) or None

        credits = tmdb_details.get('credits', {})
        cast_list = credits.get('cast', [])
        new_cast = ', '.join(c['name'] for c in cast_list[:5] if c.get('name')) or None

        new_description = (tmdb_details.get('overview') or '').strip() or None

        rating_val = tmdb_details.get('vote_average')
        new_rating = f"{rating_val:.1f}" if rating_val else None

        new_tmdb_id = str(tmdb_details.get('id', '')) or None

        orig_lang = tmdb_details.get('original_language', '')
        new_language = LANG_CODE_MAP.get(orig_lang, orig_lang.upper() if orig_lang else None)

        backdrop = tmdb_details.get('backdrop_path')
        poster_p = tmdb_details.get('poster_path')
        new_poster = (
            f"https://image.tmdb.org/t/p/w1280{backdrop}" if backdrop
            else (f"https://image.tmdb.org/t/p/w500{poster_p}" if poster_p else None)
        )

        videos = tmdb_details.get('videos', {}).get('results', [])
        new_trailer = next(
            (v['key'] for v in videos if v.get('type') == 'Trailer' and v.get('site') == 'YouTube'),
            None
        )

        t = (title or '').lower()
        g = (new_genre or '').lower()
        new_category = (
            'Anime'     if 'anime' in t or 'anime' in g else
            'Series'    if is_tv else
            'Animation' if 'animation' in g else
            'Movies'
        )

        release = tmdb_details.get('release_date') or tmdb_details.get('first_air_date') or ''
        new_year = int(release[:4]) if release and release[:4].isdigit() else year

        # Decide kya update karna hai
        def stale(v):
            return not v or str(v).strip() in ('', 'N/A', 'Unknown', 'None')

        updates = {}
        if stale(genre)       and new_genre:        updates['genre']       = new_genre
        if stale(rating)      and new_rating:        updates['rating']      = new_rating
        if stale(description) and new_description:   updates['description'] = new_description[:1500]
        if stale(cast)        and new_cast:          updates['cast']        = new_cast
        if stale(trailer_key) and new_trailer:       updates['trailer_key'] = new_trailer
        if stale(category)    and new_category:      updates['category']    = new_category
        if stale(language)    and new_language:      updates['language']    = new_language
        if new_poster:                               updates['poster_url']  = new_poster
        if stale(imdb_id)     and new_tmdb_id:      updates['imdb_id']     = new_tmdb_id
        if new_year and new_year != year:            updates['year']        = new_year

        if not updates:
            print(f"    ✓ Already complete")
            continue

        fixed += 1
        for k, v in updates.items():
            print(f"    {k}: '{str(v)[:80]}'")

        if not dry_run:
            set_clause = ', '.join(f'"{k}" = %s' for k in updates)
            vals = list(updates.values()) + [mid]
            cur.execute(f"UPDATE movies SET {set_clause} WHERE id = %s", vals)

        time.sleep(0.3)  # TMDB rate limit

    if not dry_run:
        conn.commit()
    print(f"\n  ✅ {fixed} movies fixed")
    cur.close()


# =====================================================================
# PART 0.5 — FIX SCHEMA (Unique constraint for TV Shows)
# =====================================================================
def update_db_schema_for_episodes(conn, dry_run=False):
    """
    Purana constraint (movie_id, quality, server_name) tha.
    Agar TV show hai aur quality="480p" set ho gayi (E01, E02 ke liye),
    toh duplicate error aayega. Isliye constraint mein extra_info add karna hai.
    """
    if dry_run:
        return
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE movie_files DROP CONSTRAINT IF EXISTS unique_movie_quality_server;")
        cur.execute("ALTER TABLE movie_files ADD CONSTRAINT unique_movie_quality_server UNIQUE (movie_id, quality, server_name, extra_info);")
        conn.commit()
        print("✅ DB Schema updated to allow multiple episodes per quality!")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Could not update schema (might already be fixed): {e}")
    finally:
        cur.close()


# =====================================================================
# PART 0 — server_name cleanup
# "Download [Buzz Server]" → "Buzz Server"
# =====================================================================
SERVER_NAME_RE = re.compile(r'(?i)download\s*\[(.+?)\]')

def clean_server_names(conn, dry_run=False):
    cur = conn.cursor()
    cur.execute("SELECT id, server_name FROM movie_files WHERE server_name ILIKE 'Download [%'")
    rows = cur.fetchall()
    print(f"\n=== STEP 1: server_name cleanup ({len(rows)} rows) ===")
    fixed = 0
    for (fid, srv) in rows:
        m = SERVER_NAME_RE.search(srv or '')
        if m:
            clean = m.group(1).strip()
            print(f"  [{fid}] '{srv}' → '{clean}'")
            if not dry_run:
                cur.execute("UPDATE movie_files SET server_name = %s WHERE id = %s", (clean, fid))
            fixed += 1
    if not dry_run:
        conn.commit()
    print(f"  ✅ {fixed} rows cleaned")
    cur.close()


# =====================================================================
# MAIN
# =====================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FlimfyBox DB Fixer")
    parser.add_argument('--dry-run',     action='store_true', help="Sirf print, DB change nahi")
    parser.add_argument('--movie-id',    type=int, default=None, help="Ek specific movie_id")
    parser.add_argument('--skip-files',  action='store_true', help="movie_files step skip")
    parser.add_argument('--skip-movies', action='store_true', help="movies metadata step skip")
    args = parser.parse_args()

    print("=" * 60)
    print("🔧 FlimfyBox Database Fixer")
    print(f"   dry_run  = {args.dry_run}")
    print(f"   movie_id = {args.movie_id or 'ALL'}")
    print("=" * 60)

    try:
        conn = get_conn()
        print("✅ Database connected!")
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        sys.exit(1)

    try:
        update_db_schema_for_episodes(conn, dry_run=args.dry_run)
        clean_server_names(conn, dry_run=args.dry_run)
        if not args.skip_files:
            fix_movie_files(conn, dry_run=args.dry_run, movie_id_filter=args.movie_id)
        if not args.skip_movies:
            fix_movies_metadata(conn, dry_run=args.dry_run, movie_id_filter=args.movie_id)
    finally:
        conn.close()
        print("\n🏁 Done!")
