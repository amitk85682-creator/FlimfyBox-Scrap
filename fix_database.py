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
  - seasons_data bhi fill karenge TV series ke liye

Run: python fix_database.py
     python fix_database.py --dry-run   (sirf print karo, DB change nahi)
     python fix_database.py --movie-id 70   (ek specific movie)
"""

import re
import os
import sys
import json
import time
import urllib.parse
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
HTTP_TIMEOUT = 10

# =====================================================================
# DB HELPER
# =====================================================================
def get_conn():
    return psycopg2.connect(DATABASE_URL, connect_timeout=15)


# =====================================================================
# PART 1 — movie_files FIX
# quality column se episode parse karo, languages URL se nikalo
# Aligned with scraper.py's upsert_file() logic
# =====================================================================

QUALITY_PATTERN = re.compile(r'(?i)\b(4k|2160p|1080p|720p|576p|480p|360p)\b')
SOURCE_PATTERN = re.compile(r'(?i)\b(WEB-DL|WEBRip|BluRay|HDRip|HDTC|HDTS|CAMRip)\b')
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


def parse_episode_from_url_filename(url, quality_text='', movie_title='', movie_url=''):
    """
    Scraper.py ki upsert_file() jaisa logic:
    1. URL decode karo
    2. filename parameter se S01E01 pattern dhundo
    3. quality text se [E01] ya [COMBINED] dhundo
    4. Fallback: movie title/url se season number nikalo
    
    Returns: (episode_str, default_season)
    """
    decoded_url = urllib.parse.unquote(url) if url else ""
    filename_match = re.search(r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE)
    actual_filename = filename_match.group(1) if filename_match else decoded_url
    
    combined_text = f"{actual_filename} {quality_text}"
    
    # Detect [COMBINED] and [E01] markers from quality
    is_combined = "[COMBINED]" in quality_text
    js_ep_match = re.search(r'\[(E\d{1,3})\]', quality_text)
    js_ep_str = js_ep_match.group(1) if js_ep_match else ""
    
    # Find default season from movie title/url
    default_season = 1
    s_match = re.search(r'(?i)\bseason\s*(\d+)', f"{movie_title} {movie_url}")
    if s_match:
        default_season = int(s_match.group(1))
    
    # Try S01E01 pattern from filename
    ep_str = ""
    s_e_match = re.search(r'(?i)\bS(\d{1,2})[\s._-]*E(\d{1,3})\b', actual_filename)
    
    if s_e_match:
        ep_str = f"S{int(s_e_match.group(1)):02d}E{int(s_e_match.group(2)):02d}"
    else:
        if js_ep_str:
            ep_str = f"S{default_season:02d}{js_ep_str}"
        elif is_combined or re.search(r'(?i)\b(batch|full season|complete|all episodes|pack|zip)\b', combined_text):
            ep_str = f"S{default_season:02d} Combined"
    
    return ep_str, default_season


def parse_quality_from_combined(url, quality_text=''):
    """
    Scraper.py ki upsert_file() jaisa quality extraction:
    URL decode karke filename se quality nikalo
    """
    decoded_url = urllib.parse.unquote(url) if url else ""
    filename_match = re.search(r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE)
    actual_filename = filename_match.group(1) if filename_match else decoded_url
    
    combined_text = f"{actual_filename} {quality_text}"
    
    quality = "HD"
    q_match = re.search(r'\b(2160p|1080p|720p|480p|360p|4K)\b', combined_text, re.IGNORECASE)
    if q_match:
        quality = q_match.group(1).lower()
    
    src_match = SOURCE_PATTERN.search(combined_text)
    if src_match:
        quality += f" {src_match.group(1).upper()}"
    
    if quality == "HD" and re.search(r'\b(2160p|1080p|720p|480p|360p|4K)\b', quality_text, re.IGNORECASE):
        quality = quality_text
    
    return quality


def parse_lang_from_combined(url, quality_text=''):
    """
    Scraper.py ki upsert_file() jaisa language extraction
    """
    decoded_url = urllib.parse.unquote(url) if url else ""
    filename_match = re.search(r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE)
    actual_filename = filename_match.group(1) if filename_match else decoded_url
    
    combined_text = f"{actual_filename} {quality_text}"
    
    langs = []
    lang_keywords = ['Hindi', 'English', 'Tamil', 'Telugu', 'Malayalam', 'Dual Audio', 'Multi']
    for l in lang_keywords:
        if re.search(r'\b' + l + r'\b', combined_text, re.IGNORECASE):
            langs.append(l.title())
    return ", ".join(sorted(list(set(langs)))) if langs else "Hindi"


def parse_size_from_combined(url, quality_text='', existing_size=''):
    """File size: existing se pehle check, phir URL/quality se"""
    if existing_size and existing_size.lower() not in ('', 'n/a', 'unknown'):
        return existing_size
    
    decoded_url = urllib.parse.unquote(url) if url else ""
    filename_match = re.search(r'filename=["\']?(.*?)["\'\&]', decoded_url, re.IGNORECASE)
    actual_filename = filename_match.group(1) if filename_match else decoded_url
    
    combined_text = f"{actual_filename} {quality_text}"
    size_match = re.search(r'(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))', combined_text)
    return size_match.group(1).strip().upper().replace(' ', '') if size_match else ""


def clean_server_name(server_name_raw):
    """'Download [Buzz Server]' → 'Buzz Server'"""
    m_srv = re.search(r'(?i)download\s*\[(.+?)\]', server_name_raw or '')
    return m_srv.group(1).strip() if m_srv else (server_name_raw or '').strip()


def fix_movie_files(conn, dry_run=False, movie_id_filter=None):
    cur = conn.cursor()

    if movie_id_filter:
        cur.execute("""
            SELECT mf.id, mf.movie_id, mf.quality, mf.url, mf.file_size, mf.languages, mf.extra_info, mf.server_name, m.title, m.url, m.category
            FROM movie_files mf
            JOIN movies m ON mf.movie_id = m.id
            WHERE mf.movie_id = %s ORDER BY mf.id
        """, (movie_id_filter,))
    else:
        cur.execute("""
            SELECT mf.id, mf.movie_id, mf.quality, mf.url, mf.file_size, mf.languages, mf.extra_info, mf.server_name, m.title, m.url, m.category
            FROM movie_files mf
            JOIN movies m ON mf.movie_id = m.id
            WHERE mf.source = 'scraped' ORDER BY mf.movie_id, mf.id
        """)

    rows = cur.fetchall()
    print(f"\n=== STEP 2: movie_files fix ({len(rows)} rows) ===")
    fixed = 0

    for row in rows:
        (fid, mid, quality, url, file_size, languages, extra_info, server_name, m_title, m_url, m_category) = row
        quality_orig   = quality    or ""
        url_str        = url        or ""
        new_quality    = quality_orig
        new_extra_info = extra_info or ""
        new_languages  = languages  or ""
        new_file_size  = file_size  or ""
        new_server     = server_name or ""
        changed = False

        # 1. Server name cleanup
        cleaned_srv = clean_server_name(new_server)
        if cleaned_srv != new_server:
            new_server = cleaned_srv
            changed = True

        # 2. Episode info: URL filename se parse karo (scraper.py style)
        ep_str, _ = parse_episode_from_url_filename(url_str, quality_orig, m_title or '', m_url or '')
        
        # Movies ke liye episode info blank honi chahiye
        is_movie = (m_category or '').lower() in ('movies', 'movie')
        if is_movie:
            ep_str = ""
        
        if ep_str and ep_str != new_extra_info:
            new_extra_info = ep_str
            changed = True

        # 3. Quality: URL filename se proper quality nikalo
        parsed_q = parse_quality_from_combined(url_str, quality_orig)
        if parsed_q and parsed_q != quality_orig and parsed_q != "HD":
            new_quality = parsed_q
            changed = True
        elif 'episode' in quality_orig.lower() or 'ep ' in quality_orig.lower() or re.match(r'^\[E\d+\]', quality_orig):
            # Quality mein episode info hai, clean karo
            real_q = parse_quality_from_combined(url_str, quality_orig)
            if real_q and real_q != "HD":
                new_quality = real_q
            else:
                new_quality = "HD"
            changed = True

        # 4. Languages: URL se parse karo
        if not new_languages or new_languages.lower() in ('n/a', 'unknown', 'none', ''):
            lang = parse_lang_from_combined(url_str, quality_orig)
            if lang:
                new_languages = lang
                changed = True

        # 5. File size: URL se parse karo
        new_sz = parse_size_from_combined(url_str, quality_orig, new_file_size)
        if new_sz and new_sz != new_file_size:
            new_file_size = new_sz
            changed = True

        if changed:
            fixed += 1
            print(f"  [id={fid}] movie_id={mid}")
            if new_quality    != quality_orig:       print(f"    quality:    '{quality_orig}' → '{new_quality}'")
            if new_extra_info != (extra_info or ""): print(f"    extra_info: '{extra_info}' → '{new_extra_info}'")
            if new_languages  != (languages  or ""): print(f"    languages:  '{languages}' → '{new_languages}'")
            if new_file_size  != (file_size  or ""): print(f"    file_size:  '{file_size}' → '{new_file_size}'")
            if new_server     != (server_name or ""): print(f"    server:     '{server_name}' → '{new_server}'")

            if not dry_run:
                try:
                    cur.execute("""
                        UPDATE movie_files
                        SET quality = %s, extra_info = %s, languages = %s, file_size = %s, server_name = %s
                        WHERE id = %s
                    """, (new_quality, new_extra_info, new_languages, new_file_size, new_server, fid))
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
# Aligned with scraper.py's get_tmdb_details() — strict tv/movie search
# =====================================================================

def detect_type_from_title(title, url=''):
    """
    Scraper.py ki fix_movie_details() jaisa type detection:
    Title/URL se TV Series ya Movie detect karo
    """
    combined = f"{title or ''} {url or ''}"
    if re.search(r'(?i)\b(season|episode|series|web series)\b', combined):
        return 'TV Series'
    return 'Movie'


def extract_season_from_title(title, url=''):
    """Title/URL se season number nikalo"""
    combined = f"{title or ''} {url or ''}"
    s_match = re.search(r'(?i)\bseason\s*(\d+)', combined)
    if s_match:
        return int(s_match.group(1))
    s_match = re.search(r'(?i)season-(\d+)', combined)
    if s_match:
        return int(s_match.group(1))
    return 1


def clean_title_for_search(raw_title):
    """
    Scraper.py ki fix_movie_details() jaisa title cleaning:
    Brackets ke andar se year/season nikalo, bahar ka clean title lo
    """
    if not raw_title or raw_title in ('N/A', 'Unknown', ''):
        return raw_title

    t = raw_title.replace('🎬', '').strip()
    
    # Split at first bracket
    title_parts = re.split(r'\(|\[', t)
    search_query = title_parts[0].strip()
    
    # Season text hatao title se
    search_query = re.sub(r'(?i)\bseason\s*\d+.*', '', search_query).strip()
    
    # Common junk words hatao
    junk = r'(?i)\b(uncut|hindi|english|dual\s*audio|dubbed|4k|2160p|1080p|720p|480p|360p|hdrip|webrip|web-dl|web|dl|x264|x265|hevc|esubs?|mb|gb|brrip|dvdrip|hdtc|camrip|aac|dd2\.0|dd5\.1|full\s*movie|movies?)\b'
    search_query = re.sub(junk, ' ', search_query)
    
    # Year hatao
    search_query = re.sub(r'\b(19|20)\d{2}\b', ' ', search_query)
    
    # Brackets, hyphens cleanup
    search_query = re.sub(r'[\(\)\[\]\-\&]+', ' ', search_query)
    
    return re.sub(r'\s+', ' ', search_query).strip()


def tmdb_search_strict(title, is_tv=False):
    """
    Scraper.py ki get_tmdb_details() jaisa strict search:
    TV ke liye /search/tv, Movie ke liye /search/movie
    (search/multi use NAHI karna)
    """
    kind = 'tv' if is_tv else 'movie'
    url = f"https://api.themoviedb.org/3/search/{kind}?api_key={TMDB_API_KEY}&query={requests.utils.quote(title)}"
    try:
        results = requests.get(url, timeout=HTTP_TIMEOUT).json().get('results', [])
        if not results:
            return None
        return results[0]
    except Exception as e:
        print(f"    ⚠️ TMDB search error: {e}")
        return None


def tmdb_get_full_details(tmdb_id, is_tv=False):
    """
    Scraper.py jaisa full details fetch:
    Details + Credits + External IDs + Seasons (for TV)
    """
    kind = 'tv' if is_tv else 'movie'
    result = {}
    
    try:
        # Main details
        details_url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}?api_key={TMDB_API_KEY}"
        details = requests.get(details_url, timeout=HTTP_TIMEOUT).json()
        
        genres = [g['name'] for g in details.get('genres', [])]
        genre_str = ", ".join(genres) if genres else "N/A"
        plot = details.get('overview', 'N/A')
        rating = str(round(details.get('vote_average', 0), 1)) if details.get('vote_average') else 'N/A'
        
        # Credits
        credits_url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/credits?api_key={TMDB_API_KEY}"
        credits = requests.get(credits_url, timeout=HTTP_TIMEOUT).json()
        cast_list = [c['name'] for c in credits.get('cast', [])[:5]]
        cast_str = ", ".join(cast_list) if cast_list else "N/A"
        
        # External IDs (IMDB)
        ext_url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/external_ids?api_key={TMDB_API_KEY}"
        ext_ids = requests.get(ext_url, timeout=HTTP_TIMEOUT).json()
        imdb_id = ext_ids.get('imdb_id', 'N/A')
        
        # Seasons data (TV only)
        seasons_data = {}
        if is_tv:
            for s in details.get('seasons', []):
                s_num = str(s.get('season_number', ''))
                if s_num and s_num != "0":
                    s_air_date = str(s.get('air_date', ''))
                    s_year = int(s_air_date[:4]) if len(s_air_date) >= 4 and s_air_date[:4].isdigit() else 0
                    s_poster = f"https://image.tmdb.org/t/p/original{s.get('poster_path')}" if s.get('poster_path') else None
                    
                    episodes_info = {}
                    try:
                        season_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{s_num}?api_key={TMDB_API_KEY}"
                        season_details = requests.get(season_url, timeout=5).json()
                        for ep in season_details.get('episodes', []):
                            ep_num = str(ep.get('episode_number'))
                            episodes_info[ep_num] = {'air_date': ep.get('air_date', '')}
                    except:
                        pass
                    
                    seasons_data[s_num] = {
                        "year": s_year,
                        "poster": s_poster,
                        "air_date": s_air_date,
                        "episode_count": s.get('episode_count', 0),
                        "episodes": episodes_info
                    }
        
        # Videos (trailer)
        videos_url = f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/videos?api_key={TMDB_API_KEY}"
        videos = requests.get(videos_url, timeout=HTTP_TIMEOUT).json()
        trailer_key = next(
            (v['key'] for v in videos.get('results', []) if v.get('type') == 'Trailer' and v.get('site') == 'YouTube'),
            None
        )
        
        result = {
            "Title": details.get('name') or details.get('title'),
            "Release": details.get('first_air_date') or details.get('release_date', 'N/A'),
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "Genre": genre_str,
            "Description": plot,
            "TMDb_Rating": rating,
            "Cast": cast_str,
            "seasons_data": seasons_data,
            "Poster": f"https://image.tmdb.org/t/p/original{details.get('poster_path')}" if details.get('poster_path') else 'N/A',
            "is_tv": is_tv,
            "trailer_key": trailer_key,
            "original_language": details.get('original_language', ''),
        }
    except Exception as e:
        print(f"    ⚠️ TMDB detail error: {e}")
    
    return result


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
                   category, language, "cast", trailer_key, imdb_id, url, seasons_data
            FROM movies WHERE id = %s
        """, (movie_id_filter,))
    else:
        cur.execute("""
            SELECT id, title, year, genre, rating, description,
                   category, language, "cast", trailer_key, imdb_id, url, seasons_data
            FROM movies
            WHERE (genre        IS NULL OR genre        IN ('', 'N/A', 'Unknown'))
               OR (rating       IS NULL OR rating       IN ('', 'N/A', 'Unknown'))
               OR (description  IS NULL OR description  IN ('', 'N/A', 'Unknown'))
               OR (category     IS NULL OR category     IN ('', 'N/A', 'Unknown'))
               OR (seasons_data IS NULL)
            ORDER BY id
        """)

    rows = cur.fetchall()
    print(f"\n=== STEP 3: movies metadata fix ({len(rows)} rows) ===")
    fixed = 0

    for row in rows:
        (mid, title, year, genre, rating, description,
         category, language, cast, trailer_key, imdb_id, movie_url, seasons_data) = row

        print(f"\n  [{mid}] '{title}' (year={year})")

        # Detect type from title/url
        media_type = detect_type_from_title(title, movie_url)
        is_tv = media_type == 'TV Series'

        # Clean title for search (scraper.py style)
        clean_t = clean_title_for_search(title)
        if not clean_t or clean_t in ('N/A', 'Unknown', ''):
            print(f"    ⚠️ Title clean nahi ho paya — skip")
            continue

        print(f"    🔍 Searching TMDB ({('TV' if is_tv else 'Movie')}): '{clean_t}'")

        tmdb_details = {}

        # TMDB ID exist kare aur valid ho
        if imdb_id and str(imdb_id).isdigit() and int(imdb_id) < 900_000_000:
            tmdb_details = tmdb_get_full_details(int(imdb_id), is_tv=is_tv)
            if not tmdb_details.get('tmdb_id'):
                # Try opposite type
                tmdb_details = tmdb_get_full_details(int(imdb_id), is_tv=not is_tv)
                if tmdb_details.get('tmdb_id'):
                    is_tv = not is_tv
        else:
            # Strict search (scraper.py style: tv ya movie, not multi)
            res = tmdb_search_strict(clean_t, is_tv=is_tv)
            if not res:
                # Try opposite type
                res = tmdb_search_strict(clean_t, is_tv=not is_tv)
                if res:
                    is_tv = not is_tv
            if res:
                tmdb_details = tmdb_get_full_details(res.get('id'), is_tv=is_tv)

        if not tmdb_details.get('tmdb_id'):
            print(f"    ⚠️ TMDB match nahi mila — skip")
            continue

        # Build fields (scraper.py's save_movie_to_db priority: page data pehle, TMDB fallback)
        new_genre = tmdb_details.get('Genre', 'N/A')
        new_rating = tmdb_details.get('TMDb_Rating', 'N/A')
        new_cast = tmdb_details.get('Cast', 'N/A')
        new_description = tmdb_details.get('Description', 'N/A')
        new_imdb_id = tmdb_details.get('imdb_id')
        new_seasons_data = tmdb_details.get('seasons_data', {})
        new_trailer = tmdb_details.get('trailer_key')
        
        # Category: scraper.py style
        new_category = "Web Series" if is_tv else "Movies"
        
        # Poster
        new_poster = tmdb_details.get('Poster', '')
        
        # Language
        orig_lang = tmdb_details.get('original_language', '')
        new_language = LANG_CODE_MAP.get(orig_lang, orig_lang.upper() if orig_lang else None)

        # Year
        release = tmdb_details.get('Release', '')
        new_year = int(release[:4]) if release and release[:4].isdigit() else year

        # Title: TMDB se better title
        new_title = tmdb_details.get('Title')

        # Decide kya update karna hai
        def stale(v):
            return not v or str(v).strip() in ('', 'N/A', 'Unknown', 'None')

        updates = {}
        
        # Title update — TMDB title use karo agar clean hai
        if new_title and title != new_title:
            # Junk check on existing title
            junk_pattern = r'(?i)\b(uncut|hindi|dual\s*audio|dubbed|480p|720p|1080p|hdrip|webrip|web-dl|x264|hevc|esubs?|mb|gb|brrip|dvdrip|hdtc|camrip|x265|aac)\b'
            if re.search(junk_pattern, title or ''):
                updates['title'] = new_title

        if stale(genre)       and not stale(new_genre):       updates['genre']       = new_genre
        if stale(rating)      and not stale(new_rating):      updates['rating']      = new_rating
        if stale(description) and not stale(new_description): updates['description'] = new_description[:1500]
        if stale(cast)        and not stale(new_cast):        updates['"cast"']      = new_cast
        if stale(trailer_key) and new_trailer:                updates['trailer_key'] = new_trailer
        if stale(category)    and new_category:               updates['category']    = new_category
        if stale(language)    and new_language:                updates['language']    = new_language
        if new_poster and new_poster != 'N/A':                updates['poster_url']  = new_poster
        if stale(imdb_id)     and new_imdb_id:                updates['imdb_id']     = new_imdb_id
        if new_year and new_year != year:                     updates['year']        = new_year
        
        # Seasons data: TV series ke liye always update if we have new data
        if is_tv and new_seasons_data:
            existing_seasons = None
            if seasons_data:
                try:
                    existing_seasons = json.loads(seasons_data) if isinstance(seasons_data, str) else seasons_data
                except:
                    existing_seasons = None
            
            if not existing_seasons or len(new_seasons_data) > len(existing_seasons):
                updates['seasons_data'] = json.dumps(new_seasons_data)

        if not updates:
            print(f"    ✓ Already complete")
            continue

        fixed += 1
        for k, v in updates.items():
            display_val = str(v)[:80]
            print(f"    {k}: '{display_val}'")

        if not dry_run:
            try:
                set_clause = ', '.join(f'{k} = %s' if k.startswith('"') else f'"{k}" = %s' for k in updates)
                vals = list(updates.values()) + [mid]
                cur.execute(f"UPDATE movies SET {set_clause} WHERE id = %s", vals)
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                if 'title' in updates:
                    print(f"    ⚠️ Title duplicate conflict! Retrying without title update.")
                    del updates['title']
                    if updates:
                        set_clause = ', '.join(f'{k} = %s' if k.startswith('"') else f'"{k}" = %s' for k in updates)
                        vals = list(updates.values()) + [mid]
                        cur.execute(f"UPDATE movies SET {set_clause} WHERE id = %s", vals)
                        conn.commit()
                else:
                    print(f"    ⚠️ Unique conflict. Skipping id={mid}.")

        time.sleep(0.3)  # TMDB rate limit

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
    print("🔧 FlimfyBox Database Fixer (v2 — aligned with scraper.py)")
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
        if not args.skip_files:
            fix_movie_files(conn, dry_run=args.dry_run, movie_id_filter=args.movie_id)
        if not args.skip_movies:
            fix_movies_metadata(conn, dry_run=args.dry_run, movie_id_filter=args.movie_id)
    finally:
        conn.close()
        print("\n🏁 Done!")
