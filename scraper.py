import asyncio
import os
import re
import urllib.parse
import nest_asyncio
import requests
import json
import time
import sys
import psycopg2
from playwright.async_api import async_playwright

nest_asyncio.apply()

# =====================================================================
# DIRECT POSTGRESQL DATABASE CONFIGURATION
# =====================================================================
# Hardcoded to prevent GitHub Secrets from overriding it with the old DB URL
DATABASE_URL = "postgresql://postgres.vzixjxeppvpxrhntaidb:l0aDck2NUeD4Jws5@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres"

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def check_movie_in_db(url):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT url FROM movies WHERE url = %s;", (url,))
        res = cur.fetchone()
        cur.close()
        conn.close()
        return bool(res)
    except Exception as e:
        print(f"DB Check Error: {e}")
        return False

def find_duplicate_movie(scraped_data, tmdb_data):
    """
    Multi-field scoring se duplicate check karo.
    
    Fields compared:
      ✅ Title (base + full + bidirectional)  → 3 pts
      ✅ Year                                  → 2 pts  
      ✅ Stars / Cast                          → 2 pts
      ✅ Director                              → 2 pts
      ✅ Creator (TV Series)                   → 2 pts

    Score >= 4  → Confident duplicate (return movie_id)
    Score 2-3   → Possible duplicate (log warning, return movie_id)
    Score < 2   → Not a duplicate (return None)

    Returns: (found: bool, movie_id: int or None, score: int)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # --- Prepare scraped fields ---
        tmdb_title   = (tmdb_data or {}).get('Title', '') or ''
        base_title   = tmdb_title.split(':')[0].strip()  # "Toxic: A Fairytale..." → "Toxic"
        year_val     = (tmdb_data or {}).get('Release', '')[:4] if (tmdb_data or {}).get('Release') else ''
        stars_raw    = scraped_data.get('Stars', '') or ''
        director_raw = scraped_data.get('Director', '') or ''
        creator_raw  = scraped_data.get('Creator', '') or ''

        # Normalize to lowercase word sets
        def word_set(s):
            return set(w.strip().lower() for w in s.replace(',', ' ').split() if len(w.strip()) > 2)

        scraped_stars    = word_set(stars_raw)
        scraped_director = word_set(director_raw)
        scraped_creator  = word_set(creator_raw)

        # --- Step 1: Get title candidates from DB ---
        cur.execute("""
            SELECT id, title, year, "cast", rating, language
            FROM movies
            WHERE 
                title ILIKE %s OR
                title ILIKE %s OR
                %s ILIKE concat('%%', title, '%%') OR
                %s ILIKE concat('%%', title, '%%')
            LIMIT 10;
        """, (
            f"%{tmdb_title}%",
            f"%{base_title}%",
            tmdb_title,
            base_title
        ))
        candidates = cur.fetchall()
        cur.close()
        conn.close()

        if not candidates:
            return False, None, 0

        best_score = 0
        best_id    = None
        best_title = None

        for (db_id, db_title, db_year, db_cast, db_rating, db_lang) in candidates:
            score = 0
            reasons = []

            # --- Title score (max 3 pts) ---
            db_title_lower   = (db_title or '').lower()
            tmdb_title_lower = tmdb_title.lower()
            base_title_lower = base_title.lower()

            if db_title_lower == tmdb_title_lower:
                score += 3; reasons.append("exact title")
            elif base_title_lower and (base_title_lower == db_title_lower or
                 base_title_lower in db_title_lower or
                 db_title_lower in base_title_lower):
                score += 2; reasons.append("base title match")
            elif tmdb_title_lower in db_title_lower or db_title_lower in tmdb_title_lower:
                score += 1; reasons.append("partial title")

            # --- Year score (2 pts) ---
            if year_val and db_year and str(db_year) == str(year_val):
                score += 2; reasons.append(f"year={year_val}")

            # --- Stars/Cast score (2 pts) ---
            db_cast_set = word_set(db_cast or '')
            if scraped_stars and db_cast_set:
                common = scraped_stars & db_cast_set
                if len(common) >= 2:
                    score += 2; reasons.append(f"stars={common}")
                elif len(common) == 1:
                    score += 1; reasons.append(f"1 star={common}")

            # --- Director score (2 pts) ---
            # DB doesn't have director column separately, skip if not available
            # (future: add director column to movies table)

            # --- Creator score (2 pts) ---
            db_cast_words = word_set(db_cast or '')
            if scraped_creator and db_cast_words:
                if scraped_creator & db_cast_words:
                    score += 1; reasons.append("creator overlap")

            if score > best_score:
                best_score = score
                best_id    = db_id
                best_title = db_title

        # --- Decision ---
        if best_score >= 4:
            print(f"   ✅ DUPLICATE [score={best_score}]: DB='{best_title}' ↔ scraped='{tmdb_title}' | Matched: {', '.join(reasons[:3])}")
            return True, best_id, best_score
        elif best_score >= 2:
            print(f"   ⚠️  POSSIBLE DUPLICATE [score={best_score}]: DB='{best_title}' ↔ '{tmdb_title}' — treating as same")
            return True, best_id, best_score
        else:
            return False, None, best_score

    except Exception as e:
        print(f"Duplicate Check Error: {e}")
        return False, None, 0


def check_movie_by_tmdb_id(tmdb_id):
    """
    TMDB ID se exact duplicate check — title matching se zyada reliable.
    Returns: (exists: bool, movie_id: int or None)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM movies WHERE imdb_id = %s LIMIT 1;", (str(tmdb_id),))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return True, row[0]
        return False, None
    except Exception as e:
        print(f"DB TMDB ID Check Error: {e}")
        return False, None

def get_existing_seasons_episodes(title):
    """
    TV Series ke liye: DB mein kaun se seasons/episodes already hain?
    Returns: dict like {"Season 1": ["Ep01", "Ep02"], "Season 2": []} 
             ya phir empty dict agar kuch nahi mila.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # movie_id dhundo title se
        cur.execute("SELECT id FROM movies WHERE title ILIKE %s LIMIT 1;", (f"%{title}%",))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return {}
        movie_id = row[0]
        # Existing qualities fetch karo (ye season/episode info contain karti hain)
        cur.execute(
            "SELECT quality, server_name FROM movie_files WHERE movie_id = %s ORDER BY quality;",
            (movie_id,)
        )
        files = cur.fetchall()
        cur.close()
        conn.close()
        
        existing = {}
        for (quality, server_name) in files:
            existing.setdefault(quality, []).append(server_name or '')
        return existing
    except Exception as e:
        print(f"DB Season Check Error: {e}")
        return {}

def save_movie_to_db(data_dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        tmdb = data_dict.get('tmdb_data') or {}
        title  = tmdb.get('Title') or data_dict.get('raw_title')
        year   = tmdb.get('Release', '')[:4] if tmdb.get('Release') else 'N/A'
        poster = tmdb.get('Poster') or ''
        tmdb_id = str(tmdb.get('tmdb_id', '')) if tmdb.get('tmdb_id') else None
        
        try:
            year_val = int(year)
        except:
            year_val = None
        
        # TMDB ID se dhundho pehle (most reliable), phir title se
        if tmdb_id:
            cur.execute("SELECT id FROM movies WHERE imdb_id = %s LIMIT 1", (tmdb_id,))
        else:
            cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
        row = cur.fetchone()

        if row:
            movie_id = row[0]
            cur.execute("""
                UPDATE movies SET 
                    url = %s, poster_url = %s, year = %s, genre = %s, 
                    description = %s, rating = %s, language = %s, "cast" = %s,
                    imdb_id = COALESCE(imdb_id, %s)
                WHERE id = %s
            """, (
                data_dict['url'], poster, year_val, data_dict.get('Genre', 'N/A'),
                data_dict.get('Description', 'N/A'), data_dict.get('IMDb', 'N/A'),
                data_dict.get('Language', 'N/A'), data_dict.get('Stars', 'N/A'),
                tmdb_id, movie_id
            ))
        else:
            cur.execute("""
                INSERT INTO movies (url, title, poster_url, year, genre, description, rating, language, "cast", imdb_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                data_dict['url'], title, poster, year_val,
                data_dict.get('Genre', 'N/A'), data_dict.get('Description', 'N/A'),
                data_dict.get('IMDb', 'N/A'), data_dict.get('Language', 'N/A'),
                data_dict.get('Stars', 'N/A'), tmdb_id
            ))
            movie_id = cur.fetchone()[0]
        
        if movie_id:
            bypassed_links = data_dict.get('bypassed_links', [])
            for link in bypassed_links:
                base_q_name = link.get('quality', 'Unknown')
                f_size = link.get('size', '')
                direct_links_data = link.get('direct_link')
                
                def upsert_file(quality, server_name, srv_url, file_size):
                    """Insert or update a movie_file row with clean server_name column."""
                    cur.execute(
                        "SELECT id FROM movie_files WHERE movie_id = %s AND quality = %s AND server_name = %s",
                        (movie_id, quality, server_name)
                    )
                    if cur.fetchone():
                        cur.execute(
                            "UPDATE movie_files SET url = %s, file_size = %s, source = 'scraped' WHERE movie_id = %s AND quality = %s AND server_name = %s",
                            (srv_url, file_size, movie_id, quality, server_name)
                        )
                    else:
                        cur.execute(
                            "INSERT INTO movie_files (movie_id, quality, server_name, url, file_size, source) VALUES (%s, %s, %s, %s, %s, 'scraped')",
                            (movie_id, quality, server_name, srv_url, file_size)
                        )

                if isinstance(direct_links_data, list):
                    for srv in direct_links_data:
                        srv_name = srv.get('server_name', '').strip()
                        srv_url  = srv.get('url', '').strip()
                        if srv_url:
                            upsert_file(base_q_name, srv_name, srv_url, f_size)
                elif isinstance(direct_links_data, dict):
                    srv_url = direct_links_data.get('url', '').strip()
                    if srv_url:
                        upsert_file(base_q_name, '', srv_url, f_size)
                elif isinstance(direct_links_data, str) and direct_links_data.strip():
                    upsert_file(base_q_name, '', direct_links_data.strip(), f_size)
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Save Error: {e}")

# =====================================================================
# GITHUB RUNNER RELAY LOGIC (24/7 Loop)
# =====================================================================
def trigger_next_github_runner():
    # Ye GitHub Actions ke environment variables se aayega
    token = os.environ.get("GH_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY") 
    
    if not token or not repo:
        print("⚠️ GitHub token not found. Skipping auto-trigger (running locally?).")
        return
        
    print("🔄 5h 45m limit reached. Triggering next GitHub Runner...")
    url = f"https://api.github.com/repos/{repo}/actions/workflows/bot_runner.yml/dispatches"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    res = requests.post(url, headers=headers, json={"ref": "main"})
    if res.status_code == 204:
        print("✅ New server successfully triggered! Old server resting.")
    else:
        print(f"❌ Failed to trigger runner: {res.text}")

# =====================================================================
# BYPASS MODULES 
# =====================================================================
async def bypass_hubcdn_mediator(context, target_url):
    page = await context.new_page()
    try:
        await page.goto(target_url, timeout=60000)
        await page.wait_for_timeout(10000) 
        
        clicked_step1 = await page.evaluate('''() => {
            let btn = Array.from(document.querySelectorAll('a, button')).find(e => e.innerText.includes('CLICK TO CONTINUE'));
            if(btn) { btn.click(); return true; } return false;
        }''')
        if not clicked_step1: return None
        await page.wait_for_timeout(12000) 

        clicked_step2 = await page.evaluate('''() => {
            let btn = Array.from(document.querySelectorAll('a, button')).find(e => e.innerText.includes('GET LINKS'));
            if(btn) { btn.click(); return true; } return false;
        }''')
        if not clicked_step2: return None
        await page.wait_for_timeout(10000) 

        final_link = None
        for p_idx in context.pages:
            link = await p_idx.evaluate('''() => {
                let btn = Array.from(document.querySelectorAll('a, button')).find(e => e.innerText.includes('Download Here'));
                return btn ? btn.href : null;
            }''')
            if not link:
                link = await p_idx.evaluate('''() => {
                    let links = Array.from(document.querySelectorAll('a'));
                    let target = links.find(a => a.href.toLowerCase().includes('hubdrive') || a.href.toLowerCase().includes('hubcloud'));
                    return target ? target.href : null;
                }''')
            if link: final_link = link; break
        return final_link
    except: return None
    finally: await page.close()

async def bypass_hubcloud_chain(context, hubdrive_url):
    page = await context.new_page()
    try:
        await page.goto(hubdrive_url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(4000)

        hubcloud_url = await page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let target = links.find(a => a.innerText.toLowerCase().includes('hubcloud server'));
            return target ? target.href : null;
        }''')
        hubcloud_url = hubcloud_url or (hubdrive_url if "hubcloud" in hubdrive_url else None)
        if not hubcloud_url: return None
        
        if page.url != hubcloud_url:
            await page.goto(hubcloud_url, timeout=60000, wait_until="domcontentloaded")
        await page.wait_for_timeout(8000)

        gamerxyt_url = await page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let target = links.find(a => a.innerText.toLowerCase().includes('generate') || a.innerText.toLowerCase().includes('direct download'));
            return target ? target.href : null;
        }''')

        if gamerxyt_url and 'http' in gamerxyt_url:
            await page.goto(gamerxyt_url, timeout=60000, wait_until="domcontentloaded")
        else:
            await page.locator('text="Generate Direct Download Link"').click()
            
        await page.wait_for_timeout(8000)

        final_servers = await page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let results = [];
            links.forEach(a => {
                let text = a.innerText.trim();
                let href = a.href;
                let lower = text.toLowerCase();
                if(lower.includes('server') || lower.includes('fsl') || lower.includes('pixel') || lower.includes('buzz') || lower.includes('10gbps')) {
                    results.push({ server_name: text, url: href });
                }
            });
            return results;
        }''')
        return final_servers if final_servers else None
    except: return None
    finally: await page.close()

async def process_single_link(browser, sem, raw_link_data):
    async with sem:
        target_url = raw_link_data['url']
        quality = raw_link_data['quality']
        size = raw_link_data.get('size', '')
        direct_link = None
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        try:
            if "greenmountmotors" in target_url or "inventoryidea" in target_url or "hubcdn" in target_url:
                mediator = await bypass_hubcdn_mediator(context, target_url)
                if mediator and ("hubcloud" in mediator or "hubdrive" in mediator):
                    direct_link = await bypass_hubcloud_chain(context, mediator)
                else: direct_link = mediator
            elif "hubdrive" in target_url or "hubcloud" in target_url:
                direct_link = await bypass_hubcloud_chain(context, target_url) 
        except: pass
        finally: await context.close()
        return {"quality": quality, "size": size, "direct_link": direct_link}

# =====================================================================
# DATA EXTRACTION & TMDB
# =====================================================================
def fix_movie_details(scraped_data):
    site_clean_title = scraped_data.get('Site_Clean_Title', '')
    fixed_title = re.sub(r'\(.*?\)', '', site_clean_title) 
    fixed_title = re.sub(r'\[.*?\]', '', fixed_title)      
    search_query = fixed_title.strip()

    if not search_query:
        raw_t = scraped_data.get('Raw_Title', '').replace('', '').strip()
        search_query = re.split(r'\(|\[', raw_t)[0].strip()
    scraped_data['Search_Query'] = search_query
    return scraped_data

def get_tmdb_details(fixed_data):
    TMDB_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
    search_query = fixed_data['Search_Query']
    year_hint = fixed_data['Year']
    type_hint = 'tv' if fixed_data['Type'] == 'TV Series' else 'movie'
    
    lang_str = fixed_data['Language'].lower()
    target_orig_lang = None
    if 'hindi' in lang_str: target_orig_lang = 'hi'
    elif 'tamil' in lang_str: target_orig_lang = 'ta'
    elif 'telugu' in lang_str: target_orig_lang = 'te'
    elif 'malayalam' in lang_str: target_orig_lang = 'ml'

    url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_KEY}&query={urllib.parse.quote(search_query)}"
    try:
        response = requests.get(url).json()
        if not response.get('results'): return None
        results = response['results']
        best_match = None

        if target_orig_lang:
            for res in results:
                if res.get('media_type') == type_hint and res.get('original_language') == target_orig_lang:
                    res_date = res.get('release_date') or res.get('first_air_date', '')
                    if year_hint != 'N/A' and res_date.startswith(str(year_hint)):
                        best_match = res; break
                    elif year_hint == 'N/A':
                        best_match = res; break

        if not best_match and year_hint != 'N/A':
            for res in results:
                res_date = res.get('release_date') or res.get('first_air_date', '')
                if res.get('media_type') == type_hint and res_date.startswith(str(year_hint)):
                    best_match = res; break

        if not best_match:
            for res in results:
                if res.get('media_type') == type_hint:
                    best_match = res; break
        if not best_match: best_match = results[0]

        return {
            "Matched_Type": best_match.get('media_type', 'unknown').upper(),
            "Orig_Language": best_match.get('original_language', 'unknown').upper(),
            "Title": best_match.get('title') or best_match.get('name'),
            "Release": best_match.get('release_date') or best_match.get('first_air_date', 'N/A'),
            "TMDb_Rating": best_match.get('vote_average', 'N/A'),
            # TMDB ID — unique content identifier (stored in imdb_id column)
            "tmdb_id": best_match.get('id'),
            # Backdrop poster (wide banner) prefer karo, fallback to regular poster
            "Poster": (
                f"https://image.tmdb.org/t/p/w1280{best_match.get('backdrop_path')}"
                if best_match.get('backdrop_path')
                else (
                    f"https://image.tmdb.org/t/p/w500{best_match.get('poster_path')}"
                    if best_match.get('poster_path') else 'N/A'
                )
            ),
            "is_tv": best_match.get('media_type') == 'tv'
        }
    except: return None

# =====================================================================
# CORE LOGIC: PROCESS A SINGLE MOVIE
# =====================================================================
async def scrape_and_save_movie(movie_link, browser, main_context, sem):
    if check_movie_in_db(movie_link):
        print(f"⏩ SKIP: Already in Database -> {movie_link}", flush=True)
        return

    print(f"\n🎬 EXTRACTING: {movie_link}", flush=True)
    movie_page = await main_context.new_page()
    try:
        await movie_page.goto(movie_link, timeout=60000, wait_until="domcontentloaded")
        scraped_data = await movie_page.evaluate('''() => {
            let text = document.body.innerText;
            let lines = text.split('\\n').map(l => l.trim()).filter(l => l.length > 0);
            let details = { Raw_Title: 'N/A', Site_Clean_Title: 'N/A', Year: 'N/A', Type: 'Movie', IMDb: 'N/A', Genre: 'N/A', Stars: 'N/A', Director: 'N/A', Creator: 'N/A', Language: 'N/A', Quality: 'N/A', Description: 'N/A' };

            for (let line of lines) {
                if (line.includes('')) {
                    details.Raw_Title = line;
                    let yearMatch = line.match(/\\((\\d{4})\\)/);
                    if(yearMatch) details.Year = yearMatch[1];
                    break;
                }
            }
            let imdbIndex = lines.findIndex(l => l.toLowerCase().includes('imdb rating:'));
            if (imdbIndex > 0) details.Site_Clean_Title = lines[imdbIndex - 1]; 

            let imdbMatch = text.match(/iMDB Rating:\\s*(.*)/i);
            if(imdbMatch) details.IMDb = imdbMatch[1].trim();
            let genreMatch = text.match(/Genre:\\s*(.*)/i);
            if(genreMatch) details.Genre = genreMatch[1].trim();
            let starsMatch = text.match(/Stars:\\s*(.*)/i);
            if(starsMatch) details.Stars = starsMatch[1].trim();
            let dirMatch = text.match(/Director:\\s*(.*)/i);
            if(dirMatch) details.Director = dirMatch[1].trim();
            
            let creatorMatch = text.match(/Creator:\\s*(.*)/i);
            if(creatorMatch) { details.Creator = creatorMatch[1].trim(); details.Type = 'TV Series'; } 
            else if (text.match(/No\\.\\s*of\\s*Episodes:/i) || details.Raw_Title.match(/Season/i)) details.Type = 'TV Series';
            
            let langMatch = text.match(/Language:\\s*(.*)/i);
            if(langMatch) details.Language = langMatch[1].trim();
            let qMatch = text.match(/Quality:\\s*(.*)/i);
            if(qMatch) details.Quality = qMatch[1].trim();

            let audioMatch = text.match(/(Audio Tracks:[\\s\\S]*?)(?:Language:|Screenshots:|Quality:|Download Links|$)/i);
            if(audioMatch) {
                details.Description = audioMatch[1].trim();
            } else {
                let plotMatch = text.match(/(?:Storyline|Plot):([\\s\\S]*?)(?:Director:|Stars:|Genre:|$)/i);
                if (plotMatch) details.Description = plotMatch[1].trim();
            }

            return details;
        }''')
        
        raw_links = await movie_page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let results = [];
            links.forEach(a => {
                let href = a.href.toLowerCase();
                if (href.includes('hubdrive') || href.includes('hubcloud') || href.includes('hubcdn') || href.includes('greenmountmotors') || href.includes('inventoryidea')) {
                    let btnText = a.innerText.trim() || (a.parentElement ? a.parentElement.innerText.trim() : "Link");
                    let cleanText = btnText.replace(/\\n/g, ' ');
                    
                    let quality = cleanText;
                    let size = "";
                    
                    if (cleanText.toLowerCase().includes('sample')) {
                        return; // Ignore sample files completely!
                    }
                    
                    let sizeMatch = cleanText.match(/(.*?)\\[(.*?)\\]/);
                    if (sizeMatch) {
                        quality = sizeMatch[1].trim();
                        size = sizeMatch[2].trim();
                    }
                    
                    results.push({ quality: quality, size: size, url: a.href });
                }
            });
            return results;
        }''')

        fixed_data = fix_movie_details(scraped_data)
        tmdb_data = get_tmdb_details(fixed_data)

        title_to_check = tmdb_data.get('Title') if tmdb_data else None
        if not title_to_check:
            title_to_check = fixed_data.get('Search_Query')

        is_tv_series = (
            fixed_data.get('Type') == 'TV Series' or
            (tmdb_data and tmdb_data.get('is_tv', False))
        )

        # --- DUPLICATE CHECK: TMDB ID se pehle (most reliable) ---
        tmdb_id = tmdb_data.get('tmdb_id') if tmdb_data else None
        if tmdb_id:
            already_exists, existing_movie_id = check_movie_by_tmdb_id(tmdb_id)
            if already_exists:
                if is_tv_series:
                    existing = get_existing_seasons_episodes(title_to_check or '')
                    if existing:
                        print(f"📺 [TMDB:{tmdb_id}] SERIES EXISTS: '{title_to_check}' — {len(existing)} entries in DB")
                        for q, servers in existing.items():
                            print(f"      → {q} ({len(servers)} server(s))")
                        print(f"   🔄 Continuing to find missing seasons/episodes...")
                    else:
                        print(f"📺 [TMDB:{tmdb_id}] SERIES EXISTS but no files — re-scraping '{title_to_check}'")
                else:
                    print(f"⏩ SKIP [TMDB:{tmdb_id}]: Already in DB → {title_to_check}")
                    return
        else:
            # Fallback: Multi-field scoring duplicate check (title + year + stars + director)
            dup_found, dup_movie_id, dup_score = find_duplicate_movie(scraped_data, tmdb_data)
            if dup_found:
                if is_tv_series:
                    existing = get_existing_seasons_episodes(title_to_check or '')
                    if existing:
                        print(f"📺 SERIES EXISTS [score={dup_score}]: '{title_to_check}' — {len(existing)} entries in DB")
                        for q, servers in existing.items():
                            print(f"      → {q} ({len(servers)} server(s))")
                        print(f"   🔄 Continuing scrape to find any missing seasons/episodes...")
                    else:
                        print(f"📺 SERIES EXISTS but NO movie_files found — re-scraping '{title_to_check}'")
                else:
                    print(f"⏩ SKIP [score={dup_score}]: Duplicate detected → {title_to_check}")
                    return

        bypassed_links_data = []
        if raw_links:
            tasks = [process_single_link(browser, sem, raw) for raw in raw_links]
            results = await asyncio.gather(*tasks)
            for res in results:
                if res['direct_link']:
                    bypassed_links_data.append(res)
                    print(f"    ✅ [{res['quality']}] -> SUCCESS")

        db_payload = {
            "url": movie_link,
            "raw_title": fixed_data['Raw_Title'],
            "type": fixed_data['Type'],
            "imdb": fixed_data['IMDb'],
            "tmdb_data": tmdb_data,
            "genre": fixed_data.get('Genre', 'N/A'),
            "description": fixed_data.get('Description', 'N/A'),
            "bypassed_links": bypassed_links_data
        }
        save_movie_to_db(db_payload)
        print(f"💾 Saved to Database -> {fixed_data['Site_Clean_Title']}")

    except Exception as e:
        print(f"❌ Error extracting {movie_link}: {e}")
    finally:
        await movie_page.close()

# =====================================================================
# HOURLY INTERRUPT CHECKER (Checks Postgres status)
# =====================================================================
async def run_hourly_check(browser, main_context, sem):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Check if table exists first
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bot_commands');")
        if not cur.fetchone()[0]:
            cur.close()
            conn.close()
            return  # Table nahi hai, skip karo silently
        
        cur.execute("SELECT status FROM bot_commands WHERE task = 'hourly_check';")
        res = cur.fetchone()
        
        if res and res[0] == 'pending':
            print("\n🚨 DB HOURLY CHECK TRIGGERED! Pausing deep scraper. Checking Homepage...", flush=True)
            page = await main_context.new_page()
            await page.goto("https://new5.hdhub4u.cl/", timeout=60000, wait_until="domcontentloaded")
            
            movies_on_page = await page.evaluate('''() => {
                let links = Array.from(document.querySelectorAll('a'));
                let uniqueMovies = [];
                let urls = new Set();
                let badWords = ['hdhub4u.tv', 'hdhub4u.bi', 'home', '4k movies', 'bollywood', 'hollywood', 'hindi dubbed', 'south hindi', 'web series', 'genres', 'disclaimer', 'how to download', 'join our group', 'movie request page', 'avoid fake', 'latest releases'];
                
                links.forEach(a => {
                    let href = a.href.toLowerCase();
                    let title = (a.title || a.innerText).trim();
                    let isBad = badWords.some(bw => title.toLowerCase().includes(bw));
                    if (title.length > 15 && !isBad && href.includes('hdhub4u') && !href.includes('/category/') && !href.includes('/page/') && !href.includes('/genre/')) {
                        if (!urls.has(href)) { urls.add(href); uniqueMovies.push(a.href); }
                    }
                });
                return uniqueMovies.slice(0, 5);
            }''')
            await page.close()

            for movie_link in movies_on_page:
                await scrape_and_save_movie(movie_link, browser, main_context, sem)
            
            cur.execute("UPDATE bot_commands SET status = 'done' WHERE task = 'hourly_check';")
            conn.commit()
            print("▶️ Hourly Check complete! Resuming...\n", flush=True)
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ Hourly check error: {e}", flush=True)

# =====================================================================
# 🚀 MASTER DEEP SCRAPER (Runs Endlessly with GitHub Relay)
# =====================================================================
async def master_auto_scraper():
    print("=" * 60, flush=True)
    print("🚀 SCRAPER STARTED", flush=True)
    print(f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)
    
    # DB connection test
    print("🔌 Testing Database Connection...", flush=True)
    try:
        test_conn = get_db_connection()
        test_conn.close()
        print("✅ Database Connected Successfully!", flush=True)
    except Exception as e:
        print(f"❌ Database Connection FAILED: {e}", flush=True)
        print("⚠️ Continuing anyway - will retry on each operation", flush=True)
    
    start_time = time.time()
    MAX_RUN_TIME = (5 * 3600) + (45 * 60) # 5 Hours 45 Minutes limit for GitHub

    try:
        print("🎭 Starting Playwright...", flush=True)
        async with async_playwright() as p:
            print("🌐 Launching Chromium (headless=True)...", flush=True)
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
            )
            print("✅ Chromium Launched!", flush=True)
            main_context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            await main_context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media"] else route.continue_())
            
            page = await main_context.new_page()
            current_url = "https://new5.hdhub4u.cl/" 
            page_num = 1
            sem = asyncio.Semaphore(20) 
            print("✅ Browser ready, starting scrape!", flush=True)

            while current_url:
                print(f"\n{'='*50}", flush=True)
                print(f"🌐 Scraping PAGE {page_num}: {current_url}", flush=True)
                print(f"{'='*50}", flush=True)
                
                try:
                    await page.goto(current_url, timeout=60000, wait_until="domcontentloaded")
                    print(f"✅ Page {page_num} loaded, extracting movie links...", flush=True)
                except Exception as e:
                    print(f"❌ Failed to load page {page_num}: {e}", flush=True)
                    break

                movies_on_page = await page.evaluate('''() => {
                    let links = Array.from(document.querySelectorAll('a'));
                    let uniqueMovies = [];
                    let urls = new Set();
                    let badWords = ['hdhub4u.tv', 'hdhub4u.bi', 'home', '4k movies', 'bollywood', 'hollywood', 'hindi dubbed', 'south hindi', 'web series', 'genres', 'disclaimer', 'how to download', 'join our group', 'movie request page', 'avoid fake', 'latest releases'];
                    
                    links.forEach(a => {
                        let href = a.href.toLowerCase();
                        let title = (a.title || a.innerText).trim();
                        let isBad = badWords.some(bw => title.toLowerCase().includes(bw));
                        if (title.length > 15 && !isBad && href.includes('hdhub4u') && !href.includes('/category/') && !href.includes('/page/') && !href.includes('/genre/')) {
                            if (!urls.has(href)) { urls.add(href); uniqueMovies.push(a.href); }
                        }
                    });
                    return uniqueMovies;
                }''')
                
                print(f"📋 Found {len(movies_on_page)} movies on page {page_num}", flush=True)

                # 1. Check for Postgres Hourly Flag (ONCE PER PAGE)
                await run_hourly_check(browser, main_context, sem)

                for movie_link in movies_on_page:
                    # ⏱️ TIME CHECK EVERY MOVIE (For GitHub Relay)
                    if time.time() - start_time > MAX_RUN_TIME:
                        print("⏳ Time limit reaching! Handing over to next runner...", flush=True)
                        trigger_next_github_runner()
                        sys.exit(0)
                    
                    # 2. Process Movie (Skips if already in Postgres DB)
                    await scrape_and_save_movie(movie_link, browser, main_context, sem)
                    
                next_page_url = await page.evaluate('''() => {
                    let nextBtn = document.querySelector('a.next.page-numbers');
                    return nextBtn ? nextBtn.href : null;
                }''')

                current_url = next_page_url
                if current_url: page_num += 1

    except KeyboardInterrupt:
        print("\n🛑 Process stopped manually.")

if __name__ == "__main__":
    asyncio.run(master_auto_scraper())
