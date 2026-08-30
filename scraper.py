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
DATABASE_URL = "postgresql://postgres.vzixjxeppvpxrhntaidb:l0aDck2NUeD4Jws5@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres"
TMDB_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"

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

# =====================================================================
# 1. FIX MOVIE DETAILS (Extract Title, Year, Type from Bracket)
# =====================================================================
def fix_movie_details(scraped_data, movie_url=None):
    raw_title = scraped_data.get('Raw_Title', '').replace('🎬', '').strip()
    search_query = 'UNKNOWN_TITLE'
    year = 'N/A'
    media_type = 'Movie'
    default_season = 1  

    if raw_title and raw_title != 'N/A':
        title_parts = re.split(r'\(|\[', raw_title)
        search_query = title_parts[0].strip()

        brackets_content = re.findall(r'\((.*?)\)|\[(.*?)\]', raw_title)
        bracket_texts = [item for sublist in brackets_content for item in sublist if item]
        
        for text in bracket_texts:
            text_lower = text.lower().strip()
            if re.match(r'^\d{4}$', text_lower):
                year = text_lower
            elif "season" in text_lower or re.match(r'^s\d+', text_lower):
                media_type = 'TV Series'

        s_match = re.search(r'(?i)\bseason\s*(\d+)', raw_title)
        if s_match:
            media_type = 'TV Series'
            default_season = int(s_match.group(1))
            search_query = re.sub(r'(?i)\bseason\s*\d+.*', '', search_query).strip()
        elif re.search(r'(?i)\bepisode\b', raw_title):
            media_type = 'TV Series'

    if not search_query or search_query == 'UNKNOWN_TITLE':
        if movie_url:
            try:
                slug = movie_url.rstrip('/').split('/')[-1]
                if 'season' in slug.lower() or 'episode' in slug.lower():
                    media_type = 'TV Series'
                
                s_match_url = re.search(r'(?i)season-(\d+)', slug)
                if s_match_url:
                    default_season = int(s_match_url.group(1))

                junk_words = ['hindi', 'english', 'dual', 'audio', 'dubbed', 'uncut', 'hdrip', 'webrip', 
                              'bluray', 'web', 'dl', 'esubs', 'esub', '480p', '720p', '1080p', '4k',
                              'x264', 'x265', 'hevc', 'aac', 'mb', 'gb', 'full', 'movie', 'hd',
                              'pre', 'dvdrip', 'brrip', 'hdtc', 'camrip', 'south', 'bollywood',
                              'hollywood', 'series', 'season', 'complete', 'all', 'episodes']
                parts = slug.split('-')
                clean_parts = []
                for p in parts:
                    if re.match(r'^\d{4}$', p):
                        year = p
                        break
                    if re.match(r'^\d+[mg]b?$', p, re.IGNORECASE):
                        break
                    if p.lower() not in junk_words and len(p) > 1:
                        clean_parts.append(p)
                search_query = ' '.join(clean_parts).strip()
            except:
                pass

    if not search_query:
        search_query = 'UNKNOWN_TITLE'

    scraped_data['Search_Query'] = search_query
    scraped_data['Year'] = year
    scraped_data['Type'] = media_type
    scraped_data['Default_Season'] = default_season  
    
    print(f"   ✅ Cleaned Title: '{search_query}' | Season: {default_season} | Type: '{media_type}'", flush=True)
    return scraped_data

# =====================================================================
# 2. TMDB DETAILS (Strict API Call: TV for Series, Movie for Movie)
# =====================================================================
def get_tmdb_details(fixed_data):
    search_query = fixed_data['Search_Query']
    year_hint = fixed_data['Year']
    type_hint = 'tv' if fixed_data['Type'] == 'TV Series' else 'movie'

    print(f"   🌐 Fetching LIVE data from TMDB for: {search_query} (Type: {type_hint})...", flush=True)
    
    if type_hint == 'tv':
        url = f"https://api.themoviedb.org/3/search/tv?api_key={TMDB_KEY}&query={urllib.parse.quote(search_query)}"
    else:
        url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={urllib.parse.quote(search_query)}"
        
    try:
        response = requests.get(url, timeout=10).json()
        results = response.get('results', [])
        
        if not results:
            return None

        best_match = results[0]
        tmdb_id = best_match.get('id')
        
        details_url = f"https://api.themoviedb.org/3/{type_hint}/{tmdb_id}?api_key={TMDB_KEY}"
        details = requests.get(details_url, timeout=10).json()
        
        genres = [g['name'] for g in details.get('genres', [])]
        genre_str = ", ".join(genres) if genres else "N/A"
        plot = details.get('overview', 'N/A')
        rating = str(round(details.get('vote_average', 0), 1)) if details.get('vote_average') else 'N/A'
        
        credits_url = f"https://api.themoviedb.org/3/{type_hint}/{tmdb_id}/credits?api_key={TMDB_KEY}"
        credits = requests.get(credits_url, timeout=10).json()
        cast_list = [c['name'] for c in credits.get('cast', [])[:5]]
        cast_str = ", ".join(cast_list) if cast_list else "N/A"
        
        ext_url = f"https://api.themoviedb.org/3/{type_hint}/{tmdb_id}/external_ids?api_key={TMDB_KEY}"
        ext_ids = requests.get(ext_url, timeout=10).json()
        imdb_id = ext_ids.get('imdb_id', 'N/A')
        
        seasons_data = {}
        if type_hint == 'tv':
            for s in details.get('seasons', []):
                s_num = str(s.get('season_number', ''))
                if s_num and s_num != "0":
                    s_air_date = str(s.get('air_date', ''))
                    s_year = int(s_air_date[:4]) if len(s_air_date) >= 4 and s_air_date[:4].isdigit() else 0
                    s_poster = f"https://image.tmdb.org/t/p/original{s.get('poster_path')}" if s.get('poster_path') else None
                    
                    episodes_info = {}
                    try:
                        season_url = f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{s_num}?api_key={TMDB_KEY}"
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
                    
        return {
            "Title": best_match.get('name') or best_match.get('title'),
            "Release": best_match.get('first_air_date') or best_match.get('release_date', 'N/A'),
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "Genre": genre_str,
            "Description": plot,
            "TMDb_Rating": rating,
            "Cast": cast_str,
            "seasons_data": seasons_data,
            "Poster": f"https://image.tmdb.org/t/p/original{best_match.get('poster_path')}" if best_match.get('poster_path') else 'N/A',
            "is_tv": type_hint == 'tv'
        }
    except Exception as e:
        print(f"TMDB Deep Fetch Error: {e}")
        return None

# =====================================================================
# 3. SAVE TO DB (PAGE DATA PRIORITY + URL DECODING FOR FILES)
# =====================================================================
def save_movie_to_db(data_dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        tmdb = data_dict.get('tmdb_data') or {}
        title  = tmdb.get('Title') or data_dict.get('clean_title')
        
        if title:
            junk_pattern = r'(?i)\b(uncut|hindi|dual\s*audio|dubbed|480p|720p|1080p|hdrip|webrip|web-dl|x264|hevc|esubs?|mb|gb|brrip|dvdrip|hdtc|camrip|x265|aac)\b'
            title = re.sub(junk_pattern, '', title)
            title = re.sub(r'\b(19|20)\d{2}\b', '', title)
            title = re.sub(r'[\(\)\[\]\-]+', ' ', title).strip()

        year   = tmdb.get('Release', '')[:4] if tmdb.get('Release') else 'N/A'
        poster = tmdb.get('Poster') or ''
        tmdb_id = str(tmdb.get('tmdb_id', '')) if tmdb.get('tmdb_id') else None
        
        page_genre = data_dict.get('Genre', 'N/A')
        page_rating = data_dict.get('imdb', 'N/A')
        page_cast = data_dict.get('Stars', 'N/A')
        page_lang = data_dict.get('Language', 'N/A')
        page_desc = data_dict.get('description', 'N/A')

        genre_str  = page_genre if page_genre != 'N/A' else tmdb.get('Genre', 'N/A')
        rating_str = page_rating if page_rating != 'N/A' else tmdb.get('TMDb_Rating', 'N/A')
        cast_str   = page_cast if page_cast != 'N/A' else tmdb.get('Cast', 'N/A')
        plot_str   = page_desc if page_desc != 'N/A' else tmdb.get('Description', 'N/A')
        lang_str   = page_lang if page_lang != 'N/A' else 'Hindi'
        
        imdb_id_real = tmdb.get('imdb_id')
        seasons_json = tmdb.get('seasons_data', {})
        
        final_category = "Web Series" if data_dict.get('Type') == 'TV Series' or tmdb.get('is_tv') else "Movies"
        
        try: year_val = int(year)
        except: year_val = None
        
        if tmdb_id:
            cur.execute("SELECT id FROM movies WHERE imdb_id = %s LIMIT 1", (tmdb_id,))
        else:
            cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
        row = cur.fetchone()

        import json
        if row:
            movie_id = row[0]
            cur.execute("""
                UPDATE movies SET 
                    url = %s, poster_url = %s, year = %s, genre = %s, 
                    description = %s, rating = %s, language = %s, "cast" = %s,
                    imdb_id = %s, seasons_data = %s, category = %s
                WHERE id = %s
            """, (
                data_dict['url'], poster, year_val, genre_str,
                plot_str, rating_str, lang_str, cast_str,
                imdb_id_real, json.dumps(seasons_json), final_category, movie_id
            ))
        else:
            cur.execute("""
                INSERT INTO movies (url, title, poster_url, year, genre, description, rating, language, "cast", imdb_id, seasons_data, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                data_dict['url'], title, poster, year_val,
                genre_str, plot_str, rating_str, lang_str,
                cast_str, imdb_id_real, json.dumps(seasons_json), final_category
            ))
            movie_id = cur.fetchone()[0]
        
        if movie_id:
            bypassed_links = data_dict.get('bypassed_links', [])
            default_season = data_dict.get('Default_Season', 1)

            for link in bypassed_links:
                raw_q  = link.get('quality', 'Unknown')
                f_size = link.get('size', '')
                direct_links_data = link.get('direct_link')

                def upsert_file(raw_quality, server_name_raw, srv_url, file_size):
                    import urllib.parse
                    import re
                    
                    decoded_url = urllib.parse.unquote(srv_url) if srv_url else ""
                    filename_match = re.search(r'filename=["\']?(.*?)["\'&]', decoded_url, re.IGNORECASE)
                    actual_filename = filename_match.group(1) if filename_match else decoded_url
                    
                    combined_text = f"{actual_filename} {raw_quality}"
                    
                    is_combined = "[COMBINED]" in raw_quality
                    js_ep_match = re.search(r'\[(E\d{1,3})\]', raw_quality)
                    js_ep_str = js_ep_match.group(1) if js_ep_match else ""

                    ep_str = ""
                    s_e_match = re.search(r'(?i)\bS(\d{1,2})[\s._-]*E(\d{1,3})\b', actual_filename)
                    
                    if s_e_match:
                        ep_str = f"S{int(s_e_match.group(1)):02d}E{int(s_e_match.group(2)):02d}"
                    else:
                        if js_ep_str:
                            ep_str = f"S{default_season:02d}{js_ep_str}"
                        elif is_combined or re.search(r'(?i)\b(batch|full season|complete|all episodes|pack|zip)\b', combined_text):
                            ep_str = f"S{default_season:02d} Combined"

                    if final_category == "Movies":
                        ep_str = ""

                    quality = "HD"
                    q_match = re.search(r'\b(2160p|1080p|720p|480p|360p|4K)\b', combined_text, re.IGNORECASE)
                    if q_match:
                        quality = q_match.group(1).lower()
                        
                    src_match = re.search(r'\b(WEB-DL|WEBRip|BluRay|HDRip|HDTC|HDTS|CAMRip)\b', combined_text, re.IGNORECASE)
                    if src_match:
                        quality += f" {src_match.group(1).upper()}"
                        
                    if quality == "HD" and re.search(r'\b(2160p|1080p|720p|480p|360p|4K)\b', raw_quality, re.IGNORECASE):
                        quality = raw_quality

                    langs = []
                    lang_keywords = ['Hindi', 'English', 'Tamil', 'Telugu', 'Malayalam', 'Dual Audio', 'Multi']
                    for l in lang_keywords:
                        if re.search(r'\b' + l + r'\b', combined_text, re.IGNORECASE):
                            langs.append(l.title())
                    languages = ", ".join(sorted(list(set(langs)))) if langs else "Hindi"

                    if not file_size or file_size.lower() in ('', 'n/a', 'unknown'):
                        size_match = re.search(r'(?i)(\d+(?:\.\d+)?\s*(?:gb|mb))', combined_text)
                        file_size = size_match.group(1).strip().upper().replace(' ','') if size_match else ""

                    m_srv = re.search(r'(?i)download\s*\[(.+?)\]', server_name_raw or '')
                    srv_name = m_srv.group(1).strip() if m_srv else (server_name_raw or '').strip()

                    cur.execute(
                        "SELECT id FROM movie_files WHERE movie_id = %s AND quality = %s AND server_name = %s AND extra_info = %s",
                        (movie_id, quality, srv_name, ep_str)
                    )
                    if cur.fetchone():
                        cur.execute("""
                            UPDATE movie_files
                            SET url = %s, file_size = %s, languages = %s, source = 'scraped'
                            WHERE movie_id = %s AND quality = %s AND server_name = %s AND extra_info = %s
                        """, (srv_url, file_size, languages, movie_id, quality, srv_name, ep_str))
                    else:
                        cur.execute("""
                            INSERT INTO movie_files (movie_id, quality, server_name, url, file_size, languages, extra_info, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'scraped')
                        """, (movie_id, quality, srv_name, srv_url, file_size, languages, ep_str))

                if isinstance(direct_links_data, list):
                    for srv in direct_links_data:
                        srv_url = srv.get('url', '').strip()
                        if srv_url:
                            upsert_file(raw_q, srv.get('server_name', ''), srv_url, f_size)
                elif isinstance(direct_links_data, dict):
                    srv_url = direct_links_data.get('url', '').strip()
                    if srv_url:
                        upsert_file(raw_q, '', srv_url, f_size)
                elif isinstance(direct_links_data, str) and direct_links_data.strip():
                    upsert_file(raw_q, '', direct_links_data.strip(), f_size)
        
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Save Error: {e}")

# =====================================================================
# BYPASS MODULES (With DEAD LINK DETECTOR)
# =====================================================================
async def bypass_hubcdn_mediator(context, target_url):
    page = await context.new_page()
    try:
        await page.goto(target_url, timeout=60000)
        await page.wait_for_timeout(10000) 
        
        is_dead = await page.evaluate('''() => {
            let text = document.body.innerText.toLowerCase();
            return text.includes('file not found') || text.includes('file was deleted') || text.includes('no longer available') || text.includes('404 not found');
        }''')
        if is_dead:
            print(f"   ⚠️ DEAD LINK SKIPPED: {target_url}", flush=True)
            return None

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

        is_dead = await page.evaluate('''() => {
            let text = document.body.innerText.toLowerCase();
            return text.includes('file not found') || text.includes('file was deleted') || text.includes('no longer available') || text.includes('404 not found') || text.includes('file has been deleted');
        }''')
        if is_dead:
            print(f"   ⚠️ DEAD LINK SKIPPED: {hubdrive_url}", flush=True)
            return None

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

        is_dead_final = await page.evaluate('''() => {
            let text = document.body.innerText.toLowerCase();
            return text.includes('file not found') || text.includes('file was deleted') || text.includes('no longer available');
        }''')
        if is_dead_final:
            print(f"   ⚠️ DEAD LINK SKIPPED: {hubcloud_url}", flush=True)
            return None

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

            let h1 = document.querySelector('h1.entry-title, h1.post-title, h1');
            if (h1) {
                details.Raw_Title = h1.innerText.trim();
            }
            
            if (details.Raw_Title === 'N/A' || details.Raw_Title.length < 5) {
                let imdbIdx = lines.findIndex(l => l.toLowerCase().includes('imdb rating:'));
                if (imdbIdx > 0) {
                    details.Raw_Title = lines[imdbIdx - 1];
                }
            }
            
            if (details.Raw_Title === 'N/A' || details.Raw_Title.length < 5) {
                for (let line of lines) {
                    if (line.match(/\\(\\d{4}\\)/) && line.length > 10 && line.length < 200) {
                        details.Raw_Title = line;
                        break;
                    }
                }
            }
            
            details.Raw_Title = details.Raw_Title.replace(/[\\uE000-\\uF8FF]/g, '').trim();
            
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
            
            let plotMatch = text.match(/(?:Storyline|Plot):([\\s\\S]*?)(?:Director:|Stars:|Genre:|$)/i);
            if (plotMatch) details.Description = plotMatch[1].trim();

            return details;
        }''')
        
        raw_links = await movie_page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let results = [];
            let validKeywords = ['hubdrive', 'hubcloud', 'hubcdn', 'greenmountmotors', 'inventoryidea', 'indishare', 'sendit', 'clicknupload', 'upload.mn', 'openload', 'bdupload', '9xupload', 'uploadbaz', 'upfile', 'more download links', '9xplay'];

            links.forEach(a => {
                let href = a.href.toLowerCase();
                let btnText = a.innerText.trim();
                let parentText = a.parentElement ? a.parentElement.innerText.trim().replace(/\\n/g, ' ') : "";
                
                let isTarget = validKeywords.some(kw => href.includes(kw) || btnText.toLowerCase().includes(kw));
                
                if (isTarget) {
                    if (btnText.toLowerCase().includes('sample')) return;

                    // CHECK & SKIP: Watch Online links
                    let isWatch = btnText.toLowerCase().includes('watch') || btnText.toLowerCase().includes('play') || parentText.toLowerCase().includes('watch');
                    if (isWatch) return;
                    
                    // DOM SCANNER: Episode Number or Full Season?
                    let epContext = "";
                    let isCombined = false;
                    let node = a;
                    
                    for (let i = 0; i < 4 && node; i++) {
                        let prev = node.previousElementSibling;
                        for (let j = 0; j < 10 && prev; j++) {
                            let pText = (prev.innerText || "").trim().toLowerCase();
                            
                            let matchEp = pText.match(/(?:episode|ep)\\s*[\\-:]?\\s*(\\d{1,3})/i);
                            if (matchEp) {
                                epContext = `E${matchEp[1].padStart(2, '0')}`;
                                break;
                            }
                            if (pText.includes('download links') || pText.includes('full series') || pText.includes('complete season') || pText.includes('zip') || pText.includes('batch') || pText.includes('pack')) {
                                isCombined = true;
                                break;
                            }
                            prev = prev.previousElementSibling;
                        }
                        if (epContext || isCombined) break;
                        node = node.parentElement;
                    }

                    let combinedText = parentText.length < 100 ? parentText : btnText;
                    
                    let prefix = "";
                    if (epContext) prefix += `[${epContext}] `;
                    if (isCombined && !epContext) prefix += "[COMBINED] ";

                    let finalQuality = prefix + combinedText;

                    if (!results.find(r => r.url === a.href)) {
                        results.push({ quality: finalQuality, size: "", url: a.href });
                    }
                }
            });
            return results;
        }''')

        fixed_data = fix_movie_details(scraped_data, movie_url=movie_link)
        tmdb_data = get_tmdb_details(fixed_data)

        title_to_check = tmdb_data.get('Title') if tmdb_data else None
        if not title_to_check:
            title_to_check = fixed_data.get('Search_Query')

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
            "clean_title": fixed_data['Search_Query'],
            "Type": fixed_data['Type'],
            "Default_Season": fixed_data['Default_Season'],
            "imdb": fixed_data['IMDb'], 
            "tmdb_data": tmdb_data,
            "Genre": fixed_data.get('Genre', 'N/A'),
            "Stars": fixed_data.get('Stars', 'N/A'),
            "Language": fixed_data.get('Language', 'N/A'),
            "description": fixed_data.get('Description', 'N/A'),
            "bypassed_links": bypassed_links_data
        }
        
        save_movie_to_db(db_payload)
        print(f"💾 Saved to Database -> {title_to_check}")

    except Exception as e:
        print(f"❌ Error extracting {movie_link}: {e}")
    finally:
        await movie_page.close()

# =====================================================================
# GITHUB RUNNER RELAY & HOURLY CHECK LOGIC
# =====================================================================
def trigger_next_github_runner():
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

async def run_hourly_check(browser, main_context, sem):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bot_commands');")
        if not cur.fetchone()[0]:
            cur.close()
            conn.close()
            return 
        
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
async def master_auto_scraper(start_page, end_page):
    print("=" * 60, flush=True)
    print("🚀 MASTER SCRAPER STARTED", flush=True)
    print(f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print("=" * 60, flush=True)
    
    print("🔌 Testing Database Connection...", flush=True)
    try:
        test_conn = get_db_connection()
        test_conn.close()
        print("✅ Database Connected Successfully!", flush=True)
    except Exception as e:
        print(f"❌ Database Connection FAILED: {e}", flush=True)
    
    start_time = time.time()
    MAX_RUN_TIME = (5 * 3600) + (45 * 60)

    try:
        print("🎭 Starting Playwright...", flush=True)
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
            )
            main_context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            await main_context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media"] else route.continue_())
            
            page = await main_context.new_page()
            sem = asyncio.Semaphore(10) 

            page_num = start_page
            while page_num <= end_page:
                current_url = f"https://new5.hdhub4u.cl/page/{page_num}/" if page_num > 1 else "https://new5.hdhub4u.cl/"
                print(f"\n{'='*50}", flush=True)
                print(f"🌐 Scraping PAGE {page_num} of {end_page}: {current_url}", flush=True)
                print(f"{'='*50}", flush=True)
                
                try:
                    await page.goto(current_url, timeout=60000, wait_until="domcontentloaded")
                except Exception as e:
                    print(f"❌ Failed to load page {page_num}: {e}", flush=True)
                    page_num += 1
                    continue

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
                
                if len(movies_on_page) == 0:
                    page_num += 1
                    continue

                await run_hourly_check(browser, main_context, sem)

                for movie_link in movies_on_page:
                    if time.time() - start_time > MAX_RUN_TIME:
                        print("⏳ Time limit reaching! Handing over to next runner...", flush=True)
                        trigger_next_github_runner()
                        sys.exit(0)
                        
                    await scrape_and_save_movie(movie_link, browser, main_context, sem)
                    
                page_num += 1

    except KeyboardInterrupt:
        print("\n🛑 Process stopped manually.")
    finally:
        pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Master Scraper Bot")
    parser.add_argument("--start_page", type=int, required=True, help="Start page number")
    parser.add_argument("--end_page", type=int, required=True, help="End page number")
    args = parser.parse_args()

    asyncio.run(master_auto_scraper(args.start_page, args.end_page))
