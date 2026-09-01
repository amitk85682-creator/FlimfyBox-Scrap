import asyncio
import os
import re
import urllib.parse
import nest_asyncio
import requests
import json
import time
import argparse
import psycopg2
import xml.etree.ElementTree as ET
from playwright.async_api import async_playwright

nest_asyncio.apply()

# =====================================================================
# DIRECT POSTGRESQL DATABASE CONFIGURATION
# =====================================================================
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres.vzixjxeppvpxrhntaidb:l0aDck2NUeD4Jws5@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres")
TMDB_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
TARGET_WEBSITE = "https://www.filmyzilla63.com"

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
# 1. FIX MOVIE DETAILS (TMDB Search Cleaner)
# =====================================================================
def fix_movie_details(scraped_data, movie_url=None):
    raw_title = scraped_data.get('Raw_Title', '').replace('🎬', '').strip()
    search_query = 'UNKNOWN_TITLE'
    year = 'N/A'
    media_type = 'Movies'
    season_number = None

    if raw_title and raw_title != 'N/A':
        junk_pattern = r'(?i)\b(hindi|dubbed|movie|south|full|download|web-dl|webrip|bluray|hdrip|hevc|x264|x265|esub|esubs|480p|720p|1080p|4k|mkv|mp4)\b'
        clean = re.sub(r'[\(\)\[\]\-]+', ' ', raw_title)
        
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', clean)
        if year_match:
            year = year_match.group(1)
            clean = clean.replace(year, ' ')
            
        clean = re.sub(junk_pattern, ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        search_query = clean if clean else "UNKNOWN_TITLE"

    if media_type == 'Web Series' and season_number is None:
        season_number = 1

    scraped_data['Search_Query'] = search_query
    scraped_data['Year'] = year
    scraped_data['Type'] = media_type
    scraped_data['Default_Season'] = season_number  
    
    print(f"   ✅ Cleaned Title: '{search_query}' (Year: {year}) | Type: '{media_type}'", flush=True)
    return scraped_data

# =====================================================================
# 2. TMDB DETAILS FETCHER
# =====================================================================
def get_tmdb_details(fixed_data):
    search_query = fixed_data['Search_Query']
    year_hint = fixed_data['Year']
    type_hint = 'tv' if fixed_data['Type'] == 'Web Series' else 'movie'

    url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={urllib.parse.quote(search_query)}"
    if year_hint and year_hint != 'N/A':
        url += f"&year={year_hint}"
        
    try:
        response = requests.get(url, timeout=10).json()
        results = response.get('results', [])
        
        if not results:
            fallback_url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={urllib.parse.quote(search_query)}"
            results = requests.get(fallback_url, timeout=10).json().get('results', [])
            if not results:
                return None

        best_match = results[0]
        tmdb_id = best_match.get('id')
        
        details_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_KEY}"
        details = requests.get(details_url, timeout=10).json()
        
        genres = [g['name'] for g in details.get('genres', [])]
        genre_str = ", ".join(genres) if genres else "N/A"
        plot = details.get('overview', 'N/A')
        rating = str(round(details.get('vote_average', 0), 1)) if details.get('vote_average') else 'N/A'
        
        credits_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits?api_key={TMDB_KEY}"
        credits = requests.get(credits_url, timeout=10).json()
        cast_list = [c['name'] for c in credits.get('cast', [])[:5]]
        cast_str = ", ".join(cast_list) if cast_list else "N/A"
        
        ext_url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_KEY}"
        ext_ids = requests.get(ext_url, timeout=10).json()
        imdb_id = ext_ids.get('imdb_id', 'N/A')
        
        return {
            "Title": best_match.get('name') or best_match.get('title'),
            "Release": best_match.get('first_air_date') or best_match.get('release_date', 'N/A'),
            "tmdb_id": tmdb_id,
            "imdb_id": imdb_id,
            "Genre": genre_str,
            "Description": plot,
            "TMDb_Rating": rating,
            "Cast": cast_str,
            "seasons_data": {},
            "Poster": f"https://image.tmdb.org/t/p/original{best_match.get('poster_path')}" if best_match.get('poster_path') else 'N/A',
            "is_tv": False
        }
    except Exception as e:
        print(f"TMDB Fetch Error: {e}")
        return None

# =====================================================================
# 3. SAVE TO DB
# =====================================================================
def save_movie_to_db(data_dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        tmdb = data_dict.get('tmdb_data') or {}
        title = tmdb.get('Title') or data_dict.get('clean_title')

        year = tmdb.get('Release', '')[:4] if tmdb.get('Release') else data_dict.get('Year')
        poster = tmdb.get('Poster') if (tmdb.get('Poster') and tmdb.get('Poster') != 'N/A') else data_dict.get('page_poster', '')
        
        page_genre = data_dict.get('Genre', 'N/A')
        page_rating = data_dict.get('imdb', 'N/A')
        page_cast = data_dict.get('Stars', 'N/A')
        page_lang = data_dict.get('Language', 'N/A')
        page_desc = data_dict.get('description', 'N/A')

        genre_str = page_genre if page_genre != 'N/A' else tmdb.get('Genre', 'N/A')
        rating_str = page_rating if page_rating != 'N/A' else tmdb.get('TMDb_Rating', 'N/A')
        cast_str = page_cast if page_cast != 'N/A' else tmdb.get('Cast', 'N/A')
        plot_str = page_desc if page_desc != 'N/A' else tmdb.get('Description', 'N/A')
        lang_str = page_lang if page_lang != 'N/A' else 'Hindi'
        
        imdb_id_real = tmdb.get('imdb_id')
        final_category = "Movies"
        
        try: year_val = int(year)
        except: year_val = None

        cur.execute("SELECT id FROM movies WHERE title = %s LIMIT 1", (title,))
        row = cur.fetchone()

        if row:
            movie_id = row[0]
            cur.execute("""
                UPDATE movies SET 
                    url = %s, poster_url = COALESCE(NULLIF(poster_url, ''), %s)
                WHERE id = %s
            """, (data_dict['url'], poster, movie_id))
        else:
            cur.execute("""
                INSERT INTO movies (url, title, poster_url, year, genre, description, rating, language, "cast", imdb_id, seasons_data, category)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                data_dict['url'], title, poster, year_val,
                genre_str, plot_str, rating_str, lang_str,
                cast_str, imdb_id_real, json.dumps({}), final_category
            ))
            movie_id = cur.fetchone()[0]
        
        if movie_id:
            bypassed_links = data_dict.get('bypassed_links', [])

            for link in bypassed_links:
                raw_q = link.get('quality', 'Unknown')
                f_size = link.get('size', '')
                direct_links_data = link.get('direct_links', [])

                quality = "HD"
                q_match = re.search(r'\b(2160p|1080p|720p|480p|360p|4K)\b', raw_q, re.IGNORECASE)
                if q_match:
                    quality = q_match.group(1).lower()
                if "hevc" in raw_q.lower():
                    quality += " HEVC"
                elif "web-dl" in raw_q.lower() or "webdl" in raw_q.lower():
                    quality += " WEB-DL"

                for srv in direct_links_data:
                    srv_name = srv.get('server_name', 'Download Server')
                    srv_url = srv.get('url', '').strip()
                    
                    if not srv_url: continue

                    cur.execute(
                        "SELECT id FROM movie_files WHERE movie_id = %s AND quality = %s AND server_name = %s",
                        (movie_id, quality, srv_name)
                    )
                    
                    if cur.fetchone():
                        cur.execute("""
                            UPDATE movie_files
                            SET url = %s, file_size = %s, languages = %s, source = 'scraped'
                            WHERE movie_id = %s AND quality = %s AND server_name = %s
                        """, (srv_url, f_size, lang_str, movie_id, quality, srv_name))
                    else:
                        cur.execute("""
                            INSERT INTO movie_files (movie_id, quality, server_name, url, file_size, languages, extra_info, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, 'scraped')
                        """, (movie_id, quality, srv_name, srv_url, f_size, lang_str, ''))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Save Error: {e}")

# =====================================================================
# 4. SITEMAP FETCHER & FILMYZILLA SERVER SCRAPER
# =====================================================================
def get_all_movie_links_from_sitemap():
    """Fetches all movie URLs directly from the site sitemap with proper headers."""
    print("📥 Fetching Filmyzilla sitemap...", flush=True)
    movie_links = set()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    try:
        sitemap_url = f"{TARGET_WEBSITE}/sitemap.xml"
        resp = requests.get(sitemap_url, headers=headers, timeout=20)
        
        if resp.status_code != 200 or '<html' in resp.text.lower():
            sitemap_url = f"{TARGET_WEBSITE}/post-sitemap.xml"
            resp = requests.get(sitemap_url, headers=headers, timeout=20)
            
        if resp.status_code == 200 and '<html' not in resp.text.lower():
            root = ET.fromstring(resp.content)
            namespaces = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            sitemaps = root.findall('ns:sitemap', namespaces)
            if sitemaps:
                for sm in sitemaps:
                    loc = sm.find('ns:loc', namespaces).text
                    if loc and ('post' in loc or 'movie' in loc or 'sitemap' in loc):
                        try:
                            sub_resp = requests.get(loc, headers=headers, timeout=15)
                            if sub_resp.status_code == 200 and '<html' not in sub_resp.text.lower():
                                sub_root = ET.fromstring(sub_resp.content)
                                for url_elem in sub_root.findall('ns:url', namespaces):
                                    url_loc = url_elem.find('ns:loc', namespaces).text
                                    if url_loc and '/movie/' in url_loc:
                                        movie_links.add(url_loc)
                        except:
                            pass
            else:
                for url_elem in root.findall('ns:url', namespaces):
                    url_loc = url_elem.find('ns:loc', namespaces).text
                    if url_loc and '/movie/' in url_loc:
                        movie_links.add(url_loc)
        print(f"✅ Loaded {len(movie_links)} total links from Sitemap!", flush=True)
    except Exception as e:
        print(f"❌ Sitemap fetch error: {e}", flush=True)
    
    return list(movie_links)

async def extract_filmyzilla_servers(context, sem, item):
    async with sem:
        server_page_url = item['url']
        quality_name = item['quality']
        size = item['size']
        page = await context.new_page()
        servers = []
        
        try:
            await page.goto(server_page_url, timeout=45000, wait_until="domcontentloaded")
            
            servers = await page.evaluate(r'''() => {
                let links = Array.from(document.querySelectorAll('a'));
                let serverLinks = [];
                links.forEach(a => {
                    let text = a.innerText.trim();
                    let href = a.href;
                    if (text.toLowerCase().includes('server') || href.includes('/verified/')) {
                        let cleanName = text.replace(/start download/i, '').replace(/now/i, '').replace(/[-:]/g, '').trim();
                        if (!cleanName) cleanName = 'Server';
                        serverLinks.push({
                            server_name: cleanName,
                            url: href
                        });
                    }
                });
                return serverLinks;
            }''')
        except Exception as e:
            print(f"   ⚠️ Error scraping server page {server_page_url}: {e}")
        finally:
            await page.close()

        return {
            "quality": quality_name,
            "size": size,
            "direct_links": servers
        }

# =====================================================================
# 5. PROCESS A SINGLE MOVIE PAGE
# =====================================================================
async def scrape_and_save_movie(movie_link, main_context, sem):
    if check_movie_in_db(movie_link):
        print(f"⏩ SKIP: Already in Database -> {movie_link}", flush=True)
        return

    print(f"\n🎬 EXTRACTING: {movie_link}", flush=True)
    movie_page = await main_context.new_page()
    
    try:
        await movie_page.goto(movie_link, timeout=60000, wait_until="domcontentloaded")
        
        movie_data = await movie_page.evaluate(r'''() => {
            let details = {
                Raw_Title: '', Stars: 'N/A', Genre: 'N/A', Language: 'Hindi',
                Description: 'N/A', Poster: '', QualityLinks: []
            };

            let fullText = document.body.innerText;
            let titleMatch = fullText.match(/Movie Name\s*:\s*([^\n]+)/i);
            if (titleMatch) {
                details.Raw_Title = titleMatch[1].trim();
            } else {
                let titleTag = document.querySelector('title');
                if(titleTag) details.Raw_Title = titleTag.innerText.split('|')[0].replace(/FilmyZilla/ig, '').trim();
            }

            let imgs = Array.from(document.querySelectorAll('img'));
            let validImg = imgs.find(img => !img.src.toLowerCase().includes('logo') && !img.src.toLowerCase().includes('icon'));
            if (validImg) details.Poster = validImg.src;

            let starMatch = fullText.match(/Starcast\s*:\s*([^\n]+)/i);
            if (starMatch) details.Stars = starMatch[1].trim();

            let genreMatch = fullText.match(/Genres?\s*:\s*([^\n]+)/i);
            if (genreMatch) details.Genre = genreMatch[1].trim();

            let langMatch = fullText.match(/Language\s*:\s*([^\n]+)/i);
            if (langMatch) details.Language = langMatch[1].trim();

            let storyMatch = fullText.match(/(?:Storyline|Story|Movie Story)\s*:\s*([^\n]+)/i);
            if (storyMatch) details.Description = storyMatch[1].trim();

            let allLinks = Array.from(document.querySelectorAll('a'));
            allLinks.forEach(a => {
                let href = a.href;
                let text = a.innerText.trim();
                
                if (href.includes('/server/') || href.includes('.html')) {
                    if (text.toLowerCase().includes('.mkv') || text.toLowerCase().includes('.mp4') || /(480p|720p|1080p|2160p)/i.test(text)) {
                        
                        let parentText = a.parentElement ? a.parentElement.innerText : text;
                        let sizeMatch = parentText.match(/(\d+(?:\.\d+)?\s*(?:MB|GB))/i);
                        let size = sizeMatch ? sizeMatch[1].trim() : '';
                        
                        details.QualityLinks.push({
                            quality: text,
                            size: size,
                            url: href
                        });
                    }
                }
            });

            return details;
        }''')
        
        await movie_page.close()

        if not movie_data['QualityLinks']:
            print(f"   ⚠️ SKIP: No download format links found on {movie_link}", flush=True)
            return

        server_tasks = [extract_filmyzilla_servers(main_context, sem, q_link) for q_link in movie_data['QualityLinks']]
        bypassed_links_data = await asyncio.gather(*server_tasks)
        bypassed_links_data = [b for b in bypassed_links_data if b['direct_links']]

        if not bypassed_links_data:
            print(f"   ⚠️ SKIP: Failed to resolve server download links. Ignoring.", flush=True)
            return

        fixed_data = fix_movie_details(movie_data, movie_url=movie_link)
        tmdb_data = get_tmdb_details(fixed_data)

        db_payload = {
            "url": movie_link,
            "raw_title": fixed_data['Raw_Title'],
            "clean_title": fixed_data['Search_Query'],
            "Year": fixed_data['Year'],
            "page_poster": movie_data['Poster'],
            "tmdb_data": tmdb_data,
            "Genre": movie_data.get('Genre', 'N/A'),
            "Stars": movie_data.get('Stars', 'N/A'),
            "Language": movie_data.get('Language', 'Hindi'),
            "description": movie_data.get('Description', 'N/A'),
            "bypassed_links": bypassed_links_data
        }
        
        save_movie_to_db(db_payload)
        print(f"💾 Processed Database Sync for -> {fixed_data['Search_Query']}")

    except Exception as e:
        print(f"❌ Error extracting {movie_link}: {e}")

# =====================================================================
# 6. MASTER MATRIX SCRAPER
# =====================================================================
async def master_filmyzilla_scraper(bot_id, total_bots):
    print("=" * 60, flush=True)
    print(f"🚀 FILMYZILLA MATRIX BOT #{bot_id} of {total_bots} (Sitemap Mode) STARTED", flush=True)
    print("=" * 60, flush=True)

    all_links = get_all_movie_links_from_sitemap()
    if not all_links:
        print("❌ No links retrieved from sitemap. Exiting.", flush=True)
        return

    chunk_size = max(1, len(all_links) // total_bots)
    start_idx = (bot_id - 1) * chunk_size
    end_idx = len(all_links) if bot_id == total_bots else start_idx + chunk_size
    my_links = all_links[start_idx:end_idx]

    print(f"📋 Bot #{bot_id} assigned {len(my_links)} movies out of {len(all_links)} total.", flush=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        main_context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        await main_context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
        
        sem = asyncio.Semaphore(10)
        
        tasks = [scrape_and_save_movie(m_link, main_context, sem) for m_link in my_links]
        await asyncio.gather(*tasks)

        await browser.close()
        print(f"\n✅ Bot #{bot_id} finished its assigned sitemap chunk successfully! 🎉", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FilmyZilla Sitemap Matrix Scraper")
    parser.add_argument("--bot_id", type=int, default=1, help="ID of the current bot")
    parser.add_argument("--total_bots", type=int, default=1, help="Total running bots")
    args = parser.parse_args()

    asyncio.run(master_filmyzilla_scraper(args.bot_id, args.total_bots))
