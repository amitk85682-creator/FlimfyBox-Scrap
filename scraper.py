# =====================================================================
# scraper.py (GitHub Ready Matrix Version)
# =====================================================================
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
from playwright.async_api import async_playwright
from pyvirtualdisplay import Display

nest_asyncio.apply()

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres.vzixjxeppvpxrhntaidb:l0aDck2NUeD4Jws5@aws-0-ap-northeast-1.pooler.supabase.com:6543/postgres")
TMDB_KEY = "9fa44f5e9fbd41415df930ce5b81c4d7"
TARGET_WEBSITE = "https://mkvcinemas.hn"

ADULT_KEYWORDS = [
    '18+', 'adult', 'ullu', 'kooku', 'hotshots', 'primeplay', 'primeshots', 
    'hunters', 'rabbit', 'besharams', 'nuefliks', 'uncensored', 'erotic'
]

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, connect_timeout=10)

def parse_season_episode(raw_title, group_name):
    combined = f"{raw_title} {group_name}"
    m_se = re.search(r'(?i)s(\d{1,2})\s*e(\d{1,3})', combined)
    if m_se:
        return f"S{int(m_se.group(1)):02d}E{int(m_se.group(2)):02d}"
        
    s_match = re.search(r'(?i)s(?:eason)?\.?\s*(\d+)', raw_title)
    season = int(s_match.group(1)) if s_match else 1
    
    ep_match = re.search(r'(?i)(?:episode|ep|epi)\s*(\d+)', combined) or re.search(r'(?i)\b(\d{1,3})\b', group_name)
    if ep_match:
        ep = int(ep_match.group(1))
        return f"S{season:02d}E{ep:02d}"
        
    return ''

def save_movie_to_db(data_dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        tmdb = data_dict.get('tmdb_data') or {}
        raw_title = data_dict.get('raw_title', '')
        
        title = tmdb.get('Title') or raw_title
        if title:
            title = re.sub(r'(?i)\b(season\s*\d+|s\d+|episodes?|ep\s*\d+|hdrip|webrip|web-dl|hdtc|720p|1080p|480p|hindi|tv show|hd|uncut|dual\s*audio|dubbed|x264|hevc|aac)\b.*', '', title)
            title = re.sub(r'\b(19|20)\d{2}\b', '', title)
            title = re.sub(r'[\(\)\[\]\-+]+', ' ', title)
            title = re.sub(r'\s+', ' ', title).strip()
            
        year = tmdb.get('Release', '')[:4] if tmdb.get('Release') else 'N/A'
        poster = tmdb.get('Poster') or ''
        tmdb_id = str(tmdb.get('tmdb_id', '')) if tmdb.get('tmdb_id') else None
        
        try:
            year_val = int(year)
        except:
            year_val = None
        
        cur.execute("""
            INSERT INTO movies (url, title, poster_url, year, genre, description, rating, language, "cast", imdb_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (title) DO UPDATE SET 
                url = EXCLUDED.url,
                poster_url = EXCLUDED.poster_url,
                year = EXCLUDED.year,
                genre = EXCLUDED.genre,
                description = EXCLUDED.description,
                rating = EXCLUDED.rating,
                language = EXCLUDED.language,
                "cast" = EXCLUDED."cast",
                imdb_id = COALESCE(movies.imdb_id, EXCLUDED.imdb_id)
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
            for group in bypassed_links:
                group_name = group.get('group_name', '') 
                extra_info = parse_season_episode(raw_title, group_name) 
                quality = "720p" if "720P" in group_name.upper() else ("1080p" if "1080P" in group_name.upper() else "480p")
                
                direct_links_data = group.get('final_bypassed_servers')
                if isinstance(direct_links_data, list):
                    for srv in direct_links_data:
                        srv_url = srv.get('url', '').strip()
                        srv_name = srv.get('server_name', '').strip()
                        if srv_url:
                            cur.execute(
                                "SELECT id FROM movie_files WHERE movie_id = %s AND quality = %s AND server_name = %s AND extra_info = %s",
                                (movie_id, quality, srv_name, extra_info)
                            )
                            if cur.fetchone():
                                cur.execute("""
                                    UPDATE movie_files SET url = %s, source = 'scraped'
                                    WHERE movie_id = %s AND quality = %s AND server_name = %s AND extra_info = %s
                                """, (srv_url, movie_id, quality, srv_name, extra_info))
                            else:
                                cur.execute("""
                                    INSERT INTO movie_files (movie_id, quality, server_name, url, extra_info, source)
                                    VALUES (%s, %s, %s, %s, %s, 'scraped')
                                """, (movie_id, quality, srv_name, srv_url, extra_info))
                                
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"⚠️ DB Save Error: {e}", flush=True)

async def bypass_hubcloud_chain(context, hubcloud_url):
    page = await context.new_page()
    try:
        await page.goto(hubcloud_url, timeout=45000, wait_until="domcontentloaded")
        await page.wait_for_timeout(5000)

        gamerxyt_url = await page.evaluate('''() => {
            let links = Array.from(document.querySelectorAll('a'));
            let target = links.find(a => a.innerText.toLowerCase().includes('generate') || a.innerText.toLowerCase().includes('direct download'));
            return target ? target.href : null;
        }''')

        if gamerxyt_url and 'http' in gamerxyt_url:
            await page.goto(gamerxyt_url, timeout=45000, wait_until="domcontentloaded")
        else:
            await page.locator('text="Generate Direct Download Link"').click()
            
        await page.wait_for_timeout(5000)

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
    except:
        return None
    finally:
        await page.close()

def fix_movie_details(raw_title, movie_url):
    raw_title = raw_title.replace('🎬', '').strip()
    search_query = 'UNKNOWN_TITLE'
    year = 'N/A'
    media_type = 'Movies'

    if raw_title and raw_title != 'N/A':
        junk_pattern = r'(?i)\b(hindi|tamil|telugu|malayalam|dual audio|uncut|dubbed|movie|south|full|download|web-dl|webrip|bluray|hdrip|hevc|x264|x265|esub|esubs|480p|720p|1080p|4k|mkv|mp4|hd)\b'
        clean = re.sub(r'[\(\)\[\]\-+]+', ' ', raw_title)
        
        year_match = re.search(r'\b(19\d{2}|20\d{2})\b', clean)
        if year_match:
            year = year_match.group(1)
            clean = clean.replace(year, ' ')
            
        clean = re.sub(junk_pattern, ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        search_query = clean if clean else "UNKNOWN_TITLE"

    if re.search(r'(?i)\b(season|episodes?|series|tv show)\b', raw_title):
        media_type = 'TV Series'

    return {"Search_Query": search_query, "Year": year, "Type": media_type}

def get_tmdb_details(fixed_data):
    search_query = fixed_data['Search_Query']
    year_hint = fixed_data['Year']
    endpoint = 'tv' if fixed_data['Type'] == 'TV Series' else 'movie'
    clean_tmdb_query = re.sub(r'(?i)\b(season\s*\d+|s\d+|episodes?|ep\s*\d+)\b', '', search_query).strip()
    
    url = f"https://api.themoviedb.org/3/search/{endpoint}?api_key={TMDB_KEY}&query={urllib.parse.quote(clean_tmdb_query)}"
    if year_hint and year_hint != 'N/A':
        if endpoint == 'tv': url += f"&first_air_date_year={year_hint}"
        else: url += f"&year={year_hint}"
        
    try:
        response = requests.get(url, timeout=10).json()
        results = response.get('results', [])
        if not results: return None

        best_match = results[0]
        tmdb_id = best_match.get('id')
        details_url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}?api_key={TMDB_KEY}"
        details = requests.get(details_url, timeout=10).json()
        genres = [g['name'] for g in details.get('genres', [])]
        
        return {
            "Title": best_match.get('name') or best_match.get('title'),
            "Release": best_match.get('first_air_date') or best_match.get('release_date', 'N/A'),
            "Genre": ", ".join(genres) if genres else "N/A",
            "TMDb_Rating": str(round(details.get('vote_average', 0), 1)) if details.get('vote_average') else 'N/A',
            "tmdb_id": tmdb_id,
            "is_tv": endpoint == 'tv'
        }
    except:
        return None

async def extract_servers_grouped(context, intermediate_url):
    page = await context.new_page()
    try:
        await page.goto(intermediate_url, timeout=25000, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000) 
        
        grouped_data = await page.evaluate(r'''() => {
            let finalArray = [];
            let currentGroup = "Default Link Group";
            let groupsMap = {};

            let nodes = document.querySelectorAll('h1, h2, h3, h4, h5, p, a, div[class*="title"], b, strong');
            nodes.forEach(node => {
                let text = node.innerText.trim().toUpperCase();
                if (node.tagName !== 'A' && (text.includes('480P') || text.includes('720P') || text.includes('1080P') || text.includes('4K') || text.includes('EPISODE'))) {
                    if(text.length < 50) { 
                        currentGroup = text;
                        if(!groupsMap[currentGroup]) groupsMap[currentGroup] = [];
                    }
                }

                if (node.tagName === 'A') {
                    let href = node.href;
                    if ((text.includes('GDFLIX') || text.includes('VCLOUD') || text.includes('DIRECT') || text.includes('HUBCLOUD')) && href.startsWith('http') && !href.includes('mkvcinemas')) {
                        if(!groupsMap[currentGroup]) groupsMap[currentGroup] = [];
                        let exists = groupsMap[currentGroup].find(x => x.url === href);
                        if (!exists) {
                            groupsMap[currentGroup].push({ server_name: text, url: href });
                        }
                    }
                }
            });

            for (let q in groupsMap) {
                if (groupsMap[q].length > 0) {
                    finalArray.push({ group_name: q, direct_links: groupsMap[q] });
                }
            }
            return finalArray;
        }''')
        return grouped_data
    except:
        return []
    finally:
        await page.close()

async def scrape_and_save_movie(movie_link, main_context, sem):
    async with sem:
        print(f"\n🎬 [START] Processing: {movie_link}", flush=True)
        movie_page = await main_context.new_page()
        try:
            await movie_page.goto(movie_link, timeout=40000, wait_until="domcontentloaded")
            
            page_text = await movie_page.evaluate("() => document.body.innerText.toLowerCase()")
            if any(word in page_text for word in ADULT_KEYWORDS) and "18 plus" in page_text:
                print(f"   🔞 [SKIP] Adult content detected -> {movie_link}", flush=True)
                await movie_page.close()
                return

            movie_data = await movie_page.evaluate(r'''() => {
                let details = { Raw_Title: '', IntermediateUrls: [] };
                let titleTag = document.querySelector('title');
                if(titleTag) details.Raw_Title = titleTag.innerText.split('|')[0].replace(/mkvcinemas/ig, '').trim();

                let h1 = document.querySelector('h1.entry-title, h1');
                if (h1 && h1.innerText.length > 5) details.Raw_Title = h1.innerText.trim();

                let allLinks = Array.from(document.querySelectorAll('a'));
                for (let a of allLinks) {
                    let text = a.innerText.trim().toUpperCase();
                    if (text.includes('DOWNLOAD') && (text.includes('480P') || text.includes('720P') || text.includes('1080P'))) {
                        if (a.href.startsWith('http') && !a.href.includes('#')) {
                            details.IntermediateUrls.push(a.href);
                        }
                    }
                }
                return details;
            }''')
            await movie_page.close() 

            if not movie_data['IntermediateUrls']:
                print(f"   ⚠️ [SKIP] No download links found on page.", flush=True)
                return

            fixed_data = fix_movie_details(movie_data['Raw_Title'], movie_link)
            tmdb_data = get_tmdb_details(fixed_data)

            all_bypassed_links_data = []
            for inter_url in movie_data['IntermediateUrls']:
                bypassed_links_data = await extract_servers_grouped(main_context, inter_url)

                for group in bypassed_links_data:
                    group_bypassed = False
                    for link_obj in group['direct_links']:
                        if group_bypassed:
                            link_obj['final_bypassed_servers'] = "Skipped (Duplicate Mirror)"
                            continue
                        
                        target_url = link_obj['url'].lower()
                        if 'hubcloud' in target_url:
                            final_servers = await bypass_hubcloud_chain(main_context, link_obj['url'])
                            if final_servers:
                                link_obj['final_bypassed_servers'] = final_servers
                                group_bypassed = True 
                            else:
                                link_obj['final_bypassed_servers'] = "Bypass Failed"
                        else:
                            link_obj['final_bypassed_servers'] = "Skipped (Preferring HubCloud)"
                
                all_bypassed_links_data.extend(bypassed_links_data)

            db_payload = {
                "url": movie_link,
                "raw_title": movie_data['Raw_Title'],
                "tmdb_data": tmdb_data,
                "bypassed_links": all_bypassed_links_data
            }
            save_movie_to_db(db_payload)
            print(f"   💾 [SAVED/UPDATED] Successfully processed: {fixed_data['Search_Query']}", flush=True)

        except Exception as e:
            print(f"   ❌ [ERROR] Processing {movie_link}: {e}", flush=True)
            if not movie_page.is_closed():
                await movie_page.close()

async def master_mkvcinemas_scraper(bot_id, total_bots):
    print("=" * 60, flush=True)
    print(f"🚀 MATRIX BOT #{bot_id} of {total_bots} STARTED", flush=True)
    print("=" * 60, flush=True)

    display = Display(visible=0, size=(1920, 1080))
    display.start()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"]
        )
        main_context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        
        page = await main_context.new_page()
        sem = asyncio.Semaphore(10)

        # Distribute work cleanly across 15 bots (assuming ~90 total pages)
        total_pages = 90
        pages_per_bot = max(1, total_pages // total_bots)
        start_page = ((bot_id - 1) * pages_per_bot) + 1
        end_page = total_pages if bot_id == total_bots else start_page + pages_per_bot - 1

        page_num = start_page
        while page_num <= end_page:
            current_page_url = f"{TARGET_WEBSITE}/page/{page_num}/" if page_num > 1 else f"{TARGET_WEBSITE}/"
            print(f"\n🌐 Bot #{bot_id} Scanning Page {page_num} (Range {start_page}-{end_page}): {current_page_url}", flush=True)
            
            try:
                response = await page.goto(current_page_url, timeout=45000, wait_until="domcontentloaded")
                if response and response.status == 404:
                    break
            except Exception as e:
                page_num += 1
                continue

            movie_links = await page.evaluate(r'''() => {
                let links = Array.from(document.querySelectorAll('a'));
                let unique = [];
                let urls = new Set();
                let movieUrlRegex = /\/\d{4,7}\/[^\/]+\/?$/i;

                links.forEach(a => {
                    let href = a.href;
                    if (href.includes('mkvcinemas') && movieUrlRegex.test(href)) {
                        if (!urls.has(href)) {
                            urls.add(href);
                            unique.push(href);
                        }
                    }
                });
                return unique;
            }''')

            if not movie_links:
                page_num += 1
                continue

            tasks = [scrape_and_save_movie(m_link, main_context, sem) for m_link in movie_links]
            await asyncio.gather(*tasks)

            page_num += 1

        await browser.close()
    display.stop()
    print(f"\n✅ Bot #{bot_id} finished its assigned range successfully!", flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Matrix Scraper Bot")
    parser.add_argument("--bot_id", type=int, default=1, help="ID of the current bot")
    parser.add_argument("--total_bots", type=int, default=1, help="Total running bots")
    args = parser.parse_args()

    asyncio.run(master_mkvcinemas_scraper(args.bot_id, args.total_bots))
