import asyncio
import psycopg2
import nest_asyncio
import aiohttp
import os
from playwright.async_api import async_playwright

nest_asyncio.apply()

# GitHub Secrets aur Matrix se variables aayenge
DATABASE_URL = os.getenv("DATABASE_URL") 
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", 0))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", 1))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

async def check_link(session, context, file_id, url, title, sem, dead_ids):
    async with sem:
        if "pixel.hubcloud.cx" in url:
            print(f"[{CHUNK_INDEX}] ❌ DEAD (Token) | {title}")
            dead_ids.append(file_id)
            return

        is_direct_file = False
        try:
            async with session.get(url, timeout=10) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type", "").lower()
                
                if status == 200 and "text/html" not in content_type and "text/plain" not in content_type:
                    print(f"[{CHUNK_INDEX}] ✅ ALIVE (Direct) | {title}")
                    return 
                    
                if status in [404, 403, 410] and "storage.googleapis.com" in url:
                     print(f"[{CHUNK_INDEX}] ❌ DEAD (Status {status}) | {title}")
                     dead_ids.append(file_id)
                     return
        except Exception:
            pass 

        page = await context.new_page()
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            body_text = await page.evaluate("() => document.body.innerText.toLowerCase()")
            
            dead_keywords = [
                'file not found', 'deleted', 'no longer available', 
                'file has been removed', 'returned to the void', 
                'ran out', 'no one came', 'unable to get download link',
                'use another server'
            ]
            
            if any(kw in body_text for kw in dead_keywords):
                print(f"[{CHUNK_INDEX}] ❌ DEAD (Text match) | {title}")
                dead_ids.append(file_id)
            else:
                print(f"[{CHUNK_INDEX}] ✅ ALIVE (Web) | {title}")
                
        except Exception:
            print(f"[{CHUNK_INDEX}] ❌ DEAD (Error/Timeout) | {title}")
            dead_ids.append(file_id)
        finally:
            await page.close()

async def clean_database_chunk():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Modulo Logic: Apne hisse ki IDs uthayega
    cur.execute("""
        SELECT mf.id, mf.url, m.title 
        FROM movie_files mf
        JOIN movies m ON mf.movie_id = m.id
        WHERE mf.url IS NOT NULL 
        AND mf.url != ''
        AND mf.url NOT LIKE '%%X-Amz-Credential%%'
        AND mf.id %% %s = %s
    """, (TOTAL_CHUNKS, CHUNK_INDEX))
    
    files = cur.fetchall()
    
    if not files:
        print(f"✅ Chunk {CHUNK_INDEX}: No links found to check.")
        cur.close()
        conn.close()
        return

    print(f"🚀 Chunk {CHUNK_INDEX}: Checking {len(files)} links concurrently...\n")

    dead_ids = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=HEADERS["User-Agent"])
        
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            sem = asyncio.Semaphore(15) 
            tasks = [check_link(session, context, file_id, url, title, sem, dead_ids) for file_id, url, title in files]
            await asyncio.gather(*tasks)
            
        await browser.close()

    if dead_ids:
        print(f"\n🗑️ Chunk {CHUNK_INDEX}: Deleting {len(dead_ids)} DEAD links...")
        cur.execute("DELETE FROM movie_files WHERE id = ANY(%s)", (dead_ids,))
        conn.commit()
    else:
        print(f"\n✅ Chunk {CHUNK_INDEX}: All links ALIVE. Nothing to delete.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    asyncio.run(clean_database_chunk())
