"""
dead_link_cleaner.py — Memory-safe dead link checker.

Uses a PostgreSQL named (server-side) cursor so the result set is
streamed in small batches rather than loaded into RAM all at once.
This keeps RSS stable even when movie_files has millions of rows.
"""
import asyncio
import psycopg2
import nest_asyncio
import aiohttp
import os
from playwright.async_api import async_playwright

nest_asyncio.apply()

DATABASE_URL  = os.getenv("DATABASE_URL")
CHUNK_INDEX   = int(os.getenv("CHUNK_INDEX", 0))
TOTAL_CHUNKS  = int(os.getenv("TOTAL_CHUNKS", 1))

# How many rows to fetch from the DB at a time (server-side cursor)
# 1000 rows × ~200 bytes each ≈ 200 KB per batch — safe for GH Actions
DB_FETCH_SIZE = int(os.getenv("DB_FETCH_SIZE", "1000"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


async def check_link(session, context, file_id, url, title, sem, dead_ids):
    async with sem:
        if "pixel.hubcloud.cx" in url:
            print(f"[{CHUNK_INDEX}] DEAD (Token) | {title}")
            dead_ids.append(file_id)
            return

        try:
            async with session.get(url, timeout=10) as resp:
                status = resp.status
                content_type = resp.headers.get("Content-Type", "").lower()

                if status == 200 and "text/html" not in content_type and "text/plain" not in content_type:
                    print(f"[{CHUNK_INDEX}] ALIVE (Direct) | {title}")
                    return

                if status in [404, 403, 410] and "storage.googleapis.com" in url:
                    print(f"[{CHUNK_INDEX}] DEAD (Status {status}) | {title}")
                    dead_ids.append(file_id)
                    return
        except Exception:
            pass

        page = await context.new_page()
        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            body_text = await page.evaluate("() => document.body.innerText.toLowerCase()")

            dead_keywords = [
                "file not found", "deleted", "no longer available",
                "file has been removed", "returned to the void",
                "ran out", "no one came", "unable to get download link",
                "use another server",
            ]

            if any(kw in body_text for kw in dead_keywords):
                print(f"[{CHUNK_INDEX}] DEAD (Text match) | {title}")
                dead_ids.append(file_id)
            else:
                print(f"[{CHUNK_INDEX}] ALIVE (Web) | {title}")

        except Exception:
            print(f"[{CHUNK_INDEX}] DEAD (Error/Timeout) | {title}")
            dead_ids.append(file_id)
        finally:
            await page.close()


async def clean_database_chunk():
    conn = psycopg2.connect(DATABASE_URL)

    # Use a named server-side cursor so rows are streamed in batches
    # rather than being loaded all at once into Python memory.
    cur = conn.cursor("dead_link_cursor")
    cur.itersize = DB_FETCH_SIZE

    cur.execute(
        """
        SELECT mf.id, mf.url, m.title
        FROM movie_files mf
        JOIN movies m ON mf.movie_id = m.id
        WHERE mf.url IS NOT NULL
          AND mf.url != ''
          AND mf.url NOT LIKE '%%X-Amz-Credential%%'
          AND mf.id %% %s = %s
        """,
        (TOTAL_CHUNKS, CHUNK_INDEX),
    )

    # Count without loading all rows (server-side cursor has no rowcount)
    # We just report as we go.
    total_checked = 0
    total_dead    = 0

    # Plain write cursor for deletes
    write_cur = conn.cursor()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=HEADERS["User-Agent"])

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            sem = asyncio.Semaphore(15)

            # Fetch and process DB_FETCH_SIZE rows at a time
            while True:
                batch = cur.fetchmany(DB_FETCH_SIZE)
                if not batch:
                    break

                print(
                    f"Chunk {CHUNK_INDEX}: Processing batch of {len(batch)} links...",
                    flush=True,
                )

                dead_ids = []
                tasks = [
                    check_link(session, context, file_id, url, title, sem, dead_ids)
                    for file_id, url, title in batch
                ]
                await asyncio.gather(*tasks)

                if dead_ids:
                    write_cur.execute(
                        "DELETE FROM movie_files WHERE id = ANY(%s)", (dead_ids,)
                    )
                    conn.commit()
                    total_dead    += len(dead_ids)
                    print(
                        f"  Deleted {len(dead_ids)} dead links from this batch.",
                        flush=True,
                    )

                total_checked += len(batch)

        await browser.close()

    cur.close()
    write_cur.close()
    conn.close()

    print(
        f"\nChunk {CHUNK_INDEX} done. "
        f"Checked: {total_checked} | Dead/deleted: {total_dead}",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(clean_database_chunk())
