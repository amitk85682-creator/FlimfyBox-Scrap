"""
=========================================================================
 sites/hdhub4u.py — HDHub4u Site Plugin
=========================================================================
 Scraping rules for hdhub4u (currently new5.hdhub4u.cl).

 URL Discovery : Page-by-page crawling (no XML sitemap available)
 Data Extract  : JS evaluation for title, metadata, HubCloud links
 Bypass Logic  : Full HubCloud chain (Mediator → HubDrive → HubCloud
                 → Generate Link → Final server URLs)
=========================================================================
"""

import asyncio
import json
import re
from sites.base import BaseSitePlugin


class SitePlugin(BaseSitePlugin):
    SITE_NAME = "HDHub4u"
    TARGET_WEBSITE = "https://new5.hdhub4u.cl"
    WATCHDOG_LIMIT = 10  # Homepage shows fewer recent items

    # Safety cap: stop crawling after this many pages
    MAX_DISCOVERY_PAGES = 1200

    # Words that indicate navigation/category links (not movie posts)
    BAD_WORDS_JSON = json.dumps(
        [
            "hdhub4u.tv", "hdhub4u.bi", "home", "4k movies",
            "bollywood", "hollywood", "hindi dubbed", "south hindi",
            "web series", "genres", "disclaimer", "how to download",
            "join our group", "movie request page", "avoid fake",
            "latest releases",
        ]
    )

    # ==================================================================
    # 1. URL DISCOVERY — Pagination-based crawling
    # ==================================================================
    async def get_all_urls(self, context=None, watchdog_mode=False):
        """
        Crawl site pages sequentially and extract movie post links.
        Requires a Playwright BrowserContext for JS-rendered pages.
        """
        if context is None:
            print(
                "❌ HDHub4u requires a browser context for URL discovery.",
                flush=True,
            )
            return []

        # Optimization: In watchdog mode, only crawl the first 2 pages.
        max_pages = 2 if watchdog_mode else self.MAX_DISCOVERY_PAGES

        print(
            f"📥 Crawling {self.SITE_NAME} pages for URLs (Max Pages: {max_pages})...",
            flush=True,
        )

        all_urls = []
        seen = set()
        page = await context.new_page()

        # JS snippet reused on every page
        extract_js = (
            """() => {
            let links = Array.from(document.querySelectorAll('a'));
            let unique = [];
            let urls = new Set();
            let badWords = """
            + self.BAD_WORDS_JSON
            + """;

            links.forEach(a => {
                let href = a.href.toLowerCase();
                let title = (a.title || a.innerText).trim();
                let isBad = badWords.some(
                    bw => title.toLowerCase().includes(bw)
                );
                if (
                    title.length > 15 && !isBad &&
                    href.includes('hdhub4u') &&
                    !href.includes('/category/') &&
                    !href.includes('/page/') &&
                    !href.includes('/genre/') &&
                    !href.includes('/search/')
                ) {
                    if (!urls.has(href)) {
                        urls.add(href);
                        unique.push(a.href);
                    }
                }
            });
            return unique;
        }"""
        )

        page_num = 1
        consecutive_empty = 0

        while page_num <= max_pages:
            url = (
                f"{self.TARGET_WEBSITE}/page/{page_num}/"
                if page_num > 1
                else f"{self.TARGET_WEBSITE}/"
            )

            try:
                response = await page.goto(
                    url, timeout=30000, wait_until="domcontentloaded"
                )

                if response and response.status >= 400:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        break
                    page_num += 1
                    continue

                movies = await page.evaluate(extract_js)

                if not movies:
                    consecutive_empty += 1
                    if consecutive_empty >= 3:
                        print(
                            f"   📄 No more movies after page {page_num}. "
                            f"Stopping.",
                            flush=True,
                        )
                        break
                    page_num += 1
                    continue

                consecutive_empty = 0
                new_count = 0
                for m_url in movies:
                    if m_url not in seen:
                        seen.add(m_url)
                        all_urls.append(m_url)
                        new_count += 1

                # Progress logging
                if page_num % 50 == 0 or page_num <= 5:
                    print(
                        f"   📄 Page {page_num}: +{new_count} new "
                        f"(Total: {len(all_urls)})",
                        flush=True,
                    )

            except Exception as e:
                print(f"   ⚠️ Page {page_num} error: {e}", flush=True)
                consecutive_empty += 1
                if consecutive_empty >= 5:
                    break

            page_num += 1

        await page.close()
        print(
            f"✅ Discovered {len(all_urls)} URLs across "
            f"{page_num - 1} pages!",
            flush=True,
        )
        return all_urls

    # ==================================================================
    # 2. MOVIE DATA EXTRACTION
    # ==================================================================
    async def extract_movie_data(self, page):
        """
        Extract movie/series metadata and raw download links from
        an HDHub4u movie page using in-page JS evaluation.
        """
        try:
            # ── Metadata ─────────────────────────────────────────────
            scraped_data = await page.evaluate(
                r"""() => {
                let text = document.body.innerText;
                let lines = text.split('\n')
                    .map(l => l.trim())
                    .filter(l => l.length > 0);
                let d = {
                    Raw_Title: 'N/A', IMDb: 'N/A', Genre: 'N/A',
                    Stars: 'N/A', Director: 'N/A', Creator: 'N/A',
                    Language: 'N/A', Description: 'N/A',
                    Poster: '', Type: 'Movies'
                };

                // Title from <h1>
                let h1 = document.querySelector(
                    'h1.entry-title, h1.post-title, h1'
                );
                if (h1) d.Raw_Title = h1.innerText.trim();

                // Fallback: line before "iMDB Rating:"
                if (d.Raw_Title === 'N/A' || d.Raw_Title.length < 5) {
                    let idx = lines.findIndex(
                        l => l.toLowerCase().includes('imdb rating:')
                    );
                    if (idx > 0) d.Raw_Title = lines[idx - 1];
                }

                // Fallback: line containing (YYYY)
                if (d.Raw_Title === 'N/A' || d.Raw_Title.length < 5) {
                    for (let line of lines) {
                        if (
                            line.match(/\(\d{4}\)/) &&
                            line.length > 10 &&
                            line.length < 200
                        ) {
                            d.Raw_Title = line;
                            break;
                        }
                    }
                }

                d.Raw_Title = d.Raw_Title
                    .replace(/[\uE000-\uF8FF]/g, '')
                    .trim();

                // Metadata fields
                let m;
                m = text.match(/iMDB Rating:\s*(.*)/i);
                if (m) d.IMDb = m[1].trim();

                m = text.match(/Genre:\s*(.*)/i);
                if (m) d.Genre = m[1].trim();

                m = text.match(/Stars:\s*(.*)/i);
                if (m) d.Stars = m[1].trim();

                m = text.match(/Director:\s*(.*)/i);
                if (m) d.Director = m[1].trim();

                m = text.match(/Creator:\s*(.*)/i);
                if (m) {
                    d.Creator = m[1].trim();
                    d.Type = 'Web Series';
                } else if (
                    text.match(/No\.\s*of\s*Episodes:/i) ||
                    (d.Raw_Title && d.Raw_Title.match(/Season/i))
                ) {
                    d.Type = 'Web Series';
                }

                m = text.match(/Language:\s*(.*)/i);
                if (m) d.Language = m[1].trim();

                m = text.match(
                    /(?:Storyline|Plot):([\s\S]*?)(?:Director:|Stars:|Genre:|$)/i
                );
                if (m) d.Description = m[1].trim();

                return d;
            }"""
            )

            # ── Raw download links ───────────────────────────────────
            raw_links = await page.evaluate(
                r"""() => {
                let links = Array.from(document.querySelectorAll('a'));
                let results = [];
                let kw = [
                    'hubdrive', 'hubcloud', 'hubcdn',
                    'greenmountmotors', 'inventoryidea',
                    'indishare', 'sendit', 'clicknupload',
                    'upload.mn', 'openload', 'bdupload',
                    '9xupload', 'uploadbaz', 'upfile',
                    'more download links', '9xplay'
                ];

                links.forEach(a => {
                    let href = a.href.toLowerCase();
                    let btn = a.innerText.trim();
                    let par = a.parentElement
                        ? a.parentElement.innerText
                            .trim()
                            .replace(/\n/g, ' ')
                        : '';

                    let isTarget = kw.some(
                        k => href.includes(k) || btn.toLowerCase().includes(k)
                    );
                    if (!isTarget) return;
                    if (btn.toLowerCase().includes('sample')) return;

                    // Episode / combined context detection
                    let epCtx = '';
                    let isCombined = false;
                    let node = a;

                    for (let i = 0; i < 4 && node; i++) {
                        let prev = node.previousElementSibling;
                        for (let j = 0; j < 10 && prev; j++) {
                            let pt = (prev.innerText || '')
                                .trim()
                                .toLowerCase();
                            let ep = pt.match(
                                /(?:episode|ep)\s*[\-:]?\s*(\d{1,3})/i
                            );
                            if (ep) {
                                epCtx = 'E' +
                                    ep[1].padStart(2, '0');
                                break;
                            }
                            if (
                                pt.includes('download links') ||
                                pt.includes('full series') ||
                                pt.includes('complete season') ||
                                pt.includes('zip') ||
                                pt.includes('batch') ||
                                pt.includes('pack')
                            ) {
                                isCombined = true;
                                break;
                            }
                            prev = prev.previousElementSibling;
                        }
                        if (epCtx || isCombined) break;
                        node = node.parentElement;
                    }

                    let ctx = par.length < 100 ? par : btn;
                    let prefix = '';
                    if (epCtx) prefix += '[' + epCtx + '] ';
                    if (isCombined && !epCtx) prefix += '[COMBINED] ';
                    let fq = prefix + ctx;

                    if (!results.find(r => r.url === a.href)) {
                        results.push({
                            quality: fq,
                            size: '',
                            url: a.href
                        });
                    }
                });
                return results;
            }"""
            )

            if not scraped_data:
                return None

            scraped_data["raw_download_links"] = raw_links or []
            return scraped_data

        except Exception as e:
            print(f"   ⚠️ Extract error: {e}", flush=True)
            return None

    # ==================================================================
    # 3. BYPASS LOGIC — Full HubCloud Chain
    # ==================================================================
    async def bypass_links(self, context, browser, raw_links):
        """
        HDHub4u bypass chain per download link:
          HubCDN Mediator → HubDrive → HubCloud → Generate → Servers
        Each link gets its own isolated BrowserContext for safety.
        """

        async def _bypass_mediator(ctx, target_url):
            """Navigate the HubCDN mediator page to get HubDrive URL."""
            page = await ctx.new_page()
            try:
                await page.goto(target_url, timeout=60000)
                await page.wait_for_timeout(10000)

                # Dead link check
                dead = await page.evaluate(
                    """() => {
                    let t = document.body.innerText.toLowerCase();
                    return t.includes('file not found') ||
                           t.includes('file was deleted') ||
                           t.includes('no longer available') ||
                           t.includes('404 not found');
                }"""
                )
                if dead:
                    return None

                # Step 1: CLICK TO CONTINUE
                ok1 = await page.evaluate(
                    """() => {
                    let b = Array.from(
                        document.querySelectorAll('a, button')
                    ).find(e => e.innerText.includes('CLICK TO CONTINUE'));
                    if (b) { b.click(); return true; }
                    return false;
                }"""
                )
                if not ok1:
                    return None
                await page.wait_for_timeout(12000)

                # Step 2: GET LINKS
                ok2 = await page.evaluate(
                    """() => {
                    let b = Array.from(
                        document.querySelectorAll('a, button')
                    ).find(e => e.innerText.includes('GET LINKS'));
                    if (b) { b.click(); return true; }
                    return false;
                }"""
                )
                if not ok2:
                    return None
                await page.wait_for_timeout(10000)

                # Step 3: Find link across all open tabs
                for tab in ctx.pages:
                    link = await tab.evaluate(
                        """() => {
                        let b = Array.from(
                            document.querySelectorAll('a, button')
                        ).find(e =>
                            e.innerText.includes('Download Here')
                        );
                        return b ? b.href : null;
                    }"""
                    )
                    if not link:
                        link = await tab.evaluate(
                            """() => {
                            let a = Array.from(
                                document.querySelectorAll('a')
                            ).find(a =>
                                a.href.toLowerCase().includes('hubdrive') ||
                                a.href.toLowerCase().includes('hubcloud')
                            );
                            return a ? a.href : null;
                        }"""
                        )
                    if link:
                        return link
                return None
            except Exception:
                return None
            finally:
                await page.close()

        async def _bypass_hubcloud(ctx, hubdrive_url):
            """Navigate HubDrive → HubCloud → direct download servers."""
            page = await ctx.new_page()
            try:
                await page.goto(
                    hubdrive_url,
                    timeout=60000,
                    wait_until="domcontentloaded",
                )
                await page.wait_for_timeout(4000)

                # Dead link check
                dead = await page.evaluate(
                    """() => {
                    let t = document.body.innerText.toLowerCase();
                    return t.includes('file not found') ||
                           t.includes('file was deleted') ||
                           t.includes('no longer available') ||
                           t.includes('404 not found') ||
                           t.includes('file has been deleted');
                }"""
                )
                if dead:
                    return None

                # Find HubCloud server link
                hc_url = await page.evaluate(
                    """() => {
                    let a = Array.from(
                        document.querySelectorAll('a')
                    ).find(a =>
                        a.innerText.toLowerCase()
                            .includes('hubcloud server')
                    );
                    return a ? a.href : null;
                }"""
                )
                hc_url = hc_url or (
                    hubdrive_url
                    if "hubcloud" in hubdrive_url
                    else None
                )
                if not hc_url:
                    return None

                if page.url != hc_url:
                    await page.goto(
                        hc_url,
                        timeout=60000,
                        wait_until="domcontentloaded",
                    )
                await page.wait_for_timeout(8000)

                # Final dead check
                dead2 = await page.evaluate(
                    """() => {
                    let t = document.body.innerText.toLowerCase();
                    return t.includes('file not found') ||
                           t.includes('file was deleted') ||
                           t.includes('no longer available');
                }"""
                )
                if dead2:
                    return None

                # Click "Generate Direct Download Link"
                gen_url = await page.evaluate(
                    """() => {
                    let a = Array.from(
                        document.querySelectorAll('a')
                    ).find(a =>
                        a.innerText.toLowerCase().includes('generate') ||
                        a.innerText.toLowerCase()
                            .includes('direct download')
                    );
                    return a ? a.href : null;
                }"""
                )

                if gen_url and "http" in gen_url:
                    await page.goto(
                        gen_url,
                        timeout=60000,
                        wait_until="domcontentloaded",
                    )
                else:
                    try:
                        await page.locator(
                            'text="Generate Direct Download Link"'
                        ).click()
                    except Exception:
                        return None

                await page.wait_for_timeout(8000)

                # Extract final server links
                servers = await page.evaluate(
                    """() => {
                    let out = [];
                    Array.from(document.querySelectorAll('a'))
                        .forEach(a => {
                            let t = a.innerText.trim();
                            let lo = t.toLowerCase();
                            if (
                                lo.includes('server') ||
                                lo.includes('fsl') ||
                                lo.includes('pixel') ||
                                lo.includes('buzz') ||
                                lo.includes('10gbps')
                            ) {
                                out.push({
                                    server_name: t,
                                    url: a.href
                                });
                            }
                        });
                    return out;
                }"""
                )
                return servers if servers else None

            except Exception:
                return None
            finally:
                await page.close()

        async def _process_single(raw_link):
            """Process one raw download link through the bypass chain."""
            target = raw_link["url"]
            quality = raw_link["quality"]
            size = raw_link.get("size", "")
            direct_links = []

            # Each bypass gets its own isolated context
            ua = (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
            bypass_ctx = await browser.new_context(user_agent=ua)

            try:
                if any(
                    kw in target
                    for kw in [
                        "greenmountmotors",
                        "inventoryidea",
                        "hubcdn",
                    ]
                ):
                    # Route A: Mediator → HubDrive/Cloud → Final
                    med = await _bypass_mediator(bypass_ctx, target)
                    if med and (
                        "hubcloud" in med or "hubdrive" in med
                    ):
                        chain = await _bypass_hubcloud(bypass_ctx, med)
                        if chain and isinstance(chain, list):
                            direct_links = chain
                    elif med:
                        direct_links = [
                            {"server_name": "Direct", "url": med}
                        ]

                elif any(
                    kw in target for kw in ["hubdrive", "hubcloud"]
                ):
                    # Route B: Direct HubDrive/Cloud → Final
                    chain = await _bypass_hubcloud(bypass_ctx, target)
                    if chain and isinstance(chain, list):
                        direct_links = chain

            except Exception as e:
                print(
                    f"   ⚠️ Bypass error ({target}): {e}", flush=True
                )
            finally:
                await bypass_ctx.close()

            return {
                "quality": quality,
                "size": size,
                "direct_links": direct_links,
            }

        # Use a Semaphore to limit concurrent browser contexts per movie
        # to avoid OOM crashes on the runner, but fast enough to overlap waits.
        bypass_sem = asyncio.Semaphore(5)

        async def _sem_process(raw):
            async with bypass_sem:
                return await _process_single(raw)

        # Process links concurrently instead of sequentially
        tasks = [_sem_process(raw) for raw in raw_links]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for r in gathered:
            if isinstance(r, Exception):
                print(f"   ⚠️ Link bypass failed: {r}", flush=True)
            elif r and r.get("direct_links"):
                results.append(r)

        return results
