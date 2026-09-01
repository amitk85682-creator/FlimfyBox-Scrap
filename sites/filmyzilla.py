"""
=========================================================================
 sites/filmyzilla.py — FilmyZilla Site Plugin
=========================================================================
 Scraping rules for filmyzilla63.com (or whichever current mirror).

 URL Discovery : XML Sitemap parsing (fast, no browser needed)
 Data Extract  : JS evaluation for title, poster, metadata, quality links
 Bypass Logic  : Navigate to server selection pages, extract server URLs
=========================================================================
"""

import asyncio
import re
import requests
import xml.etree.ElementTree as ET
from sites.base import BaseSitePlugin


class SitePlugin(BaseSitePlugin):
    SITE_NAME = "FilmyZilla"
    TARGET_WEBSITE = "https://www.filmyzilla63.com"
    WATCHDOG_LIMIT = 50

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    # ==================================================================
    # 1. URL DISCOVERY — XML Sitemap
    # ==================================================================
    async def get_all_urls(self, context=None, watchdog_mode=False):
        """
        Fetch all movie URLs from the XML sitemap.
        Does NOT need a browser context — uses requests internally.
        """
        print(f"📥 Fetching {self.SITE_NAME} sitemap...", flush=True)
        movie_links = []

        try:
            # Try the main sitemap index first
            sitemap_url = f"{self.TARGET_WEBSITE}/sitemap.xml"
            resp = requests.get(sitemap_url, headers=self.HEADERS, timeout=20)

            # Some mirrors redirect sitemap.xml to HTML; try post-sitemap
            if resp.status_code != 200 or "<html" in resp.text.lower():
                sitemap_url = f"{self.TARGET_WEBSITE}/post-sitemap.xml"
                resp = requests.get(
                    sitemap_url, headers=self.HEADERS, timeout=20
                )

            if resp.status_code != 200 or "<html" in resp.text.lower():
                print("❌ Could not fetch sitemap XML.", flush=True)
                return []

            root = ET.fromstring(resp.content)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            urls_found = set()
            sitemaps = root.findall("ns:sitemap", ns)

            if sitemaps:
                # ── Sitemap Index: fetch each sub-sitemap ────────────
                for sm in sitemaps:
                    loc_elem = sm.find("ns:loc", ns)
                    if loc_elem is None or not loc_elem.text:
                        continue
                    loc = loc_elem.text

                    # Only process post / movie sub-sitemaps
                    if not any(kw in loc for kw in ["post", "movie", "sitemap"]):
                        continue

                    try:
                        sub_resp = requests.get(
                            loc, headers=self.HEADERS, timeout=15
                        )
                        if (
                            sub_resp.status_code == 200
                            and "<html" not in sub_resp.text.lower()
                        ):
                            sub_root = ET.fromstring(sub_resp.content)
                            for url_elem in sub_root.findall("ns:url", ns):
                                url_loc = url_elem.find("ns:loc", ns)
                                if (
                                    url_loc is not None
                                    and url_loc.text
                                    and "/movie/" in url_loc.text
                                ):
                                    urls_found.add(url_loc.text)
                    except Exception as e:
                        print(
                            f"   ⚠️ Sub-sitemap error ({loc}): {e}",
                            flush=True,
                        )
            else:
                # ── Flat sitemap: extract <url> entries directly ─────
                for url_elem in root.findall("ns:url", ns):
                    url_loc = url_elem.find("ns:loc", ns)
                    if (
                        url_loc is not None
                        and url_loc.text
                        and "/movie/" in url_loc.text
                    ):
                        urls_found.add(url_loc.text)

            movie_links = list(urls_found)
            print(
                f"✅ Discovered {len(movie_links)} movie URLs from sitemap!",
                flush=True,
            )

        except Exception as e:
            print(f"❌ Sitemap fetch error: {e}", flush=True)

        return movie_links

    # ==================================================================
    # 2. MOVIE DATA EXTRACTION
    # ==================================================================
    async def extract_movie_data(self, page):
        """
        Extract metadata + download quality links from a FilmyZilla
        movie page via in-page JS evaluation.
        """
        try:
            data = await page.evaluate(
                r"""() => {
                let details = {
                    Raw_Title: '', Stars: 'N/A', Genre: 'N/A',
                    Language: 'Hindi', Description: 'N/A', Poster: '',
                    IMDb: 'N/A', Director: 'N/A', Creator: 'N/A',
                    Type: 'Movies', QualityLinks: []
                };

                let fullText = document.body.innerText;

                // ── Title ───────────────────────────────────────────
                let titleMatch = fullText.match(
                    /Movie Name\s*:\s*([^\n]+)/i
                );
                if (titleMatch) {
                    details.Raw_Title = titleMatch[1].trim();
                } else {
                    let tag = document.querySelector('title');
                    if (tag) {
                        details.Raw_Title = tag.innerText
                            .split('|')[0]
                            .replace(/FilmyZilla/ig, '')
                            .trim();
                    }
                }

                // ── Poster ──────────────────────────────────────────
                let imgs = Array.from(document.querySelectorAll('img'));
                let valid = imgs.find(img =>
                    !img.src.toLowerCase().includes('logo') &&
                    !img.src.toLowerCase().includes('icon') &&
                    img.naturalWidth > 100
                );
                if (valid) details.Poster = valid.src;

                // ── Metadata fields ─────────────────────────────────
                let sm = fullText.match(/Starcast\s*:\s*([^\n]+)/i);
                if (sm) details.Stars = sm[1].trim();

                let gm = fullText.match(/Genres?\s*:\s*([^\n]+)/i);
                if (gm) details.Genre = gm[1].trim();

                let lm = fullText.match(/Language\s*:\s*([^\n]+)/i);
                if (lm) details.Language = lm[1].trim();

                let dm = fullText.match(
                    /(?:Storyline|Story|Movie Story)\s*:\s*([^\n]+)/i
                );
                if (dm) details.Description = dm[1].trim();

                let im = fullText.match(/iMDB Rating\s*:\s*([^\n]+)/i);
                if (im) details.IMDb = im[1].trim();

                // ── Download quality links ──────────────────────────
                let allLinks = Array.from(
                    document.querySelectorAll('a')
                );
                allLinks.forEach(a => {
                    let href = a.href;
                    let text = a.innerText.trim();

                    if (
                        href.includes('/server/') ||
                        href.includes('.html')
                    ) {
                        if (
                            text.toLowerCase().includes('.mkv') ||
                            text.toLowerCase().includes('.mp4') ||
                            /(480p|720p|1080p|2160p)/i.test(text)
                        ) {
                            let pt = a.parentElement
                                ? a.parentElement.innerText
                                : text;
                            let szMatch = pt.match(
                                /(\d+(?:\.\d+)?\s*(?:MB|GB))/i
                            );
                            let size = szMatch ? szMatch[1].trim() : '';

                            details.QualityLinks.push({
                                quality: text,
                                size: size,
                                url: href
                            });
                        }
                    }
                });

                return details;
            }"""
            )

            if not data:
                return None

            # Reshape for engine compatibility
            quality_links = data.pop("QualityLinks", [])
            data["raw_download_links"] = quality_links
            return data

        except Exception as e:
            print(f"   ⚠️ Extract error: {e}", flush=True)
            return None

    # ==================================================================
    # 3. BYPASS LOGIC — Server Selection Pages
    # ==================================================================
    async def bypass_links(self, context, browser, raw_links):
        """
        For FilmyZilla, each raw link points to a server selection page.
        Navigate there and extract the actual server download URLs.
        """

        async def _extract_servers(item):
            """Scrape a single FilmyZilla server page."""
            server_url = item["url"]
            quality = item["quality"]
            size = item["size"]
            servers = []
            page = await context.new_page()

            try:
                await page.goto(
                    server_url,
                    timeout=45000,
                    wait_until="domcontentloaded",
                )

                servers = await page.evaluate(
                    r"""() => {
                    let links = Array.from(
                        document.querySelectorAll('a')
                    );
                    let out = [];
                    links.forEach(a => {
                        let text = a.innerText.trim();
                        let href = a.href;
                        if (
                            text.toLowerCase().includes('server') ||
                            href.includes('/verified/')
                        ) {
                            let name = text
                                .replace(/start download/i, '')
                                .replace(/now/i, '')
                                .replace(/[-:]/g, '')
                                .trim();
                            if (!name) name = 'Server';
                            out.push({
                                server_name: name,
                                url: href
                            });
                        }
                    });
                    return out;
                }"""
                )
            except Exception as e:
                print(
                    f"   ⚠️ Server page error ({server_url}): {e}",
                    flush=True,
                )
            finally:
                await page.close()

            return {
                "quality": quality,
                "size": size,
                "direct_links": servers,
            }

        # Fire all server-page extractions concurrently
        tasks = [_extract_servers(item) for item in raw_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if r and r.get("direct_links"):
                valid.append(r)
        return valid
