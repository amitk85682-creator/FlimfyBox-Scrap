"""
=========================================================================
 sites/filmy4web.py — Filmy4web Site Plugin
=========================================================================
"""

import asyncio
import re
import requests
import xml.etree.ElementTree as ET
from sites.base import BaseSitePlugin


class SitePlugin(BaseSitePlugin):
    SITE_NAME = "Filmy4web"
    TARGET_WEBSITE = "https://tikfilm.org"
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
        Fetch all movie URLs from the standard WordPress XML sitemap.
        """
        print(f"📥 Fetching {self.SITE_NAME} sitemap...", flush=True)
        movie_links = []
        urls_with_meta = []

        try:
            # Try the post sitemap index first
            sitemap_url = f"{self.TARGET_WEBSITE}/sitemap_index.xml"
            resp = requests.get(sitemap_url, headers=self.HEADERS, timeout=20)

            if resp.status_code != 200 or "<html" in resp.text.lower():
                sitemap_url = f"{self.TARGET_WEBSITE}/post-sitemap.xml"
                resp = requests.get(sitemap_url, headers=self.HEADERS, timeout=20)
                
            if resp.status_code != 200 or "<html" in resp.text.lower():
                # Fallback to general sitemap
                sitemap_url = f"{self.TARGET_WEBSITE}/sitemap.xml"
                resp = requests.get(sitemap_url, headers=self.HEADERS, timeout=20)

            if resp.status_code != 200 or "<html" in resp.text.lower():
                print("❌ Could not fetch sitemap XML.", flush=True)
                return []

            root = ET.fromstring(resp.content)
            ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

            sitemaps = root.findall("ns:sitemap", ns)

            if sitemaps:
                # ── Sitemap Index: fetch each sub-sitemap ────────────
                for sm in sitemaps:
                    loc_elem = sm.find("ns:loc", ns)
                    if loc_elem is None or not loc_elem.text:
                        continue
                    loc = loc_elem.text

                    if not any(kw in loc for kw in ["post", "movie"]):
                        continue

                    try:
                        sub_resp = requests.get(loc, headers=self.HEADERS, timeout=15)
                        if sub_resp.status_code == 200 and "<html" not in sub_resp.text.lower():
                            sub_root = ET.fromstring(sub_resp.content)
                            for url_elem in sub_root.findall("ns:url", ns):
                                url_loc = url_elem.find("ns:loc", ns)
                                if url_loc is not None and url_loc.text:
                                    lm_elem = url_elem.find("ns:lastmod", ns)
                                    lm = lm_elem.text if lm_elem is not None else ""
                                    urls_with_meta.append((lm, url_loc.text))
                    except Exception as e:
                        print(f"   ⚠️ Sub-sitemap error ({loc}): {e}", flush=True)
            else:
                # ── Flat sitemap: extract <url> entries directly ─────
                for url_elem in root.findall("ns:url", ns):
                    url_loc = url_elem.find("ns:loc", ns)
                    if url_loc is not None and url_loc.text:
                        lm_elem = url_elem.find("ns:lastmod", ns)
                        lm = lm_elem.text if lm_elem is not None else ""
                        urls_with_meta.append((lm, url_loc.text))

            # Sort newest first
            urls_with_meta.sort(reverse=True)
            movie_links = [url for _, url in urls_with_meta]
            print(f"✅ Discovered {len(movie_links)} URLs from sitemap!", flush=True)

        except Exception as e:
            print(f"❌ Sitemap fetch error: {e}", flush=True)

        return movie_links

    # ==================================================================
    # 2. MOVIE DATA EXTRACTION
    # ==================================================================
    async def extract_movie_data(self, page):
        """
        Extract metadata + find quality links.
        """
        try:
            data = await page.evaluate(r"""() => {
                let details = {
                    Raw_Title: '', Stars: 'N/A', Genre: 'N/A',
                    Language: 'N/A', Description: 'N/A', Poster: '',
                    IMDb: 'N/A', Director: 'N/A', Creator: 'N/A',
                    Type: 'Movies', is_adult_bypass: false,
                    download_page_url: ''
                };
                
                let fullText = document.body.innerText;
                
                // Adult Bypass Flag
                let breadcrumbs = document.querySelector('.breadcrumb') ? document.querySelector('.breadcrumb').innerText : '';
                if (/Hot Web Series|18\+/i.test(breadcrumbs) || /Hot Web Series|18\+/i.test(fullText)) {
                    details.is_adult_bypass = true;
                }
                
                // Title
                let titleMatch = fullText.match(/Movie Name\s*[:-]\s*([^\n]+)/i);
                let raw_title = titleMatch ? titleMatch[1].trim() : (document.querySelector('title') ? document.querySelector('title').innerText : '');
                
                // Title Cleaning: strip site names, years, tags
                raw_title = raw_title.replace(/Filmy4web|Tikfilm|2024|2025|Hindi|Dubbed|HQ|WebDl/ig, '').trim();
                // Remove duplicate adjacent words
                raw_title = raw_title.replace(/\b(\w+)\s+\1\b/ig, '$1').trim();
                details.Raw_Title = raw_title;
                
                // Poster: bypass logo, icon, filmy4web.jpg
                let imgs = Array.from(document.querySelectorAll('img'));
                let valid = imgs.find(img => {
                    let src = img.src.toLowerCase();
                    return !src.includes('logo') && !src.includes('icon') && !src.includes('filmy4web.jpg') && img.naturalWidth > 100;
                });
                if (valid) details.Poster = valid.src;
                
                // Metadata
                let am = fullText.match(/Artists?\s*[:-]\s*([^\n]+)/i);
                if (am) details.Stars = am[1].trim();
                
                let lm = fullText.match(/Language\s*[:-]\s*([^\n]+)/i);
                if (lm) details.Language = lm[1].trim();
                
                let gm = fullText.match(/Genres?\s*[:-]\s*([^\n]+)/i);
                if (gm) details.Genre = gm[1].trim();
                
                let dm = fullText.match(/(?:Description|Storyline)\s*[:-]\s*([^\n]+)/i);
                if (dm) details.Description = dm[1].trim();
                
                // Quality Links: Direct append /download (robust fallback)
                let currentUrl = window.location.href;
                details.download_page_url = currentUrl.endsWith('/') ? currentUrl + 'download' : currentUrl + '/download';
                
                return details;
            }""")
            
            if not data:
                return None
                
            raw_links = []
            dl_url = data.pop('download_page_url', '')
            if dl_url:
                dl_page = await page.context.new_page()
                try:
                    await dl_page.goto(dl_url, timeout=60000, wait_until="domcontentloaded")
                    raw_links = await dl_page.evaluate(r"""() => {
                        let qLinks = [];
                        let as = Array.from(document.querySelectorAll('a'));
                        as.forEach(a => {
                            let text = a.innerText.trim();
                            if (/Server (1|2|3)/i.test(text)) {
                                qLinks.push({
                                    quality: text,
                                    size: '',
                                    url: a.href
                                });
                            }
                        });
                        return qLinks;
                    }""")
                except Exception as e:
                    print(f"   ⚠️ DL Page error: {e}", flush=True)
                finally:
                    await dl_page.close()
                    
            data["raw_download_links"] = raw_links
            return data

        except Exception as e:
            print(f"   ⚠️ Extract error: {e}", flush=True)
            return None

    # ==================================================================
    # 3. BYPASS LOGIC — GDFlix Mirrors
    # ==================================================================
    async def bypass_links(self, context, browser, raw_links):
        """
        Navigate to server link, parse GDFlix page for actual mirror buttons without waiting for timers.
        """
        async def _extract(item):
            page = await context.new_page()
            servers = []
            try:
                await page.goto(item["url"], timeout=60000, wait_until="domcontentloaded")
                servers = await page.evaluate(r"""() => {
                    let out = [];
                    let as = Array.from(document.querySelectorAll('a'));
                    as.forEach(a => {
                        let text = a.innerText.trim().toUpperCase();
                        let href = a.href;
                        
                        if (href && (text.includes("FSL V") || text.includes("INSTANT DL") || text.includes("FAST CLOUD"))) {
                            out.push({
                                server_name: text,
                                url: href
                            });
                        }
                    });
                    return out;
                }""")
            except Exception as e:
                print(f"   ⚠️ Server bypass error: {e}", flush=True)
            finally:
                await page.close()
                
            return {
                "quality": item["quality"],
                "size": item["size"],
                "direct_links": servers
            }
            
        tasks = [_extract(item) for item in raw_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid = []
        for r in results:
            if isinstance(r, Exception): continue
            if r and r.get("direct_links"): valid.append(r)
        return valid
