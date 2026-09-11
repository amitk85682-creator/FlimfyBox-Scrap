"""
=========================================================================
 sites/mkvcinemas.py — MKVCinemas Site Plugin
=========================================================================
"""

import asyncio
import re
import requests
from sites.base import BaseSitePlugin

class CloudflareBlockedError(Exception):
    pass

class SitePlugin(BaseSitePlugin):
    SITE_NAME = "MKVCinemas"
    TARGET_WEBSITE = "https://mkvcinemas.tl"
    WATCHDOG_LIMIT = 50

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )
    }

    # ==================================================================
    # 1. URL DISCOVERY
    # ==================================================================
    async def get_all_urls(self, context=None, watchdog_mode=False):
        """
        Fetch all movie URLs from the sitemap.
        """
        print(f"📥 Fetching {self.SITE_NAME} urls...", flush=True)
        urls = []
        try:
            sitemap_index_url = f"{self.TARGET_WEBSITE}/sitemap_index.xml"
            print(f"   Checking index: {sitemap_index_url}", flush=True)
            
            resp = requests.get(sitemap_index_url, headers=self.HEADERS, timeout=20)
            sitemap_urls = []
            
            if resp.status_code == 200 and "xml" in resp.headers.get("Content-Type", "").lower():
                locs = re.findall(r'<loc>(.*?)</loc>', resp.text)
                for loc in locs:
                    if 'post-sitemap' in loc:
                        sitemap_urls.append(loc)
                print(f"   Found {len(sitemap_urls)} post sitemaps in index.", flush=True)
            else:
                print(f"   ⚠️ Sitemap index unavailable (Status: {resp.status_code}, Type: {resp.headers.get('Content-Type')}). Falling back to post-sitemap1.xml", flush=True)
                sitemap_urls = [f"{self.TARGET_WEBSITE}/post-sitemap1.xml"]
                
            for s_url in sitemap_urls:
                print(f"   Fetching sitemap: {s_url}", flush=True)
                s_resp = requests.get(s_url, headers=self.HEADERS, timeout=20)
                if s_resp.status_code == 200 and "xml" in s_resp.headers.get("Content-Type", "").lower():
                    s_locs = re.findall(r'<loc>(.*?)</loc>', s_resp.text)
                    for loc in s_locs:
                        if '/category/' not in loc and '/page/' not in loc and '/tag/' not in loc:
                            if loc.strip('/') != self.TARGET_WEBSITE.strip('/'):
                                urls.append(loc)
                else:
                    print(f"   ❌ Failed to fetch {s_url} (Status: {s_resp.status_code}, Type: {s_resp.headers.get('Content-Type')})", flush=True)

            # Preserve ordering. RankMath usually has latest posts in the earliest sitemaps, but some versions reverse it.
            # We don't need to manually sort because main.py watchdog limit will grab the first N URLs from this list.
            print(f"✅ Discovered {len(urls)} URLs from sitemaps!", flush=True)

        except Exception as e:
            print(f"❌ Sitemap fetch error: {e}", flush=True)
            
        return urls

    # ==================================================================
    # 2. MOVIE DATA EXTRACTION
    # ==================================================================
    async def extract_movie_data(self, page):
        """
        Extract metadata + find quality links.
        """
        try:
            raw_h1 = await page.locator("h1").first.inner_text(timeout=10000)
            raw_h1 = re.sub(r"\s+", " ", raw_h1 or "").strip()
            
            page_title = await page.title()
            if "Just a moment" in page_title or "Attention Required" in page_title:
                raise CloudflareBlockedError("CLOUDFLARE_BLOCKED")
            
            poster = await page.evaluate("() => { let img = document.querySelector('img.wp-post-image'); return img ? img.src : ''; }")
            
            details = {
                "Raw_Title": raw_h1,
                "Poster": poster,
                "Type": "Movies",
                "download_page_url": ""
            }
            
            # Find filesdl.live aggregator link
            filesdl_master_url = await page.evaluate(r'''() => {
                let target = Array.from(document.querySelectorAll('a')).find(a => (a.href || "").includes('filesdl.live'));
                return target ? target.href : null;
            }''')
            
            if not filesdl_master_url:
                print("   ⚠️ filesdl aggregator link nahi mila.", flush=True)
                return None
                
            details["download_page_url"] = filesdl_master_url
            
            raw_links = []
            dl_page = await page.context.new_page()
            try:
                await dl_page.goto(filesdl_master_url, timeout=60000, wait_until="domcontentloaded")
                await dl_page.wait_for_timeout(3000)
                
                dl_page_title = await dl_page.title()
                if "Just a moment" in dl_page_title or "Attention Required" in dl_page_title:
                    raise CloudflareBlockedError("CLOUDFLARE_BLOCKED")
                
                raw_links = await dl_page.evaluate(r'''() => {
                    let results = [];
                    let buttons = Array.from(document.querySelectorAll('a')).filter(a => (a.innerText || "").toLowerCase().includes('hubcloud'));
                    
                    buttons.forEach(btn => {
                        let container = btn.closest('div.card') || btn.closest('div.shadow') || btn.parentElement.parentElement;
                        let textBlock = container ? container.innerText : "";
                        let match = textBlock.match(/(\d{3,4}P.*?DOWNLOAD.*?(MB|GB))/i);
                        let qualityStr = match ? match[1].replace(/DOWNLOAD/i, '').replace(/\s+/g, ' ').trim() : "Unknown Quality";
                        results.push({ quality: qualityStr, url: btn.href, size: '' });
                    });
                    return results;
                }''')
            except CloudflareBlockedError:
                raise
            except Exception as e:
                print(f"   ⚠️ DL Page error: {e}", flush=True)
            finally:
                await dl_page.close()
                
            details["raw_download_links"] = raw_links
            return details

        except CloudflareBlockedError:
            raise
        except Exception as e:
            print(f"   ⚠️ Extract error: {e}", flush=True)
            return None

    # ==================================================================
    # 3. BYPASS LOGIC
    # ==================================================================
    async def bypass_hubcloud_chain(self, context, hubdrive_url):
        page = await context.new_page()
        try:
            await page.goto(hubdrive_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(4000)
            
            page_title = await page.title()
            if "Just a moment" in page_title or "Attention Required" in page_title:
                raise CloudflareBlockedError("CLOUDFLARE_BLOCKED")
            
            hubcloud_url = await page.evaluate(r'''() => {
                let links = Array.from(document.querySelectorAll('a, button'));
                let target = links.find(a => (a.innerText || "").toLowerCase().includes('hubcloud server'));
                if (!target) return null;
                if (target.href) return target.href;
                let onclickMatch = (target.getAttribute('onclick') || "").match(/['"](https?:\/\/[^'"]+)['"]/);
                return onclickMatch ? onclickMatch[1] : null;
            }''')
            
            if not hubcloud_url: return None
            
            await page.goto(hubcloud_url, timeout=60000, wait_until="domcontentloaded")
            await page.wait_for_timeout(8000)
            
            gamerxyt_url = await page.evaluate(r'''() => {
                let links = Array.from(document.querySelectorAll('a, button'));
                let target = links.find(a => (a.innerText || "").toLowerCase().includes('generate') || (a.innerText || "").toLowerCase().includes('direct download'));
                if (!target) return null;
                if (target.href && target.href.includes('http')) return target.href;
                let onclickMatch = (target.getAttribute('onclick') || "").match(/['"](https?:\/\/[^'"]+)['"]/);
                return onclickMatch ? onclickMatch[1] : null;
            }''')
            
            if gamerxyt_url and 'http' in gamerxyt_url:
                await page.goto(gamerxyt_url, timeout=60000, wait_until="domcontentloaded")
            else:
                try:
                    await page.locator('text="Generate Direct Download Link"').click(timeout=10000)
                except:
                    pass
                
            await page.wait_for_timeout(8000)
            
            final_servers = await page.evaluate(r'''() => {
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
            return final_servers
        except CloudflareBlockedError:
            raise
        except Exception as e:
            print(f"   ⚠️ bypass_hubcloud_chain error: {e}", flush=True)
            return None
        finally:
            await page.close()

    async def bypass_links(self, context, browser, raw_links):
        async def _extract(item):
            page = await context.new_page()
            servers = []
            size = item.get("size", "")
            try:
                await page.goto(item["url"], timeout=60000, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                
                page_title = await page.title()
                if "Just a moment" in page_title or "Attention Required" in page_title:
                    raise CloudflareBlockedError("CLOUDFLARE_BLOCKED")
                
                extracted = await page.evaluate(r'''() => {
                    let docText = document.body.innerText;
                    let fNameMatch = docText.match(/(.*?\.(mkv|mp4|zip|rar|avi))/i);
                    let fSizeMatch = docText.match(/Size:\s*([\d\.]+\s*(MB|GB))/i);
                    
                    let pd = null, hc = null;
                    Array.from(document.querySelectorAll('button, a')).forEach(el => {
                        let text = (el.innerText || "").toLowerCase();
                        let onclickMatch = (el.getAttribute('onclick') || "").match(/['"](https?:\/\/[^'"]+)['"]/);
                        let finalUrl = onclickMatch ? onclickMatch[1] : (el.getAttribute('href') || "");
                        
                        if (text.includes('pixeldrain') && finalUrl) pd = finalUrl;
                        if (text.includes('hubcloud') && finalUrl) hc = finalUrl;
                    });
                    return { 
                        filename: fNameMatch ? fNameMatch[1].trim() : "Unknown", 
                        size: fSizeMatch ? fSizeMatch[1] : "Unknown", 
                        pixeldrain: pd, 
                        hubcloud: hc 
                    };
                }''')
                
                if extracted.get("size") and extracted.get("size") != "Unknown":
                    size = extracted["size"]
                
                if extracted.get("pixeldrain"):
                    servers.append({"server_name": "PixelDrain", "url": extracted["pixeldrain"]})
                    
                hc_url = extracted.get("hubcloud")
                if hc_url:
                    if "worrkers.dev" in hc_url:
                        servers.append({"server_name": "Direct Worker", "url": hc_url})
                    else:
                        hc_servers = await self.bypass_hubcloud_chain(context, hc_url)
                        if hc_servers:
                            servers.extend(hc_servers)
                            
            except CloudflareBlockedError:
                raise
            except Exception as e:
                print(f"   ⚠️ Server bypass error: {e}", flush=True)
            finally:
                await page.close()
                
            return {
                "quality": item["quality"],
                "size": size,
                "direct_links": servers
            }
            
        tasks = [_extract(item) for item in raw_links]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        valid = []
        for r in results:
            if isinstance(r, CloudflareBlockedError):
                raise r
            if isinstance(r, Exception): continue
            if r and r.get("direct_links"): valid.append(r)
        return valid
