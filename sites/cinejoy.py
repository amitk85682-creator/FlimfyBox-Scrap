"""
=========================================================================
 sites/cinejoy.py — CineJoy Site Plugin
=========================================================================
 Scraping rules for CineJoy (cinejoy.to).

 URL Discovery : Page crawling on homepage to extract /movie/ and /series/
 Data Extract  : Navigates to movie/series page, clicks download modal,
                 extracts download entries for movies or iterates season/
                 episode dropdowns for series.
 Bypass Logic  : Directly uses raw URLs (no intermediate servers needed),
                 parsing raw text into quality, size, and server name.
=========================================================================
"""

import asyncio
from sites.base import BaseSitePlugin

class SitePlugin(BaseSitePlugin):
    SITE_NAME = "CineJoy"
    TARGET_WEBSITE = "https://cinejoy.to"
    WATCHDOG_LIMIT = 50

    # ==================================================================
    # 1. URL DISCOVERY
    # ==================================================================
    async def get_all_urls(self, context=None, watchdog_mode=False):
        """
        Open homepage, scroll, and extract /movie/ and /series/ links,
        excluding /category/ and /genre/ links. Deduplicates links.
        """
        if context is None:
            print("❌ CineJoy requires a browser context for URL discovery.", flush=True)
            return []

        print(f"📥 Fetching {self.SITE_NAME} homepage...", flush=True)
        try:
            page = await context.new_page()
            
            # Block ads and unnecessary resources
            async def block_ads(route):
                if route.request.resource_type in ["image", "media", "font"]:
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_ads)

            await page.goto(self.TARGET_WEBSITE, wait_until="networkidle", timeout=60000)
            
            # Lazy load
            await page.evaluate("window.scrollBy(0, 1500)")
            await page.wait_for_timeout(3000)

            # Extract URLs
            movies_on_page = await page.evaluate('''() => {
                let links = Array.from(document.querySelectorAll('a'));
                let uniqueMovies = new Set();
                links.forEach(a => {
                    let href = a.href || '';
                    if ((href.includes('/series/') || href.includes('/movie/')) && 
                        !href.includes('/category/') && 
                        !href.includes('/genre/') && 
                        href.split('/').pop().length > 2) {
                        uniqueMovies.add(href);
                    }
                });
                return Array.from(uniqueMovies);
            }''')

            await page.close()
            
            if watchdog_mode:
                movies_on_page = movies_on_page[:self.WATCHDOG_LIMIT]
                
            print(f"✅ Discovered {len(movies_on_page)} URLs!", flush=True)
            return movies_on_page

        except Exception as e:
            print(f"❌ CineJoy discovery error: {e}", flush=True)
            return []

    # ==================================================================
    # 2. MOVIE DATA EXTRACTION
    # ==================================================================
    async def extract_movie_data(self, page):
        """
        Click the Download button to open the modal, then extract links
        using the tested JavaScript logic.
        """
        url = page.url
        is_movie = '/movie/' in url
        page_type = 'Movies' if is_movie else 'Web Series'

        try:
            # The page is already at the URL (main.py does the goto)
            # but we need to wait for idle since we need to click the modal
            # and main.py just waits for domcontentloaded.
            # We'll do a small wait for safety.
            await page.wait_for_timeout(3000)

            print("🎯 Locating the VISIBLE Download button...", flush=True)
            dl_btn = page.locator('button[aria-label="Download"]:visible').first

            modal_opened = False
            for i in range(5):
                try:
                    await dl_btn.click(force=True)
                    await page.wait_for_timeout(2000)

                    html_content = await page.content()
                    if "Downloads for" in html_content or "Season 1" in html_content:
                        modal_opened = True
                        break
                except Exception:
                    pass

            if not modal_opened:
                raise Exception("Download modal did not open")

            await page.wait_for_timeout(1000)
            
            # Run the tested JS logic
            raw_cinejoy_data = await page.evaluate('''async (isMovie) => {
                const sleep = ms => new Promise(r => setTimeout(r, ms));
                const results = [];

                const modal = Array.from(document.querySelectorAll('div')).find(d => d.innerText && d.innerText.includes('Downloads for'));
                if(!modal) return {error: "Modal container not found"};

                if(isMovie) {
                    await sleep(2000);
                    const epLinks = [];
                    const dlBtns = Array.from(modal.querySelectorAll('button, a, [role="button"]'))
                                        .filter(b => b.innerText && b.innerText.includes('Download'));

                    dlBtns.forEach(btn => {
                        let el = btn;
                        let rawInfoText = "Unknown Quality";
                        for(let i=0; i<4; i++) {
                            el = el.parentElement;
                            if(el && el.innerText && el.innerText.length > 10) {
                                rawInfoText = el.innerText.replace(/Download/ig, '').replace(/\\n/g, ' | ').trim();
                                break;
                            }
                        }
                        if (btn.href) {
                            epLinks.push({ raw_info: rawInfoText, url: btn.href });
                        }
                    });

                    if(epLinks.length > 0) {
                        results.push({ season: "Movie", episodes: [{ episode: "Full Movie", links: epLinks }] });
                    }
                    return results;
                }

                const triggers = Array.from(modal.querySelectorAll('[role="combobox"], [aria-expanded], [aria-haspopup="listbox"], button'));
                let seasonBtn = triggers.find(b => b.innerText && b.innerText.match(/Season|Specials/i)) || triggers[0];
                let episodeBtn = triggers.find(b => b.innerText && b.innerText.match(/Episode/i)) || triggers[1];

                if(!seasonBtn || !episodeBtn) return {error: "Dropdown buttons missing"};

                seasonBtn.click();
                await sleep(1000); 
                const sBox = document.querySelector('[role="listbox"], [role="menu"], [data-radix-popper-content-wrapper], .absolute.z-50');
                const sOptionsCount = sBox ? sBox.querySelectorAll('[role="option"], li, button').length : 1;
                document.body.click(); 
                await sleep(500);

                const limitSeasons = sOptionsCount; 

                for(let s = 0; s < limitSeasons; s++) {
                    seasonBtn.click(); 
                    await sleep(800);
                    
                    const currentSBox = document.querySelector('[role="listbox"], [role="menu"], [data-radix-popper-content-wrapper], .absolute.z-50');
                    if(!currentSBox) continue;
                    
                    const currentSOpts = Array.from(currentSBox.querySelectorAll('[role="option"], li, button'));
                    if(!currentSOpts[s]) continue;
                    
                    const seasonName = currentSOpts[s].innerText.trim();
                    
                    currentSOpts[s].click(); 
                    await sleep(2500); 

                    episodeBtn.click();
                    await sleep(800);
                    
                    const eBox = document.querySelector('[role="listbox"], [role="menu"], [data-radix-popper-content-wrapper], .absolute.z-50');
                    if(!eBox) { document.body.click(); continue; }
                    
                    const epOptionsCount = eBox.querySelectorAll('[role="option"], li, button').length;
                    document.body.click(); 
                    await sleep(500);

                    const seasonData = { season: seasonName, episodes: [] };
                    const limitEps = epOptionsCount; 

                    for(let e = 0; e < limitEps; e++) {
                        episodeBtn.click(); 
                        await sleep(800);
                        
                        const currentEBox = document.querySelector('[role="listbox"], [role="menu"], [data-radix-popper-content-wrapper], .absolute.z-50');
                        if(!currentEBox) continue;
                        
                        const currentEOpts = Array.from(currentEBox.querySelectorAll('[role="option"], li, button'));
                        if(!currentEOpts[e]) continue;
                        
                        const epName = currentEOpts[e].innerText.trim();
                        
                        currentEOpts[e].click(); 
                        await sleep(2500); 

                        const epLinks = [];
                        const dlBtns = Array.from(modal.querySelectorAll('button, a, [role="button"]'))
                                           .filter(b => b.innerText && b.innerText.includes('Download') && b !== seasonBtn && b !== episodeBtn);

                        dlBtns.forEach(btn => {
                            let el = btn;
                            let rawInfoText = "Unknown Quality";
                            for(let i=0; i<4; i++) {
                                el = el.parentElement;
                                if(el && el.innerText && el.innerText.length > 10) {
                                    rawInfoText = el.innerText.replace(/Download/ig, '').replace(/\\n/g, ' | ').trim();
                                    break;
                                }
                            }
                            if (btn.href) {
                                epLinks.push({ raw_info: rawInfoText, url: btn.href });
                            }
                        });

                        if (epLinks.length > 0) {
                            seasonData.episodes.push({ episode: epName, links: epLinks });
                        }
                    }
                    if (seasonData.episodes.length > 0) {
                        results.push(seasonData);
                    }
                }
                return results;
            }''', is_movie)

            if not isinstance(raw_cinejoy_data, list):
                raise Exception(f"Invalid data returned from evaluation: {raw_cinejoy_data}")

            # Pack raw entries for the bypass phase
            # CineJoy doesn't have an intermediate server page, so we just pass the raw data
            raw_links = []
            for s_data in raw_cinejoy_data:
                s_name = s_data.get('season', '')
                for e_data in s_data.get('episodes', []):
                    e_name = e_data.get('episode', '')
                    for link in e_data.get('links', []):
                        raw_links.append({
                            "season": s_name,
                            "episode": e_name,
                            "raw_info": link['raw_info'],
                            "url": link['url']
                        })

            # The exact clean title parsing is deferred to main.py's fix_movie_details via URL slug
            return {
                "Raw_Title": "UNKNOWN_TITLE",
                "Type": page_type,
                "raw_download_links": raw_links
            }

        except Exception as e:
            print(f"   ⚠️ Extract error: {e}", flush=True)
            return None

    # ==================================================================
    # 3. DOWNLOAD LINK BYPASS
    # ==================================================================
    async def bypass_links(self, context, browser, raw_links):
        """
        No intermediate navigation needed. Parse raw_info and format it.
        """
        valid = []
        for item in raw_links:
            try:
                s_name = item.get("season", "")
                e_name = item.get("episode", "")
                raw_info = item.get("raw_info", "")
                url = item.get("url", "")

                parsed_info = self._parse_cinejoy_raw_info(raw_info, s_name, e_name)
                
                # If Movie, leave extra_info blank. If Series, use parsed SxxEyy
                # In CineJoy.txt, it relied on 'is_movie', we can infer by checking if season="Movie"
                is_movie = (s_name == "Movie")
                extra_tag = "" if is_movie else parsed_info["extra_info"]
                
                # Add it to the final array
                valid.append({
                    "quality": parsed_info["quality"],
                    "size": parsed_info["size"],
                    "extra_info": extra_tag,
                    "direct_links": [
                        {
                            "server_name": parsed_info["server_name"],
                            "url": url
                        }
                    ]
                })
            except Exception as e:
                print(f"   ⚠️ Bypass parsing error: {e}", flush=True)
        
        return valid

    def _parse_cinejoy_raw_info(self, raw_info, season_text, episode_text):
        """Internal helper to parse CineJoy's pipe-delimited button text."""
        parts = [p.strip() for p in raw_info.split('|') if p.strip()]
        
        server_name = parts[0] if len(parts) > 0 else "Unknown Server"
        resolution = parts[1] if len(parts) > 1 else "HD"
        size = parts[2] if len(parts) > 2 else ""
        extra_tags = parts[3] if len(parts) > 3 else ""
        
        s_num = ''.join(filter(str.isdigit, season_text))
        e_num = ''.join(filter(str.isdigit, episode_text))
        
        s_str = f"S{int(s_num):02d}" if s_num else "S01"
        if "special" in season_text.lower():
            s_str = "S00"
            
        ep_str = f"{s_str}E{int(e_num):02d}" if e_num else ""
        quality_tag = f"{resolution} {extra_tags}".strip()
        
        return {
            "quality": quality_tag,
            "size": size,
            "server_name": server_name,
            "extra_info": ep_str
        }
