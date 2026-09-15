# Production Status

## 1. Current Architecture
- **Infrastructure**: GitHub Actions (Ubuntu-latest runners, 4 vCPU, 16GB RAM)
- **Database**: Supabase PostgreSQL connection pooler (Transaction mode)
- **Execution Model**:
  - **Watchdog (Cron)**: Periodically discovers new URLs from sitemaps and adds them to `crawl_jobs` queue.
  - **Worker (Cron/Manual)**: Drains the `crawl_jobs` queue utilizing Playwright cluster, parsing DOMs, and pushing to `scraped_urls`.
- **State Management**: Asynchronous atomic claim (`FOR UPDATE SKIP LOCKED`) with HeartbeatManager lease extension and stale-job sweeper.

## 2. Current Production Settings
- **FilmyZilla**: `max_active=2`
- **HDHub4u**: `max_active=2`
- **MKVCinemas**: Disabled from scheduled processing (due to Cloudflare).
- **Database Pool Size**: `DB_POOL_SIZE=4` (per worker process)

## 3. Completed Phases
- **Phase A**: COMPLETE (Initial robust parser refactoring)
- **Phase B**: COMPLETE (Queue infrastructure, leases, skipping locked rows, Heartbeat Manager, robust retry logic)
- **Phase C Step 1**: COMPLETE (`queue_health.py` metrics, status/velocity monitoring)
- **Phase C Step 2**:
  - C2.1 Velocity Metrics: COMPLETE
  - C2.2 Cleanup Activation: COMPLETE (Stale job sweeper logic)
  - C2.3 Per-Site Config: COMPLETE (Replaced global concurrency with `SITE_CONFIG`)
  - C2.4 Scaling: OPTIONAL / DEMAND-DRIVEN (Tested up to `max_active=3` successfully, reverted to baseline `max_active=2` pending real-world demand).

## 4. Known External Limitations
**MKVCinemas Cloudflare**:
MKVCinemas relies on a Turnstile/Cloudflare human-verification challenge. This explicitly blocks headless Playwright environments resulting in HTTP 403 blocks. 
- *Policy*: We do not bypass, evade, solve, or weaken Cloudflare. The site is safely skipped.
- *Status*: Disabled from automated schedules.

## 5. Current Monitoring Method
- Run `python queue_health.py` locally or in GitHub Actions.
- Monitors overall status, velocity, backlog growth, lease expiries, and per-site throughput.

## 6. Conditions Justifying Future MAX_ACTIVE=3 Testing
The `max_active=3` worker configuration was successfully proven in GitHub Actions during a controlled test, showing high stability and fast execution. Promote to `max_active=3` (and `DB_POOL_SIZE=5`) ONLY IF:
- Natural backlog consistently grows over 24h intervals (e.g. `backlog_growth_24h > 100`).
- Processing times lag significantly behind new releases.
- The `queue_health.py` report shows `estimated_drain_hours` expanding uncontrollably under the baseline configuration.

## 7. Conditions Justifying VPS Migration
The current GitHub Actions pipeline handles the load flawlessly and provides excellent visibility. Migrate to a persistent VPS ONLY IF:
- Job durations increase drastically (e.g., due to required long-running Playwright waits) causing GitHub Actions 6-hour timeouts.
- Concurrency demands exceed `max_active=5` per worker, pushing the limits of the public runner's CPU.
- Real-time (sub-minute) extraction is demanded, making Cron job startup latency unacceptable.

## 8. Safe Procedure for New-Site Onboarding
1. Create parser in `sites/newsite.py`.
2. Add site default config in `main.py` -> `SITE_CONFIG` with `max_active: 1`.
3. Do NOT increase global `DB_POOL_SIZE`. Keep it at 4 to ensure system stability.
4. Run a manual Matrix-Discovery run via GitHub Actions for the new site to populate the queue.
5. Trigger a worker test with `MAX_WORKER_JOBS=10` and verify DB insertion.
6. Once stable, manually increase `max_active` to 2 and monitor via `queue_health.py`.
