# Clothing app — Content engine (crawler)

This is the **content-generation engine**: a TypeScript Stagehand + Gemini scraper (pg-boss queue) that fills the clothing app's catalog. Railway-hosted. Part of a 3-project system.

**Before re-scanning this folder, load context from the `clothing-sprint` skill:**
- `~/.claude/skills/clothing-sprint/reference/content-engine.md` — full map (pipeline, scripts, env, gotchas)
- `~/.claude/skills/clothing-sprint/reference/architecture.md` — how scraped data reaches the backend + app
- `~/.claude/skills/clothing-sprint/playbook.md` — **read before running a scrape/deploy** (proven commands / known failures)

Quick facts:
- Pipeline: discover → explore → crawl → upload (images → Cloudflare R2, rows → Supabase Postgres).
- Run: `npm start` (UI at localhost:3456); `npm run explore -- --url=…`; `npm run worker` (heavy uploads, can run on the M4).
- **Deploy = git push** → Railway auto-deploys. Writes to the SAME Supabase DB as the backend; Supabase is source of truth (local artifacts are transient).

**After fixing any failure, silently update the skill playbook** (`~/.claude/skills/clothing-sprint/playbook.md`).
