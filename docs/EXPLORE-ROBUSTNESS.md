# Config identification: robustness plan

Status: the core of this plan is **implemented** (2026-06-10). Each section notes
what shipped and what is deliberately deferred.

## The shape of the problem

Config identification ("explore") must turn an arbitrary storefront URL into a
machine-readable crawl config. The failure modes observed in practice:

- Paying browser+LLM costs for sites whose structure is a known platform standard.
- LLM-emitted configs that don't validate, or validate but match the wrong URLs
  (e.g. `/journal/products/...` blog posts passing as product pages).
- Fragile CSS selectors that break the *crawl* months after a correct *explore*.

The design principle that addresses all three: **AI interprets, code verifies.**
Anything deterministic is done in code; the model is only asked to interpret
evidence we fetched ourselves, and nothing it says is trusted until code has
re-checked it against the live site.

## 1. More config types + a robust non-AI crawl

### The explore ladder (implemented in `exploreRetailer`, cheapest first)

| Rung | Detector | Cost | Covers |
|---|---|---|---|
| 1 | Shopify `/products.json` (`shopifyExplore.ts`) | $0, ~5s | Most independent brands today |
| 2 | WooCommerce Store API `/wp-json/wc/store/v1/products` (`platformExplore.ts`) | $0 | WordPress shops |
| 3 | Universal product sitemap (`platformExplore.ts`) | $0 | Squarespace, BigCommerce, Magento, SFCC, custom — anything with SEO-grade sitemaps |
| 4 | Evidence explore — one Gemini call (`evidenceExplore.ts`) | ~$0.003 | Custom storefronts with unusual URL schemes |
| 5 | Stagehand browser+LLM loop (legacy `explore.ts`) | $0.10–$1+ | JS-rendered SPAs, bot-hostile sites |

Adding a future platform (BigCommerce native API, Centra, Shopware…) = one new
`try<Platform>Explore()` in `platformExplore.ts` + one line in the ladder. The
universal sitemap rung already absorbs most of them in the meantime.

Key safeguards in rung 3 (these are what make non-AI identification *trustworthy*):
- Editorial-path exclusion (`/journal/`, `/blogs/`, …) before pattern inference,
  and baked into the emitted regex as a negative lookahead.
- A pattern must match ≥8 sitemap URLs to be considered.
- Three sampled URLs must show product JSON-LD or OG tags, or the rung refuses
  and passes the site down the ladder. (Verified live: asket.com is correctly
  refused here — its `/products/` sitemap entries are blog posts — and is then
  solved by rung 4.)

### Crawl-side robustness (implemented in `crawl.ts`)

- **Sitemap**: gzip payloads (magic-byte sniffing), nested indexes to depth 3,
  product-named sub-sitemaps fetched first, 60-sub-sitemap cap, CDATA/entity
  decoding.
- **Category pagination**: honors `rel=next` (handles `/page/2/`-style paths),
  falls back to scanning *every* anchor when the configured selector matches
  nothing (selector drift no longer kills a config — the product URL regex is
  the real filter), stops after two consecutive pages with no new URLs.
- **API**: `pageSizeParam` (Shopify `limit` vs Woo `per_page`), `startPage`,
  explicit `itemsPath` dot-path with auto-detection fallback.
- **Global**: `CRAWL_MAX_URLS` runaway guard (default 50k), shared in-page link
  collector for both browser strategies.

Deferred deliberately: cursor-based API pagination (no current config needs it);
"load more" button clicking for infinite scroll (Stagehand path handles those).

## 2. AI cost-efficiency and model choice

**Model: `gemini-3-flash-preview` (env `GEMINI_EXPLORE_MODEL`).** Rationale:

- The task after evidence collection is *structured interpretation of ~20–40k
  characters of URLs*, not agentic browsing — flash-class models handle it
  reliably once the output is schema-constrained, at roughly 10–40× cheaper
  than pro-class models.
- Measured: asket.com config = ~$0.003 (one call, verified correct). The legacy
  Stagehand loop for the same site is tens of calls plus browser minutes.
- The key already exists (`GOOGLE_GENERATIVE_AI_API_KEY`) and pricing flows
  through the same ledger (`usageLedger` → dashboard cost metrics).
- Escalation knob: if a site's evidence call fails twice, the ladder falls to
  Stagehand anyway, so a stronger model would only buy marginal wins; revisit
  only if rung-5 volume becomes a real cost line.

**The bigger cost lever than model choice is call architecture**: one call over
pre-fetched evidence instead of a multi-turn browser loop. That is rungs 1–4's
entire design — most sites never reach the model at all, and those that do get
exactly one (occasionally two) calls.

## 3. Prompt + context optimization (always-valid, accurate output)

Implemented in `evidenceExplore.ts`:

- **Evidence pack, not raw HTML**: robots.txt sitemap list; 60 URLs sampled
  *across* the sitemap (every Nth, so multiple URL shapes appear); homepage
  internal-link inventory (150); one listing page's link inventory + whether it
  declares `rel=next`; a JS-rendered heuristic flag. URLs-only context keeps
  tokens low (~10–20k) and removes the model's main hallucination surface.
- **Strict output contract**: `responseMimeType: application/json` +
  `responseJsonSchema` (flat object with optional per-method blocks — Gemini
  handles flat schemas far more reliably than `oneOf` unions). `maxOutputTokens`
  8192 because Gemini 3 thinking tokens count against the budget (2048 caused
  mid-string truncation in testing).
- **Grounding rules in the prompt**: method preference order; "regex must match
  the evidence URLs"; "sampleProductUrls must be copied VERBATIM from the
  evidence"; explicit permission to answer `method: none`.
- **Verify, then retry with the reason**: code re-checks every answer — regex
  compiles, matches the model's own samples, matches sitemap evidence; sample
  pages are fetched and must show JSON-LD/OG. A rejected answer triggers ONE
  retry whose prompt includes the precise rejection reason ("your pattern
  matches zero sitemap URLs"), which converts most near-misses. Invalid JSON
  also retries with feedback instead of aborting.
- **Quality fields are never model-authored**: `dataQuality`/`productPage`
  blocks are computed from the fetched sample pages, and the final object must
  pass the zod `ConfigSchema` before it's written.

Future option (deferred): feed the two highest-value Stagehand failures per week
back into the evidence prompt as few-shot examples. Not needed until there's
volume at rung 5.

## Operational notes

- Smoke test any site without spending AI: `npx tsx scripts/test-explore-ladder.ts <url>`
  (add `--ai` to allow the one evidence call).
- `generatedBy` on each config records which rung produced it — visible in the
  dashboard's Configure tab via the method column, and useful for auditing
  which rungs earn their keep.
