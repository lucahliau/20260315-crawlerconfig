import { Router, type Request, type Response } from "express";
import { getPool } from "../pipelineStore.js";

/**
 * Read-only product-analytics for the dashboard's Analytics tab.
 *
 * The iOS app writes append-only rows to "AnalyticsEvent" in the shared
 * Supabase DB (sessions, screen views, item views, searches). This computes the
 * four things the team cares about — engagement/retention, the activation
 * funnel, feature usage, and content/brand performance — off-band here so the
 * heavy GROUP BY / cohort queries never touch the app-serving backend.
 *
 * Design note: most of the funnel and all content/brand performance come from
 * the existing domain tables (User / Swipe / Collection / ClothingItem), NOT
 * from analytics events — that's why the iOS client only emits the few events
 * the DB can't already reconstruct.
 */

type Range = "7d" | "30d" | "all";

function sinceFor(range: Range): Date {
  if (range === "all") return new Date(0);
  const days = range === "7d" ? 7 : 30;
  return new Date(Date.now() - days * 24 * 60 * 60 * 1000);
}

export function createAnalyticsRouter(): Router {
  const router = Router();

  router.get("/api/analytics/summary", async (req: Request, res: Response) => {
    const pg = getPool();
    if (!pg) {
      return res.status(503).json({ error: "DATABASE_URL not configured." });
    }

    const rawRange = String(req.query.range ?? "30d");
    const range: Range = rawRange === "7d" || rawRange === "all" ? rawRange : "30d";
    const since = sinceFor(range);

    try {
      const engagementQ = pg.query(
        `SELECT
           (SELECT count(DISTINCT "userId") FROM "AnalyticsEvent"
              WHERE "userId" IS NOT NULL AND "createdAt" >= now() - interval '1 day')::int   AS dau,
           (SELECT count(DISTINCT "userId") FROM "AnalyticsEvent"
              WHERE "userId" IS NOT NULL AND "createdAt" >= now() - interval '7 days')::int  AS wau,
           (SELECT count(DISTINCT "userId") FROM "AnalyticsEvent"
              WHERE "userId" IS NOT NULL AND "createdAt" >= now() - interval '30 days')::int AS mau,
           (SELECT count(DISTINCT "sessionId") FROM "AnalyticsEvent" WHERE "createdAt" >= $1)::int AS sessions,
           (SELECT count(*) FROM "AnalyticsEvent" WHERE "createdAt" >= $1)::int AS events,
           (SELECT count(DISTINCT "sessionId") FROM "AnalyticsEvent"
              WHERE "createdAt" >= $1 AND "userId" IS NULL)::int AS anon_sessions,
           (SELECT count(DISTINCT "sessionId") FROM "AnalyticsEvent"
              WHERE "createdAt" >= $1 AND "userId" IS NOT NULL)::int AS identified_sessions`,
        [since],
      );

      const sessionLenQ = pg.query(
        `SELECT coalesce(avg(d), 0)::int AS avg_ms,
                coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY d), 0)::int AS median_ms
           FROM (
             SELECT (metadata->>'durationMs')::numeric AS d
               FROM "AnalyticsEvent"
              WHERE "eventName" = 'session_end' AND "createdAt" >= $1
                AND metadata->>'durationMs' ~ '^[0-9]+$'
           ) t`,
        [since],
      );

      // Cohort = each identified user's first-seen day. d1 = active the very
      // next day; d7 = any activity within the 7 days after first-seen. Only
      // users old enough to have had the chance count as "eligible".
      const retentionQ = pg.query(
        `WITH fs AS (
           SELECT "userId", min("createdAt") AS first_at
             FROM "AnalyticsEvent" WHERE "userId" IS NOT NULL GROUP BY "userId"
         ),
         act AS (
           SELECT DISTINCT "userId", date_trunc('day', "createdAt") AS day
             FROM "AnalyticsEvent" WHERE "userId" IS NOT NULL
         )
         SELECT
           count(*) FILTER (WHERE first_at >= $1)::int AS cohort_size,
           count(*) FILTER (WHERE first_at >= $1 AND first_at < now() - interval '1 day')::int AS d1_eligible,
           count(*) FILTER (WHERE first_at >= $1 AND first_at < now() - interval '1 day' AND EXISTS (
             SELECT 1 FROM act a WHERE a."userId" = fs."userId"
                AND a.day = date_trunc('day', fs.first_at) + interval '1 day'))::int AS d1_returned,
           count(*) FILTER (WHERE first_at >= $1 AND first_at < now() - interval '7 days')::int AS d7_eligible,
           count(*) FILTER (WHERE first_at >= $1 AND first_at < now() - interval '7 days' AND EXISTS (
             SELECT 1 FROM act a WHERE a."userId" = fs."userId"
                AND a.day > date_trunc('day', fs.first_at)
                AND a.day <= date_trunc('day', fs.first_at) + interval '7 days'))::int AS d7_returned
           FROM fs`,
        [since],
      );

      // Activation funnel straight from domain tables — no analytics events
      // needed. Cohort = users who registered within the range.
      const funnelQ = pg.query(
        `WITH cohort AS (SELECT id, "onboardingCompleted" FROM "User" WHERE "createdAt" >= $1)
         SELECT
           (SELECT count(*) FROM cohort)::int AS registered,
           (SELECT count(*) FROM cohort WHERE "onboardingCompleted")::int AS onboarded,
           (SELECT count(DISTINCT s."userId") FROM "Swipe" s
              WHERE s."userId" IN (SELECT id FROM cohort))::int AS swiped,
           (SELECT count(DISTINCT c."userId") FROM "Collection" c
              WHERE c."userId" IN (SELECT id FROM cohort))::int AS collected`,
        [since],
      );

      const screensQ = pg.query(
        `SELECT "screenName" AS screen, count(*)::int AS count
           FROM "AnalyticsEvent"
          WHERE "eventName" = 'screen_view' AND "createdAt" >= $1 AND "screenName" IS NOT NULL
          GROUP BY "screenName" ORDER BY count DESC`,
        [since],
      );

      const searchQ = pg.query(
        `SELECT count(*)::int AS searches,
                count(*) FILTER (
                  WHERE metadata->>'results' ~ '^[0-9]+$' AND (metadata->>'results')::int = 0
                )::int AS zero_results
           FROM "AnalyticsEvent" WHERE "eventName" = 'search' AND "createdAt" >= $1`,
        [since],
      );

      const swipeActionsQ = pg.query(
        `SELECT action, count(*)::int AS count
           FROM "Swipe" WHERE "createdAt" >= $1 GROUP BY action ORDER BY count DESC`,
        [since],
      );

      const itemViewsQ = pg.query(
        `SELECT count(*)::int AS item_views,
                count(DISTINCT metadata->>'itemId')::int AS unique_items
           FROM "AnalyticsEvent" WHERE "eventName" = 'item_view' AND "createdAt" >= $1`,
        [since],
      );

      const socialQ = pg.query(
        `SELECT
           (SELECT count(*) FROM "Follow" WHERE "createdAt" >= $1)::int AS follows,
           (SELECT count(*) FROM "Message" WHERE "createdAt" >= $1)::int AS messages`,
        [since],
      );

      const topLovedQ = pg.query(
        `SELECT s."itemId" AS "itemId", ci.name, ci.brand, count(*)::int AS n
           FROM "Swipe" s JOIN "ClothingItem" ci ON ci.id = s."itemId"
          WHERE s.action = 'LOVE' AND s."createdAt" >= $1
          GROUP BY s."itemId", ci.name, ci.brand ORDER BY n DESC LIMIT 10`,
        [since],
      );

      const topViewedQ = pg.query(
        `SELECT (ae.metadata->>'itemId') AS "itemId", ci.name, ci.brand, count(*)::int AS n
           FROM "AnalyticsEvent" ae JOIN "ClothingItem" ci ON ci.id = ae.metadata->>'itemId'
          WHERE ae."eventName" = 'item_view' AND ae."createdAt" >= $1
          GROUP BY ae.metadata->>'itemId', ci.name, ci.brand ORDER BY n DESC LIMIT 10`,
        [since],
      );

      const topBrandsQ = pg.query(
        `SELECT ci.brand, count(*)::int AS n
           FROM "Swipe" s JOIN "ClothingItem" ci ON ci.id = s."itemId"
          WHERE s.action = 'LOVE' AND s."createdAt" >= $1
          GROUP BY ci.brand ORDER BY n DESC LIMIT 10`,
        [since],
      );

      const [
        engagementR,
        sessionLenR,
        retentionR,
        funnelR,
        screensR,
        searchR,
        swipeActionsR,
        itemViewsR,
        socialR,
        topLovedR,
        topViewedR,
        topBrandsR,
      ] = await Promise.all([
        engagementQ,
        sessionLenQ,
        retentionQ,
        funnelQ,
        screensQ,
        searchQ,
        swipeActionsQ,
        itemViewsQ,
        socialQ,
        topLovedQ,
        topViewedQ,
        topBrandsQ,
      ]);

      const e = engagementR.rows[0] ?? {};
      const sl = sessionLenR.rows[0] ?? {};
      const ret = retentionR.rows[0] ?? {};
      const fn = funnelR.rows[0] ?? {};
      const sr = searchR.rows[0] ?? {};
      const iv = itemViewsR.rows[0] ?? {};
      const soc = socialR.rows[0] ?? {};

      res.json({
        range,
        since: since.toISOString(),
        checkedAt: new Date().toISOString(),
        engagement: {
          dau: e.dau ?? 0,
          wau: e.wau ?? 0,
          mau: e.mau ?? 0,
          sessions: e.sessions ?? 0,
          events: e.events ?? 0,
          anonSessions: e.anon_sessions ?? 0,
          identifiedSessions: e.identified_sessions ?? 0,
          avgSessionMs: sl.avg_ms ?? 0,
          medianSessionMs: sl.median_ms ?? 0,
        },
        retention: {
          cohortSize: ret.cohort_size ?? 0,
          d1Eligible: ret.d1_eligible ?? 0,
          d1Returned: ret.d1_returned ?? 0,
          d7Eligible: ret.d7_eligible ?? 0,
          d7Returned: ret.d7_returned ?? 0,
        },
        funnel: {
          registered: fn.registered ?? 0,
          onboarded: fn.onboarded ?? 0,
          swiped: fn.swiped ?? 0,
          collected: fn.collected ?? 0,
        },
        features: {
          screens: screensR.rows,
          searches: sr.searches ?? 0,
          zeroResultSearches: sr.zero_results ?? 0,
          swipeActions: swipeActionsR.rows,
          itemViews: iv.item_views ?? 0,
          uniqueItemsViewed: iv.unique_items ?? 0,
          follows: soc.follows ?? 0,
          messages: soc.messages ?? 0,
        },
        content: {
          topLoved: topLovedR.rows,
          topViewed: topViewedR.rows,
          topBrands: topBrandsR.rows,
        },
      });
    } catch (err) {
      console.error("[api/analytics/summary] Error:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Query failed" });
    }
  });

  return router;
}
