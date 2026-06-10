import crypto from "node:crypto";
import type { Express, Request } from "express";

/**
 * Single-password gate for the public Railway dashboard (legacy UI at /,
 * React dashboard at /app, mobile swipe at /swipe, and every /api route).
 *
 * Design goals: log in once per browser/device (~90-day signed cookie), no DB,
 * no extra deps. The HMAC key is derived from DASHBOARD_PASSWORD itself, so
 * rotating the password invalidates every outstanding session.
 *
 * - No DASHBOARD_PASSWORD set → auth disabled with a loud warning (local dev).
 * - Browsers: GET /login form → POST /api/login → httpOnly cookie.
 * - curl/scripts: `Authorization: Bearer <DASHBOARD_PASSWORD>` works everywhere.
 * - SSE (EventSource) sends cookies automatically, so job streams keep working.
 */

const COOKIE_NAME = "dash_auth";
const SESSION_TTL_MS = 90 * 24 * 60 * 60 * 1000; // 90 days
const LOGIN_PATH = "/login";
const LOGIN_FAILURES_BEFORE_COOLDOWN = 5;
const LOGIN_COOLDOWN_MS = 30_000;

function hmacKey(password: string): Buffer {
  return crypto.createHash("sha256").update(`clothedd-dashboard-v1:${password}`).digest();
}

function sign(expiresAtMs: number, key: Buffer): string {
  const mac = crypto.createHmac("sha256", key).update(String(expiresAtMs)).digest("base64url");
  return `${expiresAtMs}.${mac}`;
}

function verifyToken(token: string | undefined, key: Buffer): boolean {
  if (!token) return false;
  const dot = token.indexOf(".");
  if (dot <= 0) return false;
  const expiresAtMs = Number(token.slice(0, dot));
  if (!Number.isFinite(expiresAtMs) || expiresAtMs < Date.now()) return false;
  const expected = sign(expiresAtMs, key);
  const a = Buffer.from(token);
  const b = Buffer.from(expected);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function readCookie(req: Request, name: string): string | undefined {
  const header = req.headers.cookie;
  if (!header) return undefined;
  for (const part of header.split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0) continue;
    if (part.slice(0, eq).trim() === name) {
      return decodeURIComponent(part.slice(eq + 1).trim());
    }
  }
  return undefined;
}

function constantTimeEquals(a: string, b: string): boolean {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) {
    // Burn a comparison anyway so length mismatches don't return faster.
    crypto.timingSafeEqual(ab, ab);
    return false;
  }
  return crypto.timingSafeEqual(ab, bb);
}

function clientIp(req: Request): string {
  const fwd = req.headers["x-forwarded-for"];
  if (typeof fwd === "string" && fwd.length > 0) return fwd.split(",")[0]!.trim();
  return req.socket.remoteAddress ?? "unknown";
}

// Matches the dashboard design system: light, Inter, white card, near-black button.
const LOGIN_PAGE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Clothedd pipeline — log in</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: Inter, system-ui, -apple-system, sans-serif; background: #f9fafb; color: #111827;
           display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
    .card { background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 2rem; width: 20rem; }
    h1 { font-size: 1rem; font-weight: 600; margin: 0 0 0.25rem; }
    p { font-size: 0.8rem; color: #6b7280; margin: 0 0 1.25rem; }
    input { width: 100%; padding: 0.55rem 0.7rem; border: 1px solid #e5e7eb; border-radius: 8px;
            font-size: 0.9rem; margin-bottom: 0.75rem; outline: none; }
    input:focus { border-color: #111827; }
    button { width: 100%; padding: 0.55rem; font-size: 0.9rem; font-weight: 600; border: none;
             border-radius: 8px; background: #111827; color: #fff; cursor: pointer; }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    #err { font-size: 0.8rem; color: #dc2626; margin: 0.75rem 0 0; min-height: 1rem; }
  </style>
</head>
<body>
  <form class="card" id="f">
    <h1>Clothedd pipeline</h1>
    <p>Enter the dashboard password. You'll stay signed in on this device for 90 days.</p>
    <input id="pw" type="password" autocomplete="current-password" placeholder="Password" autofocus required>
    <button type="submit" id="btn">Log in</button>
    <p id="err" role="alert"></p>
  </form>
  <script>
    const form = document.getElementById('f');
    const err = document.getElementById('err');
    const btn = document.getElementById('btn');
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      err.textContent = '';
      btn.disabled = true;
      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ password: document.getElementById('pw').value }),
        });
        if (res.ok) {
          const params = new URLSearchParams(location.search);
          const next = params.get('next');
          location.href = next && next.startsWith('/') && !next.startsWith('//') ? next : '/app/';
          return;
        }
        const data = await res.json().catch(() => ({}));
        err.textContent = data.error || 'Login failed.';
      } catch {
        err.textContent = 'Network error. Try again.';
      } finally {
        btn.disabled = false;
      }
    });
  </script>
</body>
</html>`;

/**
 * Install the gate. Must run after express.json() and BEFORE any route or
 * static mount — everything registered later is protected.
 */
export function installDashboardAuth(app: Express): void {
  const password = process.env.DASHBOARD_PASSWORD?.trim();
  if (!password) {
    console.warn(
      "[dashboard-auth] DASHBOARD_PASSWORD not set — dashboard is UNPROTECTED. " +
        "Fine for local dev; set it in the Railway service variables for production.",
    );
    return;
  }
  const key = hmacKey(password);
  console.log("[dashboard-auth] enabled (90-day sessions; rotate DASHBOARD_PASSWORD to revoke)");

  const failures = new Map<string, { count: number; lockedUntil: number }>();

  app.get(LOGIN_PATH, (req, res) => {
    if (verifyToken(readCookie(req, COOKIE_NAME), key)) {
      res.redirect("/app/");
      return;
    }
    res.status(200).type("html").send(LOGIN_PAGE_HTML);
  });

  app.post("/api/login", (req, res) => {
    const ip = clientIp(req);
    const f = failures.get(ip);
    if (f && f.lockedUntil > Date.now()) {
      res.status(429).json({ error: "Too many attempts — wait 30 seconds." });
      return;
    }

    const supplied =
      typeof (req.body as { password?: unknown })?.password === "string"
        ? (req.body as { password: string }).password
        : "";
    if (!supplied || !constantTimeEquals(supplied, password)) {
      const count = (f?.count ?? 0) + 1;
      failures.set(
        ip,
        count >= LOGIN_FAILURES_BEFORE_COOLDOWN
          ? { count: 0, lockedUntil: Date.now() + LOGIN_COOLDOWN_MS }
          : { count, lockedUntil: 0 },
      );
      res.status(401).json({ error: "Wrong password." });
      return;
    }

    failures.delete(ip);
    const expiresAtMs = Date.now() + SESSION_TTL_MS;
    const secure = req.secure || req.headers["x-forwarded-proto"] === "https";
    res.setHeader(
      "Set-Cookie",
      `${COOKIE_NAME}=${encodeURIComponent(sign(expiresAtMs, key))}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${Math.floor(SESSION_TTL_MS / 1000)}${secure ? "; Secure" : ""}`,
    );
    res.json({ ok: true });
  });

  app.post("/api/logout", (_req, res) => {
    res.setHeader("Set-Cookie", `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
    res.json({ ok: true });
  });

  app.use((req, res, next) => {
    if (req.path === LOGIN_PATH || req.path === "/api/login") {
      next();
      return;
    }
    if (verifyToken(readCookie(req, COOKIE_NAME), key)) {
      next();
      return;
    }
    const bearer = req.headers.authorization;
    if (bearer?.startsWith("Bearer ") && constantTimeEquals(bearer.slice(7), password)) {
      next();
      return;
    }
    if (req.path.startsWith("/api/")) {
      res.status(401).json({
        error: "Unauthorized — log in at /login or send Authorization: Bearer <DASHBOARD_PASSWORD>",
      });
      return;
    }
    res.redirect(`${LOGIN_PATH}?next=${encodeURIComponent(req.originalUrl)}`);
  });
}
