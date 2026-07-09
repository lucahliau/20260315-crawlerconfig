import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import { App } from "./App.tsx";
import { SwipeView } from "./components/swipe/SwipeView.tsx";

const rootEl = document.getElementById("root");
if (!rootEl) throw new Error("Root element #root not found");

// One bundle, two entry experiences: the desktop dashboard at /app and the
// mobile brand-swipe app at /swipe (server serves index.html for both).
const isSwipe = window.location.pathname.startsWith("/swipe");
if (isSwipe) {
  document.title = "Brand swipe";
  document.documentElement.style.colorScheme = "dark";
  document.querySelector('meta[name="theme-color"]')?.setAttribute("content", "#030712");
}

createRoot(rootEl).render(<StrictMode>{isSwipe ? <SwipeView /> : <App />}</StrictMode>);

// ---------------------------------------------------------------------------
// Stale-PWA auto-refresh. An iOS home-screen app resumes its FROZEN page on
// reopen and may not re-fetch index.html for days, so deploys never appear.
// Vite fingerprints the entry bundle (/assets/index-<hash>.js); compare the
// hash we're running against the one in the server's current index.html and
// hard-reload when a new build has shipped. Checked when the app comes back
// to the foreground (the PWA-resume moment) and every 5 minutes while open.
// ---------------------------------------------------------------------------
const BUNDLE_RE = /\/assets\/index-[\w-]+\.js/;

function currentBundle(): string | null {
  for (const s of Array.from(document.querySelectorAll("script[src]"))) {
    const m = (s as HTMLScriptElement).src.match(BUNDLE_RE);
    if (m) return m[0];
  }
  return null;
}

let lastVersionCheck = 0;
async function reloadIfNewBuild(): Promise<void> {
  const now = Date.now();
  if (now - lastVersionCheck < 60_000) return; // at most once a minute
  lastVersionCheck = now;
  const running = currentBundle();
  if (!running) return;
  try {
    const res = await fetch(`/app/index.html?ts=${now}`, { cache: "no-store" });
    if (!res.ok) return;
    const html = await res.text();
    const served = html.match(BUNDLE_RE)?.[0];
    if (served && served !== running) {
      window.location.reload();
    }
  } catch {
    // offline / transient — try again on the next resume or tick
  }
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible") void reloadIfNewBuild();
});
window.addEventListener("pageshow", (e) => {
  if ((e as PageTransitionEvent).persisted) void reloadIfNewBuild();
});
setInterval(() => void reloadIfNewBuild(), 5 * 60_000);
