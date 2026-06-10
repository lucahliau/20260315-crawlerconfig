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
