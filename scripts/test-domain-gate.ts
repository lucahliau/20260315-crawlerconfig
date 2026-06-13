/**
 * Smoke test for the per-domain upload gate (src/domainGate.ts).
 *
 * Asserts:
 *   1. Two tasks for the SAME domain never overlap (concurrency 1).
 *   2. Consecutive same-domain tasks are spaced by >= the configured gap.
 *   3. Tasks for DIFFERENT domains run in parallel (not serialized).
 *
 * Run: UPLOAD_PER_DOMAIN_DELAY_MS=300 npx tsx scripts/test-domain-gate.ts
 */
import { runForDomain, domainKey } from "../src/domainGate.js";

const GAP = parseInt(process.env.UPLOAD_PER_DOMAIN_DELAY_MS ?? "300", 10);
const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

let active = 0;
let maxConcurrentSameDomain = 0;
const sameDomainStartTimes: number[] = [];

async function sameDomainTask(label: string): Promise<void> {
  await runForDomain("shop.example.com", async () => {
    sameDomainStartTimes.push(Date.now());
    active += 1;
    maxConcurrentSameDomain = Math.max(maxConcurrentSameDomain, active);
    await sleep(50); // simulate a fetch+upload
    active -= 1;
  });
}

async function main(): Promise<void> {
  const t0 = Date.now();

  // 1 + 2: fire 4 same-domain tasks at once; the gate must serialize + space them.
  await Promise.all([
    sameDomainTask("a"),
    sameDomainTask("b"),
    sameDomainTask("c"),
    sameDomainTask("d"),
  ]);

  // 3: two different domains fired together should run concurrently.
  let crossDomainOverlap = false;
  let dOneActive = false;
  await Promise.all([
    runForDomain("one.example.com", async () => {
      dOneActive = true;
      await sleep(80);
      dOneActive = false;
    }),
    runForDomain("two.example.com", async () => {
      await sleep(10);
      if (dOneActive) crossDomainOverlap = true; // good: ran while domain one was busy
    }),
  ]);

  // Evaluate assertions.
  const errors: string[] = [];
  if (maxConcurrentSameDomain !== 1) {
    errors.push(`same-domain concurrency was ${maxConcurrentSameDomain}, expected 1 (no overlap).`);
  }
  let minGapObserved = Infinity;
  for (let i = 1; i < sameDomainStartTimes.length; i++) {
    minGapObserved = Math.min(minGapObserved, sameDomainStartTimes[i] - sameDomainStartTimes[i - 1]);
  }
  // Allow a small scheduler slack below the configured gap.
  if (minGapObserved < GAP - 30) {
    errors.push(`min same-domain gap was ${minGapObserved}ms, expected >= ${GAP}ms.`);
  }
  if (!crossDomainOverlap) {
    errors.push("different domains did NOT run concurrently (gate over-serialized).");
  }
  if (domainKey("https://www.cafeducycliste.com/products/x", "fallback") !== "www.cafeducycliste.com") {
    errors.push("domainKey did not extract hostname from a URL.");
  }
  if (domainKey("not a url", "fallback") !== "fallback") {
    errors.push("domainKey did not fall back on an invalid URL.");
  }

  console.log(
    `gate test: maxConcurrentSameDomain=${maxConcurrentSameDomain} minGap=${minGapObserved}ms ` +
      `crossDomainOverlap=${crossDomainOverlap} totalMs=${Date.now() - t0}`,
  );
  if (errors.length) {
    console.error("FAIL:\n - " + errors.join("\n - "));
    process.exit(1);
  }
  console.log("PASS — per-domain gate serializes same-domain, spaces by gap, parallelizes across domains.");
}

main().catch((err) => {
  console.error("test crashed:", err);
  process.exit(1);
});
