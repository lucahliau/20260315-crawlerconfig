/**
 * Hostname-based slug used for config filenames (matches explore output).
 */
export function retailerSlugFromUrl(url: string): string {
  const hostname = new URL(url).hostname;
  return hostname.replace(/^www\./, "").split(".")[0];
}
