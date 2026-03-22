/** Default matches Stagehand v3 docs: https://docs.stagehand.dev/v3/configuration/models */
export const DEFAULT_STAGEHAND_MODEL = "google/gemini-3-flash-preview";

export function getStagehandModel(): string {
  const m = process.env.STAGEHAND_MODEL?.trim();
  return m && m.length > 0 ? m : DEFAULT_STAGEHAND_MODEL;
}
