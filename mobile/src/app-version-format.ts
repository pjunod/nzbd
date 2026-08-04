export function formatAppVersion(
  version: string | null | undefined,
  build: string | number | null | undefined,
) {
  const normalizedVersion = version?.trim() || 'Unknown';
  const normalizedBuild = build == null ? '' : String(build).trim();
  return normalizedBuild ? `${normalizedVersion} (${normalizedBuild})` : normalizedVersion;
}
