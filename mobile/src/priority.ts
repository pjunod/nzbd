export interface DownloadPriorityChoice {
  value: number;
  label: string;
}

export const DOWNLOAD_PRIORITIES: readonly DownloadPriorityChoice[] = [
  { value: -100, label: 'Very low' },
  { value: -50, label: 'Low' },
  { value: 0, label: 'Normal' },
  { value: 50, label: 'High' },
  { value: 100, label: 'Very high' },
  { value: 900, label: 'Force' },
] as const;

export function downloadPriorityLabel(priority: number): string {
  const choice = DOWNLOAD_PRIORITIES.find(({ value }) => value === priority);
  if (choice) return choice.label;
  return priority > 0 ? `Custom +${priority}` : `Custom ${priority}`;
}
