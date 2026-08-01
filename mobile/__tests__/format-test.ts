import {
  criticalStorage,
  formatBytes,
  jobProgress,
  jobStatusKey,
  jobStatusLabel,
  normalizeServerUrl,
  storageUsage,
} from '../src/api/format';
import { JobSummary } from '../src/api/types';

const job = (changes: Partial<JobSummary> = {}): JobSummary => ({
  id: 1,
  name: 'example',
  status: 'downloading',
  category: null,
  priority: 0,
  size_bytes: 100,
  downloaded_bytes: 25,
  failed_bytes: 0,
  remaining_bytes: 75,
  total_articles: 10,
  done_articles: 2,
  failed_articles: 0,
  files_total: 1,
  files_done: 0,
  health: 1000,
  critical_health: 850,
  rate_bps: 10,
  retried_articles: 0,
  assigned_node: null,
  pp_done: false,
  dupe_key: '',
  dupe_score: 0,
  params: [],
  stages: [],
  ...changes,
});

describe('API presentation helpers', () => {
  test('normalizes a host and supplies the LAN-friendly HTTP scheme', () => {
    expect(normalizeServerUrl(' 192.168.1.20:6789 ')).toBe('http://192.168.1.20:6789');
    expect(normalizeServerUrl('https://downloads.example.test')).toBe(
      'https://downloads.example.test:6789',
    );
    expect(normalizeServerUrl('nuc3')).toBe('http://nuc3:6789');
    expect(normalizeServerUrl('nuc3.local')).toBe('http://nuc3.local:6789');
    expect(normalizeServerUrl('192.168.4.7')).toBe('http://192.168.4.7:6789');
    expect(normalizeServerUrl('[2001:db8::7]')).toBe('http://[2001:db8::7]:6789');
  });

  test('preserves an explicitly selected port', () => {
    expect(normalizeServerUrl('nuc3:8080')).toBe('http://nuc3:8080');
    expect(normalizeServerUrl('https://nuc3:8443')).toBe('https://nuc3:8443');
  });

  test('rejects credentials and API paths embedded in the server URL', () => {
    expect(() => normalizeServerUrl('http://user:pass@example.test:6789')).toThrow(
      'credentials',
    );
    expect(() => normalizeServerUrl('http://example.test:6789/api')).toThrow(
      'without a path',
    );
  });

  test('reads both unit and structured Rust enum status shapes', () => {
    expect(jobStatusKey('paused')).toBe('paused');
    expect(jobStatusKey({ post: { stage: 'par_repair' } })).toBe('post:par_repair');
    expect(jobStatusLabel({ post: { stage: 'par_repair' } })).toBe('par repair');
  });

  test('calculates bounded byte progress and formats binary units', () => {
    expect(jobProgress(job())).toBe(0.25);
    expect(jobProgress(job({ downloaded_bytes: 250 }))).toBe(1);
    expect(formatBytes(1_048_576)).toBe('1.00 MiB');
  });

  test('selects the most-full configured filesystem as critical storage', () => {
    const paths = [
      {
        label: 'working',
        path: '/data',
        available_bytes: 40,
        total_bytes: 100,
      },
      {
        label: 'complete',
        path: '/downloads',
        available_bytes: 5,
        total_bytes: 100,
      },
      {
        label: 'measuring',
        path: '/later',
        available_bytes: null,
        total_bytes: null,
      },
    ];

    const critical = criticalStorage(paths);
    expect(critical?.label).toBe('complete');
    expect(critical && storageUsage(critical)).toEqual({
      availableBytes: 5,
      totalBytes: 100,
      usedBytes: 95,
      usedPercent: 95,
    });
  });
});
