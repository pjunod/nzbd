import { formatAppVersion } from '../src/app-version-format';

describe('app version formatting', () => {
  it('includes the native build number when it is available', () => {
    expect(formatAppVersion('1.1.0', '3')).toBe('1.1.0 (3)');
    expect(formatAppVersion('1.1.0', 3)).toBe('1.1.0 (3)');
  });

  it('handles development and incomplete manifests', () => {
    expect(formatAppVersion('1.1.0', null)).toBe('1.1.0');
    expect(formatAppVersion(undefined, undefined)).toBe('Unknown');
  });
});
