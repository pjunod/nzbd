import { resolveTheme } from '../src/theme';

test('system theme follows the device and falls back to dark', () => {
  expect(resolveTheme('system', 'light').dark).toBe(false);
  expect(resolveTheme('system', 'dark').dark).toBe(true);
  expect(resolveTheme('system', null).dark).toBe(true);
  expect(resolveTheme('system', undefined).dark).toBe(true);
});

test('an explicit theme overrides the device preference', () => {
  expect(resolveTheme('light', 'dark').dark).toBe(false);
  expect(resolveTheme('dark', 'light').dark).toBe(true);
});
