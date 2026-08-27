import { LAYOUT_OPTIONS, PALETTE_OPTIONS, resolveTheme } from '../src/theme';

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

test('the current native colors remain Classic', () => {
  expect(resolveTheme('dark', 'light', 'classic')).toMatchObject({
    background: '#0B1219',
    panel: '#121C25',
    accent: '#55AFFF',
  });
  expect(resolveTheme('light', 'dark', 'classic')).toMatchObject({
    background: '#F2F5F8',
    panel: '#FFFFFF',
    accent: '#0B77D5',
  });
});

test('the native app ships the shared display catalogue', () => {
  expect(LAYOUT_OPTIONS.map((option) => option.id)).toEqual(['classic', 'plex', 'theater']);
  expect(PALETTE_OPTIONS.map((option) => option.id)).toEqual([
    'classic',
    'terminal',
    'noirr',
    'amber',
    'giallo',
    'silver',
    'void',
    'vhs',
    'paper',
    'tide',
    'panoptic',
    'redline',
    'panovic',
  ]);
});

test('midnight-only palettes ignore light appearance', () => {
  expect(resolveTheme('light', 'light', 'void').dark).toBe(true);
  expect(resolveTheme('system', 'light', 'vhs').dark).toBe(true);
  expect(resolveTheme('light', 'light', 'panoptic').dark).toBe(true);
  expect(resolveTheme('system', 'light', 'redline').dark).toBe(true);
  expect(resolveTheme('light', 'light', 'panovic')).toMatchObject({
    dark: true,
    background: '#000000',
    panel: '#181818',
    panelAlt: '#242424',
    border: 'rgba(255, 255, 255, 0.06)',
    text: '#e8e6e1',
    textMuted: '#9a9aa0',
    accent: '#f0723b',
    onAccent: '#140a00',
    success: '#5fb582',
    warning: '#d9a05b',
    danger: '#ff7a66',
  });
});
