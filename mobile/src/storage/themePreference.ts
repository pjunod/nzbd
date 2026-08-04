import * as SecureStore from 'expo-secure-store';

import type { LayoutPreference, PalettePreference, ThemePreference } from '../theme';

const THEME_KEY = 'nzbd.theme.v1';
const LAYOUT_KEY = 'nzbd.layout.v1';
const PALETTE_KEY = 'nzbd.palette.v1';

export async function loadThemePreference(): Promise<ThemePreference | null> {
  if (!(await SecureStore.isAvailableAsync())) return null;
  const stored = await SecureStore.getItemAsync(THEME_KEY);
  return stored === 'system' || stored === 'light' || stored === 'dark' ? stored : null;
}

export async function saveThemePreference(preference: ThemePreference): Promise<void> {
  if (!(await SecureStore.isAvailableAsync())) return;
  await SecureStore.setItemAsync(THEME_KEY, preference, {
    keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
  });
}

export async function loadLayoutPreference(): Promise<LayoutPreference | null> {
  if (!(await SecureStore.isAvailableAsync())) return null;
  const stored = await SecureStore.getItemAsync(LAYOUT_KEY);
  return stored === 'classic' || stored === 'plex' || stored === 'theater' ? stored : null;
}

export async function saveLayoutPreference(layout: LayoutPreference): Promise<void> {
  if (!(await SecureStore.isAvailableAsync())) return;
  await SecureStore.setItemAsync(LAYOUT_KEY, layout, {
    keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
  });
}

export async function loadPalettePreference(): Promise<PalettePreference | null> {
  if (!(await SecureStore.isAvailableAsync())) return null;
  const stored = await SecureStore.getItemAsync(PALETTE_KEY);
  return ['classic', 'terminal', 'noirr', 'amber', 'giallo', 'silver', 'void', 'vhs', 'paper', 'tide'].includes(stored ?? '')
    ? stored as PalettePreference
    : null;
}

export async function savePalettePreference(palette: PalettePreference): Promise<void> {
  if (!(await SecureStore.isAvailableAsync())) return;
  await SecureStore.setItemAsync(PALETTE_KEY, palette, {
    keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
  });
}
