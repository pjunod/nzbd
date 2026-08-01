import * as SecureStore from 'expo-secure-store';

import type { ThemePreference } from '../theme';

const THEME_KEY = 'nzbd.theme.v1';

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
