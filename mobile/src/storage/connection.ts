import * as SecureStore from 'expo-secure-store';

import { ConnectionConfig } from '../api/types';

const CONNECTION_KEY = 'nzbd.connection.v1';

export async function loadConnection(): Promise<ConnectionConfig | null> {
  if (!(await SecureStore.isAvailableAsync())) return null;
  const raw = await SecureStore.getItemAsync(CONNECTION_KEY);
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<ConnectionConfig>;
    if (!parsed.baseUrl || typeof parsed.baseUrl !== 'string') return null;
    return {
      baseUrl: parsed.baseUrl,
      username: parsed.username ?? '',
      password: parsed.password ?? '',
      token: parsed.token ?? '',
    };
  } catch {
    return null;
  }
}

export async function saveConnection(config: ConnectionConfig): Promise<void> {
  await SecureStore.setItemAsync(CONNECTION_KEY, JSON.stringify(config), {
    keychainAccessible: SecureStore.AFTER_FIRST_UNLOCK,
  });
}

export async function clearConnection(): Promise<void> {
  if (await SecureStore.isAvailableAsync()) {
    await SecureStore.deleteItemAsync(CONNECTION_KEY);
  }
}
