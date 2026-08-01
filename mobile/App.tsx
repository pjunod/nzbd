import { StatusBar } from 'expo-status-bar';
import { useEffect, useState } from 'react';
import { ActivityIndicator, StyleSheet, View } from 'react-native';
import { SafeAreaProvider } from 'react-native-safe-area-context';

import { ConnectionConfig } from './src/api/types';
import { ConnectionScreen } from './src/screens/ConnectionScreen';
import { DashboardScreen } from './src/screens/DashboardScreen';
import {
  clearConnection,
  loadConnection,
  saveConnection,
} from './src/storage/connection';
import { ThemeProvider, useTheme } from './src/theme';

export default function App() {
  return (
    <SafeAreaProvider>
      <ThemeProvider>
        <AppContent />
      </ThemeProvider>
    </SafeAreaProvider>
  );
}

function AppContent() {
  const theme = useTheme();
  const [loading, setLoading] = useState(true);
  const [config, setConfig] = useState<ConnectionConfig | null>(null);
  const [editing, setEditing] = useState(false);

  useEffect(() => {
    loadConnection()
      .then(setConfig)
      .finally(() => setLoading(false));
  }, []);

  const connect = async (next: ConnectionConfig) => {
    await saveConnection(next);
    setConfig(next);
    setEditing(false);
  };

  const forget = async () => {
    await clearConnection();
    setConfig(null);
    setEditing(false);
  };

  if (loading) {
    return (
      <View style={[styles.loading, { backgroundColor: theme.background }]}>
        <ActivityIndicator color={theme.accent} size="large" />
        <StatusBar style={theme.dark ? 'light' : 'dark'} />
      </View>
    );
  }

  return (
    <>
      {!config || editing ? (
        <ConnectionScreen
          initial={config ?? undefined}
          onCancel={config ? () => setEditing(false) : undefined}
          onConnect={connect}
          onForget={config ? forget : undefined}
        />
      ) : (
        <DashboardScreen config={config} onEditConnection={() => setEditing(true)} />
      )}
      <StatusBar style={theme.dark ? 'light' : 'dark'} />
    </>
  );
}

const styles = StyleSheet.create({
  loading: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
  },
});
