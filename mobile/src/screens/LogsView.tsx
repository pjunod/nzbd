import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ActivityIndicator,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Switch,
  Text,
  View,
} from 'react-native';

import { NzbdClient } from '../api/client';
import { ConnectionConfig, LogEntry } from '../api/types';
import { Theme, useTheme } from '../theme';

export function LogsView({ config }: { config: ConnectionConfig }) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const client = useMemo(() => new NzbdClient(config), [config]);
  const [entries, setEntries] = useState<LogEntry[] | null>(null);
  const [includeFiles, setIncludeFiles] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(
    async (showSpinner = true) => {
      if (showSpinner) setRefreshing(true);
      try {
        const next = await client.getLogs(includeFiles);
        setEntries([...next.entries].reverse());
        setError(null);
      } catch (cause) {
        setError(cause instanceof Error ? cause.message : 'Could not load Runner logs.');
      } finally {
        if (showSpinner) setRefreshing(false);
      }
    },
    [client, includeFiles],
  );

  useEffect(() => {
    void refresh();
    const timer = setInterval(() => void refresh(false), 4_000);
    return () => clearInterval(timer);
  }, [refresh]);

  return (
    <ScrollView
      contentContainerStyle={styles.content}
      refreshControl={
        <RefreshControl onRefresh={() => void refresh()} refreshing={refreshing} tintColor={theme.accent} />
      }
    >
      <View style={styles.heading}>
        <View>
          <Text style={styles.eyebrow}>LOGS</Text>
          <Text style={styles.title}>Recent activity</Text>
          <Text style={styles.subtitle}>Newest first · refreshes every 4 seconds</Text>
        </View>
        <View style={styles.fileToggle}>
          <Text style={styles.toggleText}>File details</Text>
          <Switch
            onValueChange={setIncludeFiles}
            trackColor={{ false: theme.border, true: theme.accent }}
            value={includeFiles}
          />
        </View>
      </View>

      {error ? (
        <Pressable accessibilityRole="button" onPress={() => void refresh()} style={styles.error}>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.retry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {!entries && refreshing ? (
        <View style={styles.empty}>
          <ActivityIndicator color={theme.accent} />
          <Text style={styles.emptyText}>Loading logs…</Text>
        </View>
      ) : entries?.length === 0 ? (
        <View style={styles.empty}>
          <Text style={styles.emptyTitle}>No log entries</Text>
          <Text style={styles.emptyText}>New daemon activity will appear here automatically.</Text>
        </View>
      ) : (
        <View style={styles.list}>
          {entries?.map((entry) => <LogRow entry={entry} key={entry.id} styles={styles} />)}
        </View>
      )}
    </ScrollView>
  );
}

function LogRow({ entry, styles }: { entry: LogEntry; styles: ReturnType<typeof makeStyles> }) {
  return (
    <View style={styles.row}>
      <View style={styles.rowMeta}>
        <Text style={[styles.kind, kindStyle(entry.kind, styles)]}>{entry.kind}</Text>
        <Text style={styles.time}>{formatLogTime(entry.time_unix)}</Text>
        <Text style={styles.scope}>
          {entry.scope}{entry.job == null ? '' : ` · job #${entry.job}`}
        </Text>
      </View>
      <Text selectable style={styles.message}>
        {entry.text}
      </Text>
    </View>
  );
}

function kindStyle(kind: string, styles: ReturnType<typeof makeStyles>) {
  if (kind === 'ERROR') return styles.kindError;
  if (kind === 'WARNING') return styles.kindWarning;
  if (kind === 'DETAIL') return styles.kindDetail;
  return styles.kindInfo;
}

function formatLogTime(unix: number): string {
  if (!Number.isFinite(unix) || unix <= 0) return '—';
  return new Date(unix * 1000).toLocaleTimeString();
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    content: { padding: 14, paddingBottom: 44, gap: 12, maxWidth: 1100, width: '100%', alignSelf: 'center' },
    heading: { flexDirection: 'row', alignItems: 'flex-end', justifyContent: 'space-between', gap: 14, paddingHorizontal: 2 },
    eyebrow: { color: theme.textMuted, fontSize: 10, fontWeight: '800', letterSpacing: 1.3 },
    title: { color: theme.text, fontSize: 23, fontWeight: '800', letterSpacing: -0.4 },
    subtitle: { color: theme.textMuted, fontSize: 10, marginTop: 2 },
    fileToggle: { alignItems: 'center', gap: 3 },
    toggleText: { color: theme.textMuted, fontSize: 10, fontWeight: '700' },
    list: { gap: 7 },
    row: { padding: 12, borderRadius: 12, borderWidth: 1, borderColor: theme.border, backgroundColor: theme.panel, gap: 7 },
    rowMeta: { flexDirection: 'row', alignItems: 'center', flexWrap: 'wrap', gap: 7 },
    kind: { fontSize: 9, fontWeight: '900', paddingHorizontal: 6, paddingVertical: 3, borderRadius: 6, overflow: 'hidden' },
    kindInfo: { color: theme.accent, backgroundColor: theme.accentSoft },
    kindWarning: { color: theme.warning, backgroundColor: theme.panelAlt },
    kindError: { color: theme.danger, backgroundColor: theme.dangerSoft },
    kindDetail: { color: theme.textMuted, backgroundColor: theme.panelAlt },
    time: { color: theme.textMuted, fontSize: 10, fontVariant: ['tabular-nums'] },
    scope: { color: theme.textMuted, fontSize: 9, textTransform: 'uppercase', fontWeight: '700' },
    message: { color: theme.text, fontSize: 12, lineHeight: 18, fontFamily: 'monospace' },
    empty: { minHeight: 230, padding: 28, borderRadius: 18, borderWidth: 1, borderStyle: 'dashed', borderColor: theme.border, backgroundColor: theme.panel, alignItems: 'center', justifyContent: 'center', gap: 10 },
    emptyTitle: { color: theme.text, fontSize: 20, fontWeight: '800' },
    emptyText: { color: theme.textMuted, fontSize: 14, lineHeight: 20, textAlign: 'center' },
    error: { padding: 13, borderRadius: 12, backgroundColor: theme.dangerSoft, gap: 4 },
    errorText: { color: theme.danger, fontSize: 13, lineHeight: 18 },
    retry: { color: theme.danger, fontSize: 11, fontWeight: '800' },
  });
