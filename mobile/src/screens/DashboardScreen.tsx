import * as DocumentPicker from 'expo-document-picker';
import { File } from 'expo-file-system';
import { useMemo, useState } from 'react';
import {
  Alert,
  Modal,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Switch,
  Text,
  TextInput,
  useWindowDimensions,
  View,
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';

import {
  formatBytes,
  formatDuration,
  isJobPaused,
  jobEta,
  jobProgress,
  jobStatusKey,
  jobStatusLabel,
} from '../api/format';
import {
  AddNzbOptions,
  ConnectionConfig,
  ConnectionState,
  JobSummary,
  StatusDto,
} from '../api/types';
import { ActionButton } from '../components/ActionButton';
import { useNzbd } from '../hooks/useNzbd';
import { Theme, useTheme } from '../theme';
import { HistoryView } from './HistoryView';
import { LogsView } from './LogsView';

type AppSection = 'queue' | 'history' | 'logs';

interface Props {
  config: ConnectionConfig;
  onEditConnection: () => void;
}

export function DashboardScreen({ config, onEditConnection }: Props) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const { width } = useWindowDimensions();
  const wide = width >= 820;
  const [addOpen, setAddOpen] = useState(false);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [activeSection, setActiveSection] = useState<AppSection>('queue');
  const {
    snapshot,
    connectionState,
    error,
    busyKey,
    lastUpdated,
    refresh,
    queueAction,
    jobAction,
    addNzb,
  } = useNzbd(config);

  const status = snapshot?.status;
  const jobs = snapshot?.jobs ?? [];
  const mutateJob = async (
    job: JobSummary,
    action:
      | 'pause'
      | 'resume'
      | 'delete'
      | 'move-top'
      | 'move-up'
      | 'move-down'
      | 'move-bottom',
  ) => {
    try {
      const result = await jobAction(job.id, action);
      if (action === 'delete') {
        setExpanded(null);
        setNotice(result.parked ? `${job.name} removed. It can be restored from history.` : `${job.name} removed.`);
      }
    } catch {
      // The hook owns the visible error banner.
    }
  };

  const confirmDelete = (job: JobSummary) => {
    Alert.alert(
      'Remove this job?',
      `Remove “${job.name}” from the queue? Downloaded files are left in place.`,
      [
        { text: 'Cancel', style: 'cancel' },
        { text: 'Remove', style: 'destructive', onPress: () => void mutateJob(job, 'delete') },
      ],
    );
  };

  const toggleQueue = async () => {
    if (!status) return;
    const action = status.download_paused ? 'resume' : 'pause';
    try {
      await queueAction(action);
      setNotice(action === 'pause' ? 'Queue paused.' : 'Queue resumed.');
    } catch {
      // The hook owns the visible error banner.
    }
  };

  const submitNzb = async (file: File, options: AddNzbOptions) => {
    const result = await addNzb(file, options);
    setAddOpen(false);
    setNotice(`Added ${options.name} as job #${result.id}.`);
  };

  return (
    <SafeAreaView style={styles.safe} edges={['top', 'left', 'right']}>
      <View style={styles.header}>
        <View style={styles.brandRow}>
          <View style={styles.brandMark}>
            <Text style={styles.brandMarkText}>n</Text>
          </View>
          <View style={styles.brandText}>
            <Text style={styles.brand}>nzbd</Text>
            <View style={styles.connectionRow}>
              <View
                style={[
                  styles.connectionDot,
                  connectionColor(connectionState, theme),
                ]}
              />
              <Text numberOfLines={1} style={styles.serverName}>
                {connectionLabel(connectionState)} · {config.baseUrl}
              </Text>
            </View>
          </View>
        </View>
        <View style={styles.headerActions}>
          {activeSection === 'queue' ? (
            <ActionButton
              compact
              disabled={!status || busyKey !== null}
              label={status?.download_paused ? 'Resume' : 'Pause'}
              loading={busyKey?.startsWith('queue:')}
              onPress={() => void toggleQueue()}
            />
          ) : null}
          <ActionButton compact label="Add NZB" onPress={() => setAddOpen(true)} variant="primary" />
          <ActionButton compact label="Server" onPress={onEditConnection} variant="ghost" />
        </View>
      </View>

      {error ? (
        <View style={styles.errorBanner}>
          <Text accessibilityLiveRegion="polite" style={styles.errorText}>
            {error}
          </Text>
          <Pressable accessibilityRole="button" onPress={() => void refresh()}>
            <Text style={styles.retryText}>Retry</Text>
          </Pressable>
        </View>
      ) : null}
      {notice ? (
        <Pressable
          accessibilityRole="button"
          accessibilityLabel="Dismiss message"
          onPress={() => setNotice(null)}
          style={styles.notice}
        >
          <Text accessibilityLiveRegion="polite" style={styles.noticeText}>
            {notice}
          </Text>
        </Pressable>
      ) : null}
      {activeSection === 'queue' && status ? <GuardBanner status={status} styles={styles} /> : null}

      <View accessibilityRole="tablist" style={styles.tabs}>
        {(['queue', 'history', 'logs'] as AppSection[]).map((section) => (
          <Pressable
            accessibilityRole="tab"
            accessibilityState={{ selected: activeSection === section }}
            key={section}
            onPress={() => setActiveSection(section)}
            style={({ pressed }) => [
              styles.tab,
              activeSection === section && styles.tabSelected,
              pressed && styles.pressed,
            ]}
          >
            <Text style={[styles.tabText, activeSection === section && styles.tabTextSelected]}>
              {section[0].toUpperCase() + section.slice(1)}
            </Text>
          </Pressable>
        ))}
      </View>

      {activeSection === 'queue' ? (
        <ScrollView
          contentContainerStyle={[styles.content, wide && styles.contentWide]}
          refreshControl={
            <RefreshControl
              onRefresh={() => void refresh()}
              refreshing={connectionState === 'connecting'}
              tintColor={theme.accent}
            />
          }
        >
          {!wide && status ? <Overview status={status} styles={styles} /> : null}

          <View style={[styles.dashboard, wide && styles.dashboardWide]}>
            <View style={styles.queueColumn}>
              <View style={styles.sectionHeading}>
                <View>
                  <Text style={styles.eyebrow}>QUEUE</Text>
                  <Text style={styles.sectionTitle}>
                    {jobs.length === 1 ? '1 job' : `${jobs.length} jobs`}
                  </Text>
                </View>
                <Text style={styles.updated}>
                  {lastUpdated ? `updated ${formatAge(lastUpdated)}` : 'connecting…'}
                </Text>
              </View>

              {jobs.length === 0 ? (
                <View style={styles.empty}>
                  <Text style={styles.emptyTitle}>{snapshot ? 'Queue is empty' : 'Loading queue'}</Text>
                  <Text style={styles.emptyText}>
                    {snapshot
                      ? 'Pick an NZB from Files or Android’s document picker to start a download.'
                      : 'Waiting for the first snapshot from nzbd.'}
                  </Text>
                  {snapshot ? (
                    <ActionButton label="Add an NZB" onPress={() => setAddOpen(true)} variant="primary" />
                  ) : null}
                </View>
              ) : (
                <View style={styles.jobList}>
                  {jobs.map((job, index) => (
                    <JobCard
                      busy={busyKey !== null}
                      expanded={expanded === job.id}
                      index={index}
                      job={job}
                      key={job.id}
                      onAction={(action) => void mutateJob(job, action)}
                      onDelete={() => confirmDelete(job)}
                      onToggle={() => setExpanded(expanded === job.id ? null : job.id)}
                      styles={styles}
                      total={jobs.length}
                    />
                  ))}
                </View>
              )}
            </View>

            {wide && status ? (
              <View style={styles.sidebar}>
                <Overview status={status} styles={styles} vertical />
                <ServerList status={status} styles={styles} />
              </View>
            ) : null}
          </View>
          {!wide && status ? <ServerList status={status} styles={styles} /> : null}
        </ScrollView>
      ) : activeSection === 'history' ? (
        <HistoryView config={config} />
      ) : (
        <LogsView config={config} />
      )}

      <AddNzbModal
        busy={busyKey === 'add'}
        onClose={() => setAddOpen(false)}
        onSubmit={submitNzb}
        open={addOpen}
      />
    </SafeAreaView>
  );
}

function Overview({
  status,
  styles,
  vertical = false,
}: {
  status: StatusDto;
  styles: ReturnType<typeof makeStyles>;
  vertical?: boolean;
}) {
  const eta = status.download_paused
    ? 'paused'
    : status.download_rate_bps > 0
      ? formatDuration(status.remaining_bytes / status.download_rate_bps)
      : '—';
  return (
    <View style={[styles.metrics, vertical && styles.metricsVertical]}>
      <Metric
        label="Speed"
        styles={styles}
        value={formatBytes(status.download_rate_bps, '/s')}
      />
      <Metric label="Remaining" styles={styles} value={formatBytes(status.remaining_bytes)} />
      <Metric label="Time left" styles={styles} value={eta} />
      <Metric
        label="Active / queued"
        styles={styles}
        value={`${status.jobs_downloading} / ${status.jobs_queued}`}
      />
    </View>
  );
}

function Metric({
  label,
  value,
  styles,
}: {
  label: string;
  value: string;
  styles: ReturnType<typeof makeStyles>;
}) {
  return (
    <View style={styles.metric}>
      <Text style={styles.metricLabel}>{label}</Text>
      <Text adjustsFontSizeToFit numberOfLines={1} style={styles.metricValue}>
        {value}
      </Text>
    </View>
  );
}

function JobCard({
  job,
  index,
  total,
  expanded,
  busy,
  onToggle,
  onAction,
  onDelete,
  styles,
}: {
  job: JobSummary;
  index: number;
  total: number;
  expanded: boolean;
  busy: boolean;
  onToggle: () => void;
  onAction: (
    action: 'pause' | 'resume' | 'move-top' | 'move-up' | 'move-down' | 'move-bottom',
  ) => void;
  onDelete: () => void;
  styles: ReturnType<typeof makeStyles>;
}) {
  const progress = jobProgress(job);
  const statusKey = jobStatusKey(job.status);
  const canPause = ['queued', 'downloading', 'fetching'].includes(statusKey);
  const canResume = isJobPaused(job.status);
  return (
    <View style={[styles.jobCard, expanded && styles.jobCardExpanded]}>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        onPress={onToggle}
        style={({ pressed }) => [styles.jobMain, pressed && styles.pressed]}
      >
        <View style={styles.jobTopline}>
          <Text numberOfLines={2} style={styles.jobName}>
            {job.name}
          </Text>
          <Text style={styles.jobPercent}>{Math.floor(progress * 100)}%</Text>
        </View>
        <View style={styles.progressTrack}>
          <View style={[styles.progressFill, { width: `${Math.max(progress * 100, 1)}%` }]} />
        </View>
        <View style={styles.jobMeta}>
          <Text style={[styles.status, statusKey === 'failed' && styles.statusFailed]}>
            {jobStatusLabel(job.status)}
          </Text>
          <Text style={styles.metaText}>
            {formatBytes(job.downloaded_bytes)} / {formatBytes(job.size_bytes)}
          </Text>
          <Text style={styles.metaText}>{formatBytes(job.rate_bps, '/s')}</Text>
          <Text style={styles.metaText}>ETA {jobEta(job)}</Text>
        </View>
      </Pressable>

      {expanded ? (
        <View style={styles.jobActions}>
          <View style={styles.jobFacts}>
            <Fact label="Health" styles={styles} value={`${(job.health / 10).toFixed(1)}%`} />
            <Fact label="Files" styles={styles} value={`${job.files_done}/${job.files_total}`} />
            <Fact label="Priority" styles={styles} value={String(job.priority)} />
            <Fact label="Category" styles={styles} value={job.category || '—'} />
          </View>
          <View style={styles.actionRow}>
            {canPause ? (
              <ActionButton compact disabled={busy} label="Pause" onPress={() => onAction('pause')} />
            ) : null}
            {canResume ? (
              <ActionButton compact disabled={busy} label="Resume" onPress={() => onAction('resume')} />
            ) : null}
            <ActionButton
              compact
              disabled={busy || index === 0}
              label="Top"
              onPress={() => onAction('move-top')}
            />
            <ActionButton
              compact
              disabled={busy || index === 0}
              label="Up"
              onPress={() => onAction('move-up')}
            />
            <ActionButton
              compact
              disabled={busy || index === total - 1}
              label="Down"
              onPress={() => onAction('move-down')}
            />
            <ActionButton
              compact
              disabled={busy || index === total - 1}
              label="Bottom"
              onPress={() => onAction('move-bottom')}
            />
            <ActionButton compact disabled={busy} label="Remove" onPress={onDelete} variant="danger" />
          </View>
        </View>
      ) : null}
    </View>
  );
}

function Fact({
  label,
  value,
  styles,
}: {
  label: string;
  value: string;
  styles: ReturnType<typeof makeStyles>;
}) {
  return (
    <View style={styles.fact}>
      <Text style={styles.factLabel}>{label}</Text>
      <Text numberOfLines={1} style={styles.factValue}>
        {value}
      </Text>
    </View>
  );
}

function ServerList({
  status,
  styles,
}: {
  status: StatusDto;
  styles: ReturnType<typeof makeStyles>;
}) {
  if (status.servers.length === 0) return null;
  return (
    <View style={styles.serverPanel}>
      <Text style={styles.eyebrow}>PROVIDERS</Text>
      {status.servers.map((server) => (
        <View key={server.server} style={styles.serverRow}>
          <View style={styles.serverIdentity}>
            <View
              style={[
                styles.serverDot,
                status.blocked_servers.includes(server.server) && styles.serverDotBlocked,
              ]}
            />
            <Text numberOfLines={1} style={styles.providerName}>
              {server.name}
            </Text>
          </View>
          <Text style={styles.providerRate}>{formatBytes(server.rate_bps, '/s')}</Text>
        </View>
      ))}
    </View>
  );
}

function GuardBanner({
  status,
  styles,
}: {
  status: StatusDto;
  styles: ReturnType<typeof makeStyles>;
}) {
  let message: string | null = null;
  if (status.disk_low) message = 'Downloads are held because the destination volume is low on space.';
  else if (status.quota_reached) message = 'Downloads are held because the configured quota was reached.';
  else if (status.blocked_servers.length > 0)
    message = `${status.blocked_servers.length} provider ${status.blocked_servers.length === 1 ? 'is' : 'are'} temporarily blocked.`;
  if (!message) return null;
  return (
    <View style={styles.guardBanner}>
      <Text style={styles.guardText}>{message}</Text>
    </View>
  );
}

function AddNzbModal({
  open,
  busy,
  onClose,
  onSubmit,
}: {
  open: boolean;
  busy: boolean;
  onClose: () => void;
  onSubmit: (file: File, options: AddNzbOptions) => Promise<void>;
}) {
  const theme = useTheme();
  const styles = useMemo(() => makeStyles(theme), [theme]);
  const [asset, setAsset] = useState<DocumentPicker.DocumentPickerAsset | null>(null);
  const [name, setName] = useState('');
  const [category, setCategory] = useState('');
  const [priority, setPriority] = useState(0);
  const [paused, setPaused] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const pick = async () => {
    setError(null);
    const result = await DocumentPicker.getDocumentAsync({
      type: '*/*',
      copyToCacheDirectory: true,
      multiple: false,
    });
    if (result.canceled) return;
    const selected = result.assets[0];
    if (!selected.name.toLowerCase().endsWith('.nzb')) {
      setError('Choose a file ending in .nzb. Compressed NZBs are not accepted by this endpoint.');
      return;
    }
    setAsset(selected);
    setName(selected.name.replace(/\.nzb$/i, ''));
  };

  const submit = async () => {
    if (!asset) {
      setError('Choose an NZB file first.');
      return;
    }
    setError(null);
    try {
      await onSubmit(new File(asset.uri), {
        name: name.trim() || asset.name.replace(/\.nzb$/i, ''),
        category: category.trim() || undefined,
        priority,
        paused,
      });
      setAsset(null);
      setName('');
      setCategory('');
      setPriority(0);
      setPaused(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'nzbd could not add this file.');
    }
  };

  return (
    <Modal animationType="slide" onRequestClose={onClose} transparent visible={open}>
      <View style={styles.modalBackdrop}>
        <SafeAreaView style={styles.modalCard} edges={['bottom']}>
          <View style={styles.modalHandle} />
          <View style={styles.modalHeading}>
            <View>
              <Text style={styles.eyebrow}>NEW DOWNLOAD</Text>
              <Text style={styles.modalTitle}>Add an NZB</Text>
            </View>
            <ActionButton compact disabled={busy} label="Close" onPress={onClose} variant="ghost" />
          </View>

          <Pressable
            accessibilityRole="button"
            disabled={busy}
            onPress={() => void pick()}
            style={styles.filePicker}
          >
            <Text style={styles.filePickerTitle}>{asset ? asset.name : 'Choose NZB from Files'}</Text>
            <Text style={styles.filePickerMeta}>
              {asset
                ? asset.size
                  ? formatBytes(asset.size)
                  : 'Ready to upload'
                : 'iCloud Drive, On My iPhone/iPad, or an Android document provider'}
            </Text>
          </Pressable>

          <Text style={styles.inputLabel}>Display name</Text>
          <TextInput
            editable={!busy}
            onChangeText={setName}
            placeholder="Taken from the NZB filename"
            placeholderTextColor={theme.textMuted}
            style={styles.input}
            value={name}
          />
          <Text style={styles.inputLabel}>Category</Text>
          <TextInput
            autoCapitalize="none"
            editable={!busy}
            onChangeText={setCategory}
            placeholder="movies, tv, music…"
            placeholderTextColor={theme.textMuted}
            style={styles.input}
            value={category}
          />

          <Text style={styles.inputLabel}>Priority</Text>
          <View style={styles.priorityRow}>
            {[
              [-100, 'Low'],
              [0, 'Normal'],
              [100, 'High'],
              [900, 'Force'],
            ].map(([value, label]) => (
              <Pressable
                accessibilityRole="button"
                accessibilityState={{ selected: priority === value }}
                disabled={busy}
                key={value}
                onPress={() => setPriority(value as number)}
                style={[styles.priorityChip, priority === value && styles.priorityChipSelected]}
              >
                <Text
                  style={[
                    styles.priorityChipText,
                    priority === value && styles.priorityChipTextSelected,
                  ]}
                >
                  {label}
                </Text>
              </Pressable>
            ))}
          </View>

          <View style={styles.switchRow}>
            <View style={styles.switchText}>
              <Text style={styles.switchTitle}>Add paused</Text>
              <Text style={styles.switchHint}>The job waits until you resume it.</Text>
            </View>
            <Switch
              disabled={busy}
              onValueChange={setPaused}
              trackColor={{ false: theme.border, true: theme.accent }}
              value={paused}
            />
          </View>

          {error ? (
            <View style={styles.modalError}>
              <Text accessibilityLiveRegion="polite" style={styles.errorText}>
                {error}
              </Text>
            </View>
          ) : null}
          <ActionButton
            disabled={!asset}
            label="Add to queue"
            loading={busy}
            onPress={() => void submit()}
            variant="primary"
          />
        </SafeAreaView>
      </View>
    </Modal>
  );
}

function connectionLabel(state: ConnectionState): string {
  return {
    connecting: 'connecting',
    live: 'live',
    polling: 'polling',
    reconnecting: 'reconnecting',
    offline: 'unreachable',
  }[state];
}

function connectionColor(state: ConnectionState, theme: Theme) {
  return {
    backgroundColor:
      state === 'live'
        ? theme.success
        : state === 'offline'
          ? theme.danger
          : theme.warning,
  };
}

function formatAge(updatedAt: number): string {
  const seconds = Math.max(0, Math.floor((Date.now() - updatedAt) / 1000));
  return seconds < 2 ? 'now' : `${seconds}s ago`;
}

const makeStyles = (theme: Theme) =>
  StyleSheet.create({
    safe: { flex: 1, backgroundColor: theme.background },
    header: {
      minHeight: 70,
      paddingHorizontal: 18,
      paddingVertical: 11,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: theme.border,
      backgroundColor: theme.panel,
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 12,
      flexWrap: 'wrap',
    },
    brandRow: { flexDirection: 'row', alignItems: 'center', gap: 11, minWidth: 170, flex: 1 },
    brandMark: {
      width: 40,
      height: 40,
      borderRadius: 12,
      backgroundColor: theme.accent,
      alignItems: 'center',
      justifyContent: 'center',
    },
    brandMarkText: { color: '#FFFFFF', fontSize: 27, fontWeight: '900' },
    brandText: { flex: 1 },
    brand: { color: theme.text, fontSize: 20, fontWeight: '900', letterSpacing: -0.5 },
    connectionRow: { flexDirection: 'row', alignItems: 'center', gap: 5 },
    connectionDot: { width: 7, height: 7, borderRadius: 4 },
    serverName: { color: theme.textMuted, fontSize: 10, flexShrink: 1 },
    headerActions: { flexDirection: 'row', alignItems: 'center', gap: 5 },
    errorBanner: {
      paddingHorizontal: 18,
      paddingVertical: 10,
      backgroundColor: theme.dangerSoft,
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 12,
    },
    errorText: { color: theme.danger, fontSize: 13, lineHeight: 18, flex: 1 },
    retryText: { color: theme.danger, fontWeight: '800', fontSize: 13 },
    notice: { paddingHorizontal: 18, paddingVertical: 10, backgroundColor: theme.accentSoft },
    noticeText: { color: theme.accent, fontSize: 13, lineHeight: 18 },
    guardBanner: { paddingHorizontal: 18, paddingVertical: 9, backgroundColor: theme.dangerSoft },
    guardText: { color: theme.danger, fontSize: 12, lineHeight: 17, fontWeight: '600' },
    tabs: {
      paddingHorizontal: 14,
      paddingVertical: 8,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: theme.border,
      backgroundColor: theme.panel,
      flexDirection: 'row',
      justifyContent: 'center',
      gap: 6,
    },
    tab: {
      minWidth: 88,
      minHeight: 36,
      paddingHorizontal: 14,
      borderRadius: 10,
      alignItems: 'center',
      justifyContent: 'center',
    },
    tabSelected: { backgroundColor: theme.accentSoft },
    tabText: { color: theme.textMuted, fontSize: 12, fontWeight: '800' },
    tabTextSelected: { color: theme.accent },
    content: { padding: 14, paddingBottom: 40, gap: 14 },
    contentWide: { padding: 24, maxWidth: 1300, width: '100%', alignSelf: 'center' },
    dashboard: { gap: 14 },
    dashboardWide: { flexDirection: 'row', alignItems: 'flex-start', gap: 20 },
    queueColumn: { flex: 1, minWidth: 0 },
    sidebar: { width: 278, gap: 14 },
    metrics: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
    metricsVertical: { flexDirection: 'column' },
    metric: {
      minWidth: 140,
      flex: 1,
      minHeight: 86,
      padding: 14,
      borderRadius: 15,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      justifyContent: 'space-between',
    },
    metricLabel: { color: theme.textMuted, fontSize: 11, fontWeight: '700', letterSpacing: 0.4 },
    metricValue: { color: theme.text, fontSize: 24, fontWeight: '800', letterSpacing: -0.5 },
    sectionHeading: {
      flexDirection: 'row',
      alignItems: 'flex-end',
      justifyContent: 'space-between',
      marginBottom: 10,
      paddingHorizontal: 2,
    },
    eyebrow: { color: theme.textMuted, fontSize: 10, fontWeight: '800', letterSpacing: 1.3 },
    sectionTitle: { color: theme.text, fontSize: 23, fontWeight: '800', letterSpacing: -0.4 },
    updated: { color: theme.textMuted, fontSize: 10 },
    empty: {
      minHeight: 230,
      padding: 28,
      borderRadius: 18,
      borderWidth: 1,
      borderStyle: 'dashed',
      borderColor: theme.border,
      alignItems: 'center',
      justifyContent: 'center',
      gap: 12,
      backgroundColor: theme.panel,
    },
    emptyTitle: { color: theme.text, fontSize: 20, fontWeight: '800' },
    emptyText: { color: theme.textMuted, fontSize: 14, lineHeight: 20, maxWidth: 400, textAlign: 'center' },
    jobList: { gap: 9 },
    jobCard: {
      borderRadius: 16,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      overflow: 'hidden',
    },
    jobCardExpanded: { borderColor: theme.accent },
    jobMain: { padding: 14, gap: 10 },
    pressed: { opacity: 0.72 },
    jobTopline: { flexDirection: 'row', alignItems: 'flex-start', gap: 12 },
    jobName: { color: theme.text, fontSize: 16, fontWeight: '700', flex: 1, lineHeight: 21 },
    jobPercent: { color: theme.text, fontSize: 15, fontWeight: '800', fontVariant: ['tabular-nums'] },
    progressTrack: { height: 5, borderRadius: 3, backgroundColor: theme.panelAlt, overflow: 'hidden' },
    progressFill: { height: 5, borderRadius: 3, backgroundColor: theme.accent },
    jobMeta: { flexDirection: 'row', flexWrap: 'wrap', alignItems: 'center', gap: 9 },
    status: { color: theme.accent, fontSize: 11, fontWeight: '800', textTransform: 'uppercase' },
    statusFailed: { color: theme.danger },
    metaText: { color: theme.textMuted, fontSize: 11, fontVariant: ['tabular-nums'] },
    jobActions: { padding: 13, paddingTop: 0, gap: 12 },
    jobFacts: {
      flexDirection: 'row',
      flexWrap: 'wrap',
      borderRadius: 10,
      backgroundColor: theme.panelAlt,
      padding: 9,
      gap: 12,
    },
    fact: { minWidth: 82, flex: 1 },
    factLabel: { color: theme.textMuted, fontSize: 9, fontWeight: '800', textTransform: 'uppercase' },
    factValue: { color: theme.text, fontSize: 13, fontWeight: '600', marginTop: 2 },
    actionRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 6 },
    serverPanel: {
      borderRadius: 15,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      padding: 14,
      gap: 9,
    },
    serverRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 10 },
    serverIdentity: { flexDirection: 'row', alignItems: 'center', gap: 8, minWidth: 0, flex: 1 },
    serverDot: { width: 7, height: 7, borderRadius: 4, backgroundColor: theme.success },
    serverDotBlocked: { backgroundColor: theme.danger },
    providerName: { color: theme.text, fontSize: 13, fontWeight: '600', flex: 1 },
    providerRate: { color: theme.textMuted, fontSize: 11, fontVariant: ['tabular-nums'] },
    modalBackdrop: { flex: 1, justifyContent: 'flex-end', backgroundColor: theme.overlay },
    modalCard: {
      width: '100%',
      maxWidth: 680,
      maxHeight: '94%',
      alignSelf: 'center',
      padding: 20,
      borderTopLeftRadius: 24,
      borderTopRightRadius: 24,
      backgroundColor: theme.panel,
      gap: 12,
    },
    modalHandle: { width: 38, height: 4, borderRadius: 2, backgroundColor: theme.border, alignSelf: 'center' },
    modalHeading: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' },
    modalTitle: { color: theme.text, fontSize: 25, fontWeight: '800', letterSpacing: -0.4 },
    filePicker: {
      minHeight: 88,
      padding: 16,
      borderRadius: 14,
      borderWidth: 1,
      borderStyle: 'dashed',
      borderColor: theme.accent,
      backgroundColor: theme.accentSoft,
      justifyContent: 'center',
      gap: 4,
    },
    filePickerTitle: { color: theme.accent, fontSize: 15, fontWeight: '800' },
    filePickerMeta: { color: theme.textMuted, fontSize: 11, lineHeight: 16 },
    inputLabel: { color: theme.text, fontSize: 12, fontWeight: '700', marginBottom: -7 },
    input: {
      minHeight: 46,
      paddingHorizontal: 13,
      borderRadius: 11,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panelAlt,
      color: theme.text,
      fontSize: 15,
    },
    priorityRow: { flexDirection: 'row', gap: 6 },
    priorityChip: {
      flex: 1,
      minHeight: 39,
      paddingHorizontal: 6,
      borderRadius: 9,
      borderWidth: 1,
      borderColor: theme.border,
      alignItems: 'center',
      justifyContent: 'center',
    },
    priorityChipSelected: { backgroundColor: theme.accent, borderColor: theme.accent },
    priorityChipText: { color: theme.text, fontSize: 11, fontWeight: '700' },
    priorityChipTextSelected: { color: '#FFFFFF' },
    switchRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 14 },
    switchText: { flex: 1 },
    switchTitle: { color: theme.text, fontSize: 14, fontWeight: '700' },
    switchHint: { color: theme.textMuted, fontSize: 11, marginTop: 2 },
    modalError: { borderRadius: 10, padding: 10, backgroundColor: theme.dangerSoft },
  });
