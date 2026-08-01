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
  criticalStorage,
  formatBytes,
  formatDuration,
  isJobPaused,
  jobEta,
  jobProgress,
  jobStatusKey,
  jobStatusLabel,
  storageUsage,
} from '../api/format';
import {
  AddNzbOptions,
  ConnectionConfig,
  ConnectionState,
  JobSummary,
  StoragePath,
  StatusDto,
} from '../api/types';
import { ActionButton } from '../components/ActionButton';
import { ThemeSwitcher } from '../components/ThemeSwitcher';
import { useNzbd } from '../hooks/useNzbd';
import { QueueSectionKey, sectionQueueJobs } from '../queueSections';
import { DOWNLOAD_PRIORITIES, downloadPriorityLabel } from '../priority';
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
    setJobPriority,
    addNzb,
  } = useNzbd(config);

  const status = snapshot?.status;
  const jobs = snapshot?.jobs ?? [];
  const jobSections = useMemo(() => sectionQueueJobs(jobs), [jobs]);
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

  const mutatePriority = async (job: JobSummary, priority: number) => {
    if (priority === job.priority) return;
    try {
      await setJobPriority(job.id, priority);
      setNotice(`${job.name} is now ${downloadPriorityLabel(priority).toLowerCase()} priority.`);
    } catch {
      // The hook owns the visible error banner.
    }
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
          <ThemeSwitcher compact />
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

      {activeSection === 'queue' && status ? (
        <View style={styles.overviewDock}>
          <Overview status={status} styles={styles} wide={wide} />
        </View>
      ) : null}

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
          style={styles.queueScroll}
        >
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
                  {jobSections.map(({ definition, jobs: sectionJobs }) => {
                    const tone = queueSectionTone(definition.key, theme);
                    return (
                      <View key={definition.key} style={styles.queueGroup}>
                        <View accessibilityRole="header" style={styles.queueGroupHeading}>
                          <View style={[styles.queueGroupAccent, { backgroundColor: tone.accent }]} />
                          <Text style={styles.queueGroupTitle}>{definition.label}</Text>
                          <Text style={styles.queueGroupCount}>{sectionJobs.length}</Text>
                        </View>
                        <View style={styles.queueGroupJobs}>
                          {sectionJobs.map(({ job, index }) => (
                            <JobCard
                              busy={busyKey !== null}
                              expanded={expanded === job.id}
                              index={index}
                              job={job}
                              key={job.id}
                              movable={definition.ordered}
                              onAction={(action) => void mutateJob(job, action)}
                              onDelete={() => confirmDelete(job)}
                              onPriorityChange={(priority) => void mutatePriority(job, priority)}
                              onToggle={() => setExpanded(expanded === job.id ? null : job.id)}
                              sectionKey={definition.key}
                              sectionLabel={definition.label}
                              styles={styles}
                              tone={tone}
                              total={jobs.length}
                            />
                          ))}
                        </View>
                      </View>
                    );
                  })}
                </View>
              )}
            </View>

            {wide && status ? (
              <View style={styles.sidebar}>
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
  wide,
}: {
  status: StatusDto;
  styles: ReturnType<typeof makeStyles>;
  wide: boolean;
}) {
  const eta = status.download_paused
    ? 'paused'
    : status.download_rate_bps > 0
      ? formatDuration(status.remaining_bytes / status.download_rate_bps)
      : '—';
  const volumes = status.storage ?? [];
  const critical = criticalStorage(volumes);
  const criticalUsage = critical ? storageUsage(critical) : null;
  return (
    <View style={styles.overviewBlocks}>
      <View style={styles.queueSummaryBlock}>
        <Text style={styles.overviewBlockTitle}>Queue</Text>
        <View style={[styles.queueSummaryGrid, wide && styles.queueSummaryGridWide]}>
          <SummaryMetric
            label="Speed"
            styles={styles}
            value={formatBytes(status.download_rate_bps, '/s')}
            wide={wide}
          />
          <SummaryMetric
            label="Remaining"
            styles={styles}
            value={formatBytes(status.remaining_bytes)}
            wide={wide}
          />
          <SummaryMetric label="Time left" styles={styles} value={eta} wide={wide} />
          <SummaryMetric
            label="Active / queued"
            styles={styles}
            value={`${status.jobs_downloading} / ${status.jobs_queued}`}
            wide={wide}
          />
        </View>
      </View>

      <View style={styles.storageBlock}>
        <View style={styles.storageBlockHeading}>
          <Text style={styles.overviewBlockTitle}>Storage volumes</Text>
          <Text numberOfLines={1} style={styles.storageCritical}>
            {criticalUsage && critical ? `critical: ${critical.label}` : 'measuring volumes…'}
          </Text>
        </View>
        {volumes.length > 0 ? (
          <View style={styles.storageList}>
            {volumes.map((volume, index) => (
              <StorageVolume
                critical={criticalUsage !== null && volume === critical}
                key={`${volume.path}:${index}`}
                storage={volume}
                styles={styles}
                wide={wide}
              />
            ))}
          </View>
        ) : (
          <Text style={styles.storageEmpty}>Waiting for filesystem capacity readings.</Text>
        )}
      </View>
    </View>
  );
}

function SummaryMetric({
  label,
  value,
  styles,
  wide,
}: {
  label: string;
  value: string;
  styles: ReturnType<typeof makeStyles>;
  wide: boolean;
}) {
  return (
    <View style={[styles.summaryMetric, wide && styles.summaryMetricWide]}>
      <Text style={styles.summaryMetricLabel}>{label}</Text>
      <Text adjustsFontSizeToFit numberOfLines={1} style={styles.summaryMetricValue}>
        {value}
      </Text>
    </View>
  );
}

function StorageVolume({
  storage,
  critical,
  styles,
  wide,
}: {
  storage: StoragePath;
  critical: boolean;
  styles: ReturnType<typeof makeStyles>;
  wide: boolean;
}) {
  const usage = storageUsage(storage);
  const tone = usage
    ? usage.usedPercent >= 95
      ? 'danger'
      : usage.usedPercent >= 85
        ? 'warning'
        : undefined
    : undefined;
  const percent = usage ? Math.round(usage.usedPercent) : null;
  const fillWidth = `${usage?.usedPercent ?? 0}%` as `${number}%`;
  const capacity = usage
    ? `${formatBytes(usage.usedBytes)} used / ${formatBytes(usage.totalBytes)} total`
    : 'capacity unavailable';

  return (
    <View
      accessibilityLabel={`${storage.label}, ${storage.path}, ${percent === null ? 'capacity unavailable' : `${percent} percent used, ${capacity}`}`}
      style={[
        styles.storageRow,
        wide ? styles.storageRowWide : styles.storageRowPhone,
        critical && styles.storageRowCritical,
      ]}
    >
      <View style={styles.storageTop}>
        <Text numberOfLines={1} style={styles.storageLabel}>
          {storage.label || 'volume'}
        </Text>
        <Text
          style={[
            styles.storagePercent,
            tone === 'warning' && styles.storageTextWarning,
            tone === 'danger' && styles.storageTextDanger,
          ]}
        >
          {percent === null ? 'measuring…' : `${percent}%`}
        </Text>
      </View>
      <Text numberOfLines={1} style={styles.storagePath}>
        {storage.path || '—'}
      </Text>
      <View
        accessibilityRole="progressbar"
        accessibilityValue={percent === null ? undefined : { min: 0, max: 100, now: percent }}
        style={styles.storageTrack}
      >
        <View
          style={[
            styles.storageFill,
            { width: fillWidth },
            tone === 'warning' && styles.storageFillWarning,
            tone === 'danger' && styles.storageFillDanger,
          ]}
        />
      </View>
      <Text numberOfLines={1} style={styles.storageCapacity}>
        {capacity}
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
  onPriorityChange,
  movable,
  sectionKey,
  sectionLabel,
  styles,
  tone,
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
  onPriorityChange: (priority: number) => void;
  movable: boolean;
  sectionKey: QueueSectionKey;
  sectionLabel: string;
  styles: ReturnType<typeof makeStyles>;
  tone: QueueSectionTone;
}) {
  const progress = jobProgress(job);
  const statusKey = jobStatusKey(job.status);
  const canPause = ['queued', 'downloading', 'fetching'].includes(statusKey);
  const canResume = isJobPaused(job.status);
  const postProcessing = !['downloading', 'fetching', 'waiting'].includes(sectionKey);
  const showStatus = sectionKey === 'waiting';
  return (
    <View
      style={[
        styles.jobCard,
        styles.jobCardStage,
        { backgroundColor: tone.background, borderLeftColor: tone.accent },
        expanded && styles.jobCardExpanded,
      ]}
    >
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
          {!postProcessing ? (
            <Text style={styles.jobPercent}>{Math.floor(progress * 100)}%</Text>
          ) : null}
        </View>
        {!postProcessing ? (
          <View style={styles.progressTrack}>
            <View style={[styles.progressFill, { width: `${Math.max(progress * 100, 1)}%` }]} />
          </View>
        ) : null}
        <View style={styles.jobMeta}>
          {showStatus ? (
            <Text style={[styles.status, statusKey === 'failed' && styles.statusFailed]}>
              {jobStatusLabel(job.status)}
            </Text>
          ) : null}
          <Text style={styles.metaText}>
            {formatBytes(job.downloaded_bytes)} / {formatBytes(job.size_bytes)}
          </Text>
          {postProcessing ? (
            <Text style={[styles.metaText, { color: tone.accent }]}>
              {postProcessingDetail(job, sectionKey, sectionLabel)}
            </Text>
          ) : (
            <>
              <Text style={styles.metaText}>{formatBytes(job.rate_bps, '/s')}</Text>
              <Text style={styles.metaText}>ETA {jobEta(job)}</Text>
            </>
          )}
        </View>
      </Pressable>

      {expanded ? (
        <View style={styles.jobActions}>
          <View style={styles.jobFacts}>
            <Fact label="Health" styles={styles} value={`${(job.health / 10).toFixed(1)}%`} />
            <Fact label="Files" styles={styles} value={`${job.files_done}/${job.files_total}`} />
            <Fact label="Priority" styles={styles} value={downloadPriorityLabel(job.priority)} />
            <Fact label="Category" styles={styles} value={job.category || '—'} />
          </View>
          <View style={styles.jobPriorityEditor}>
            <Text style={styles.inputLabel}>Download priority</Text>
            <View style={styles.priorityRow}>
              {DOWNLOAD_PRIORITIES.map(({ value, label }) => (
                <PriorityChip
                  busy={busy}
                  key={value}
                  label={label}
                  onPress={() => onPriorityChange(value)}
                  selected={job.priority === value}
                  styles={styles}
                />
              ))}
            </View>
            {job.priority >= 900 ? (
              <Text style={styles.priorityHint}>Force can download through queue pauses and quota holds.</Text>
            ) : null}
          </View>
          <View style={styles.actionRow}>
            {canPause ? (
              <ActionButton compact disabled={busy} label="Pause" onPress={() => onAction('pause')} />
            ) : null}
            {canResume ? (
              <ActionButton compact disabled={busy} label="Resume" onPress={() => onAction('resume')} />
            ) : null}
            {movable ? (
              <>
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
              </>
            ) : null}
            <ActionButton compact disabled={busy} label="Remove" onPress={onDelete} variant="danger" />
          </View>
        </View>
      ) : null}
    </View>
  );
}

interface QueueSectionTone {
  accent: string;
  background: string;
}

const QUEUE_STAGE_ACCENTS: Partial<Record<QueueSectionKey, string>> = {
  fetching: '#42A5D5',
  renaming: '#A57BD8',
  verifying: '#38A9AD',
  repairing: '#DC7844',
  extracting: '#C86FA7',
  cleaning: '#6B9F74',
  moving: '#5B86D9',
  scripting: '#8D72D8',
};

function queueSectionTone(section: QueueSectionKey, theme: Theme): QueueSectionTone {
  if (section === 'waiting') return { accent: theme.textMuted, background: theme.panel };
  const accent =
    section === 'downloading'
      ? theme.success
      : section === 'post_queued'
        ? theme.warning
        : QUEUE_STAGE_ACCENTS[section] ?? theme.accent;
  return {
    accent,
    background: colorWithAlpha(accent, theme.dark ? 0.1 : 0.055),
  };
}

function colorWithAlpha(color: string, alpha: number): string {
  const match = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(color);
  if (!match) return color;
  const [, red, green, blue] = match;
  return `rgba(${Number.parseInt(red, 16)}, ${Number.parseInt(green, 16)}, ${Number.parseInt(blue, 16)}, ${alpha})`;
}

function postProcessingDetail(
  job: JobSummary,
  section: QueueSectionKey,
  sectionLabel: string,
): string {
  if (section === 'post_queued') return 'waiting for a post-processing slot';
  const currentStage = [...(job.stages ?? [])].reverse().find((stage) => stage.ms == null);
  if (!currentStage) return sectionLabel.toLowerCase();
  const elapsedSeconds = Math.max(0, Date.now() / 1000 - currentStage.started_at_unix);
  return `${formatDuration(elapsedSeconds)} in ${sectionLabel.toLowerCase()}`;
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
            {DOWNLOAD_PRIORITIES.map(({ value, label }) => (
              <PriorityChip
                busy={busy}
                key={value}
                label={label}
                onPress={() => setPriority(value)}
                selected={priority === value}
                styles={styles}
              />
            ))}
          </View>
          {priority >= 900 ? (
            <Text style={styles.priorityHint}>Force can download through queue pauses and quota holds.</Text>
          ) : null}

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

function PriorityChip({
  busy,
  label,
  onPress,
  selected,
  styles,
}: {
  busy: boolean;
  label: string;
  onPress: () => void;
  selected: boolean;
  styles: ReturnType<typeof makeStyles>;
}) {
  return (
    <Pressable
      accessibilityRole="button"
      accessibilityState={{ selected }}
      disabled={busy}
      onPress={onPress}
      style={[styles.priorityChip, selected && styles.priorityChipSelected]}
    >
      <Text style={[styles.priorityChipText, selected && styles.priorityChipTextSelected]}>
        {label}
      </Text>
    </Pressable>
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
    overviewDock: {
      paddingHorizontal: 8,
      paddingVertical: 7,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: theme.border,
      backgroundColor: theme.background,
    },
    queueScroll: { flex: 1 },
    content: { padding: 14, paddingBottom: 40, gap: 14 },
    contentWide: { padding: 24, maxWidth: 1300, width: '100%', alignSelf: 'center' },
    dashboard: { gap: 14 },
    dashboardWide: { flexDirection: 'row', alignItems: 'flex-start', gap: 20 },
    queueColumn: { flex: 1, minWidth: 0 },
    sidebar: { width: 278, gap: 14 },
    overviewBlocks: {
      width: '100%',
      maxWidth: 1300,
      alignSelf: 'center',
      flexDirection: 'row',
      alignItems: 'stretch',
      gap: 6,
    },
    queueSummaryBlock: {
      minWidth: 0,
      flexBasis: 0,
      flexGrow: 0.72,
      flexShrink: 1,
      padding: 6,
      borderRadius: 10,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
    },
    overviewBlockTitle: {
      color: theme.text,
      fontSize: 9,
      fontWeight: '900',
      letterSpacing: 0.7,
      textTransform: 'uppercase',
    },
    queueSummaryGrid: {
      flex: 1,
      marginTop: 4,
      flexDirection: 'row',
      flexWrap: 'wrap',
      alignContent: 'stretch',
      gap: 4,
    },
    queueSummaryGridWide: { flex: 0, alignContent: 'flex-start' },
    summaryMetric: {
      minWidth: 46,
      flexBasis: '45%',
      flexGrow: 1,
      paddingHorizontal: 5,
      paddingVertical: 4,
      borderRadius: 6,
      backgroundColor: theme.panelAlt,
      justifyContent: 'center',
    },
    summaryMetricWide: { flexBasis: 46 },
    summaryMetricLabel: {
      color: theme.textMuted,
      fontSize: 7,
      fontWeight: '800',
      letterSpacing: 0.35,
      textTransform: 'uppercase',
    },
    summaryMetricValue: {
      color: theme.text,
      marginTop: 1,
      fontSize: 13,
      fontWeight: '800',
      letterSpacing: -0.2,
      fontVariant: ['tabular-nums'],
    },
    storageBlock: {
      minWidth: 0,
      flexBasis: 0,
      flexGrow: 1.28,
      flexShrink: 1,
      paddingHorizontal: 7,
      paddingVertical: 6,
      borderRadius: 10,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
    },
    storageBlockHeading: {
      flexDirection: 'row',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 6,
      marginBottom: 4,
    },
    storageCritical: { color: theme.textMuted, fontSize: 7, flexShrink: 1 },
    storageList: { flexDirection: 'row', flexWrap: 'wrap', gap: 5 },
    storageRow: {
      minWidth: 0,
      maxWidth: '100%',
      flexShrink: 1,
      paddingHorizontal: 6,
      paddingVertical: 4,
      borderRadius: 7,
      borderWidth: 1,
      borderColor: 'transparent',
      backgroundColor: theme.panelAlt,
    },
    storageRowPhone: { width: '100%' },
    storageRowWide: { minWidth: 150, flexBasis: 170, flexGrow: 1 },
    storageRowCritical: { borderColor: theme.accent },
    storageTop: { flexDirection: 'row', alignItems: 'center', gap: 5 },
    storageLabel: {
      color: theme.text,
      fontSize: 8,
      fontWeight: '800',
      textTransform: 'uppercase',
      flex: 1,
      minWidth: 0,
    },
    storagePercent: {
      color: theme.accent,
      fontSize: 9,
      fontWeight: '900',
      fontVariant: ['tabular-nums'],
    },
    storageTextWarning: { color: theme.warning },
    storageTextDanger: { color: theme.danger },
    storagePath: { color: theme.textMuted, fontSize: 7, marginTop: 1, minWidth: 0 },
    storageTrack: {
      height: 4,
      marginTop: 3,
      overflow: 'hidden',
      borderRadius: 99,
      backgroundColor: theme.border,
    },
    storageFill: { height: '100%', borderRadius: 99, backgroundColor: theme.accent },
    storageFillWarning: { backgroundColor: theme.warning },
    storageFillDanger: { backgroundColor: theme.danger },
    storageCapacity: {
      color: theme.textMuted,
      fontSize: 8,
      lineHeight: 10,
      marginTop: 2,
      fontVariant: ['tabular-nums'],
    },
    storageEmpty: { color: theme.textMuted, fontSize: 9, lineHeight: 12 },
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
    jobList: { gap: 14 },
    queueGroup: { gap: 6 },
    queueGroupHeading: {
      minHeight: 20,
      paddingHorizontal: 4,
      flexDirection: 'row',
      alignItems: 'center',
      gap: 6,
    },
    queueGroupAccent: { width: 3, height: 13, borderRadius: 2 },
    queueGroupTitle: {
      color: theme.textMuted,
      fontSize: 10,
      fontWeight: '900',
      letterSpacing: 1,
      textTransform: 'uppercase',
    },
    queueGroupCount: {
      color: theme.textMuted,
      fontSize: 10,
      fontWeight: '700',
      fontVariant: ['tabular-nums'],
    },
    queueGroupJobs: { gap: 9 },
    jobCard: {
      borderRadius: 16,
      borderWidth: 1,
      borderColor: theme.border,
      backgroundColor: theme.panel,
      overflow: 'hidden',
    },
    jobCardStage: { borderLeftWidth: 3 },
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
    jobPriorityEditor: { gap: 8 },
    priorityRow: { flexDirection: 'row', flexWrap: 'wrap', gap: 6 },
    priorityChip: {
      flexGrow: 1,
      flexBasis: '30%',
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
    priorityHint: { color: theme.textMuted, fontSize: 11, lineHeight: 16 },
    switchRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', gap: 14 },
    switchText: { flex: 1 },
    switchTitle: { color: theme.text, fontSize: 14, fontWeight: '700' },
    switchHint: { color: theme.textMuted, fontSize: 11, marginTop: 2 },
    modalError: { borderRadius: 10, padding: 10, backgroundColor: theme.dangerSoft },
  });
