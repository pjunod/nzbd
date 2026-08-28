import { JobStatus, JobSummary, PostStage, StageSpan } from './api/types';

export type QueueSectionKey =
  | 'downloading'
  | 'fetching'
  | 'post_queued'
  | 'renaming'
  | 'verifying'
  | 'repairing'
  | 'extracting'
  | 'cleaning'
  | 'moving'
  | 'scripting'
  | 'waiting';

export interface QueueSectionDefinition {
  key: QueueSectionKey;
  label: string;
  ordered: boolean;
}

export interface SectionedJob {
  job: JobSummary;
  index: number;
}

export interface QueueJobSection {
  definition: QueueSectionDefinition;
  jobs: SectionedJob[];
}

export const QUEUE_SECTIONS: readonly QueueSectionDefinition[] = [
  { key: 'downloading', label: 'Downloading', ordered: true },
  { key: 'fetching', label: 'Fetching NZB', ordered: true },
  { key: 'post_queued', label: 'Waiting to post-process', ordered: false },
  { key: 'renaming', label: 'Renaming', ordered: false },
  { key: 'verifying', label: 'Checking integrity', ordered: false },
  { key: 'repairing', label: 'Repairing', ordered: false },
  { key: 'extracting', label: 'Extracting', ordered: false },
  { key: 'cleaning', label: 'Cleaning up', ordered: false },
  { key: 'moving', label: 'Moving', ordered: false },
  { key: 'scripting', label: 'Running scripts', ordered: false },
  { key: 'waiting', label: 'Waiting', ordered: true },
];

const POST_STAGE_SECTIONS: Record<PostStage, QueueSectionKey> = {
  par_rename: 'renaming',
  rar_rename: 'renaming',
  post_unpack_rename: 'renaming',
  par_verify: 'verifying',
  par_repair: 'repairing',
  unpack: 'extracting',
  cleanup: 'cleaning',
  move: 'moving',
  script: 'scripting',
};

export function currentPostStage(
  status: JobStatus,
  stages: readonly StageSpan[] = [],
): PostStage | null {
  if (typeof status !== 'string') {
    return status.post.stage;
  }
  const last = stages.length ? stages[stages.length - 1] : undefined;
  return last && last.ms == null ? last.stage : null;
}

export function queueSectionKey(
  status: JobStatus,
  stages: readonly StageSpan[] = [],
): QueueSectionKey {
  const stage = currentPostStage(status, stages);
  if (stage) return POST_STAGE_SECTIONS[stage] ?? 'post_queued';
  if (status === 'downloading' || status === 'fetching' || status === 'post_queued') {
    return status;
  }
  return 'waiting';
}

export function sectionQueueJobs(jobs: readonly JobSummary[]): QueueJobSection[] {
  const grouped = new Map<QueueSectionKey, SectionedJob[]>();
  jobs.forEach((job, index) => {
    const key = queueSectionKey(job.status, job.stages);
    const section = grouped.get(key);
    const entry = { job, index };
    if (section) section.push(entry);
    else grouped.set(key, [entry]);
  });

  return QUEUE_SECTIONS.flatMap((definition) => {
    const sectionJobs = grouped.get(definition.key);
    return sectionJobs ? [{ definition, jobs: sectionJobs }] : [];
  });
}
