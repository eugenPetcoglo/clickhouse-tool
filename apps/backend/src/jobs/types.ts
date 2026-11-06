import type { ChangeRecord } from '../types.js';

export type JobKind = 'backup' | 'import' | 'restore-check' | 'restore';
export type JobStatus = 'running' | 'cancelling' | 'succeeded' | 'failed' | 'cancelled' | 'interrupted';

export interface Job {
  id: string;
  kind: JobKind;
  status: JobStatus;
  phase: string;
  createdAt: string;
  updatedAt: string;
  connectionId?: string;
  table?: string;
  bytes: number;
  completedObjects: string[];
  queryId?: string;
  requiresRecovery: boolean;
  changes: ChangeRecord[];
  input: Record<string, unknown>;
  result?: unknown;
  error?: { code: string; message: string };
}
