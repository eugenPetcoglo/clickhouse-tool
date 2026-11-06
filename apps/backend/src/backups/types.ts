import type { Readable } from 'node:stream';
import type { DatabaseSchema } from '../types.js';

export interface BackupFile {
  path: string;
  size: number;
  sha256: string;
}

export interface Manifest {
  formatVersion: 1;
  id: string;
  createdAt: string;
  finishedAt: string;
  source: { database: string; serverVersion: string; clientVersion: string };
  schema: DatabaseSchema;
  files: BackupFile[];
  tableExports: Array<{ objectId: string; startedAt: string; finishedAt: string }>;
}

export interface BackupSummary {
  id: string;
  database: string;
  createdAt: string;
  finishedAt: string;
  serverVersion: string;
  clientVersion: string;
  bytes: number;
  path: string;
  status: 'ready' | 'invalid';
  error?: string;
}

export interface CheckedBackup {
  manifest: Manifest;
  schema: DatabaseSchema;
  path: string;
  readData(objectId: string): Promise<Readable>;
  close(): Promise<void>;
}
