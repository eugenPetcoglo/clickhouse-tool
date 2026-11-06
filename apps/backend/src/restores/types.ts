import type { ChangeRecord, ClickHouseClient, ConnectedConnection, RestoreMode } from '../types.js';

/** Only the job store may supply this value. It must never come directly from HTTP JSON. */
export interface RestoreRecovery {
  backupId: string;
  targetServerId: string;
  database: string;
  backupFingerprint: string;
  changes: ChangeRecord[];
}

export interface RestoreInput {
  client: ClickHouseClient;
  target: ConnectedConnection;
  dataDir: string;
  backupId: string;
  mode: RestoreMode;
  recovery?: RestoreRecovery;
  expectedCheck?: RestoreCheck;
}

export interface RestoreCheck {
  backupId: string;
  database: string;
  mode: RestoreMode;
  targetConnectionId: string;
  targetServerId: string;
  targetServerVersion: string;
  sourceServerVersion: string;
  backupFingerprint: string;
  targetFingerprint?: string;
  fingerprint: string;
  actions: string[];
  warnings: string[];
}

export interface RestoreResult {
  database: string;
  mode: RestoreMode;
  tablesRestored: number;
  viewsRestored: number;
}
