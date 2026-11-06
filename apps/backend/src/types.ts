import type { Readable, Writable } from 'node:stream';

export interface SshSettings {
  host: string;
  port: number;
  username: string;
  identityFile?: string;
}

export interface ConnectionProfile {
  id: string;
  name: string;
  host: string;
  port: number;
  username: string;
  tls: boolean;
  tlsServerName?: string;
  ssh?: SshSettings;
}

export interface ConnectedConnection {
  profile: ConnectionProfile;
  password: string;
  host: string;
  port: number;
  serverVersion: string;
  serverId: string;
}

export interface ColumnInfo {
  name: string;
  type: string;
  position: number;
  defaultKind: string;
  defaultExpression: string;
}

export type ObjectKind = 'table' | 'view' | 'materialized-view';

export interface SchemaObject {
  id: string;
  name: string;
  kind: ObjectKind;
  engine: string;
  ddl: string;
  columns: ColumnInfo[];
  dependencies: string[];
  storagePolicy: string;
  targetTable?: string;
}

export interface DatabaseSchema {
  name: string;
  engine: string;
  ddl: string;
  objects: SchemaObject[];
  fingerprint: string;
}

export interface ChangeRecord {
  action: string;
  object?: string;
  state: 'started' | 'completed';
  at: string;
}

export interface OperationContext {
  signal: AbortSignal;
  setPhase(phase: string, table?: string): Promise<void>;
  addBytes(bytes: number): void;
  setQueryId(queryId: string): Promise<void>;
  recordChange(action: string, object?: string, completed?: boolean): Promise<void>;
  completeObject?(name: string): Promise<void>;
}

export interface QueryOptions {
  signal?: AbortSignal;
  context?: OperationContext;
  parameters?: Record<string, string>;
}

export interface ClickHouseClient {
  version(): Promise<string>;
  query<T>(connection: ConnectedConnection, sql: string, options?: QueryOptions): Promise<T[]>;
  execute(connection: ConnectedConnection, sql: string, options?: QueryOptions): Promise<void>;
  exportTable(connection: ConnectedConnection, database: string, table: SchemaObject, output: Writable, context: OperationContext): Promise<void>;
  importTable(connection: ConnectedConnection, database: string, table: SchemaObject, input: Readable, context: OperationContext): Promise<void>;
  cancelQuery(connection: ConnectedConnection, queryId: string): Promise<void>;
}

export type RestoreMode = 'replace-data' | 'create-new';
