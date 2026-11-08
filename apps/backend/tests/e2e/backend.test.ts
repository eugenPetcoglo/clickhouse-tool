import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { afterAll, afterEach, beforeAll, beforeEach, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import type { BackupSummary } from '../../src/backups/types.js';
import { createClickHouseClient } from '../../src/clickhouse/client.js';
import { loadConfig } from '../../src/config.js';
import type { Job } from '../../src/jobs/types.js';
import type { ClickHouseClient, ConnectedConnection, ConnectionProfile, OperationContext, RestoreMode, SchemaObject } from '../../src/types.js';
import { startTestServer, type TestServer } from './server.js';

let directory: string;
let sourceServer: TestServer;
let targetServer: TestServer;
let client: ClickHouseClient;
let source: ConnectedConnection;
let target: ConnectedConnection;
let app: Awaited<ReturnType<typeof createApp>>;
let baseUrl: string;
let database: string;
let sourceProfile: ConnectionProfile;
let targetProfile: ConnectionProfile;
let backup: BackupSummary;
let afterImport: ((context: OperationContext, table: SchemaObject) => Promise<void>) | undefined;
let afterExport: ((table: SchemaObject) => Promise<void>) | undefined;
let beforeExecute: ((sql: string) => Promise<void>) | undefined;

async function json<T>(method: string, path: string, body?: unknown, status = 200): Promise<T> {
  const response = await fetch(`${baseUrl}/api${path}`, {
    method,
    headers: { 'X-Clickhouse-Tool': '1', ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  expect(response.status, text).toBe(status);
  return text ? JSON.parse(text) as T : undefined as T;
}

async function waitForJob(id: string): Promise<Job> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const job = await json<Job>('GET', `/jobs/${id}`);
    if (!['running', 'cancelling'].includes(job.status)) return job;
    await delay(50);
  }
  throw new Error(`Job ${id} did not finish.`);
}

async function runJob(path: string, body: unknown): Promise<Job> {
  const { jobId } = await json<{ jobId: string }>('POST', path, body, 202);
  const job = await waitForJob(jobId);
  expect(job.status, job.error?.message).toBe('succeeded');
  return job;
}

async function connect(port: number): Promise<ConnectionProfile> {
  const profile = await json<ConnectionProfile>('POST', '/connections', { name: `Server ${port}`, host: '127.0.0.1', port }, 201);
  await json('POST', `/connections/${profile.id}/connect`, {});
  return profile;
}

async function restore(mode: RestoreMode, targetId = targetProfile.id, backupId = backup.id): Promise<void> {
  const check = await runJob('/restores/check', { backupId, targetConnectionId: targetId, mode });
  await runJob('/restores', { checkJobId: check.id, confirmationName: database, targetWritesPaused: true });
}

async function rows(connection: ConnectedConnection) {
  return client.query(connection, `SELECT id, value, computed FROM ${database}.raw ORDER BY id`);
}

async function total(connection: ConnectedConnection): Promise<number> {
  const result = await client.query<{ total: number | string }>(connection, `SELECT sumMerge(total) AS total FROM ${database}.agg`);
  return Number(result[0]!.total);
}

async function refreshStates(connection: ConnectedConnection): Promise<Record<string, string>> {
  const views = await client.query<{ view: string; status: string }>(connection,
    'SELECT view, status FROM system.view_refreshes WHERE database = {database:String} ORDER BY view',
    { parameters: { database } });
  expect(views.map(view => view.view)).toEqual(['append_mv', 'refresh_mv']);
  return Object.fromEntries(views.map(view => [view.view, view.status]));
}

async function expectSavedRefreshData(connection: ConnectedConnection): Promise<void> {
  expect(await client.query(connection, `SELECT id, value FROM ${database}.refreshed ORDER BY id`)).toEqual([{ id: 90, value: 900 }]);
  expect(await client.query(connection, `SELECT id, value FROM ${database}.appended ORDER BY id`)).toEqual([{ id: 80, value: 800 }]);
}

async function pendingBufferRows(connection: ConnectedConnection): Promise<Array<{ id: number }>> {
  return client.query(connection, `SELECT id FROM ${database}.buffered WHERE _table = 'buffered' ORDER BY id`);
}

async function expectRestoredBufferData(connection: ConnectedConnection): Promise<void> {
  const expected = [{ id: 1 }, { id: 2 }];
  expect(await client.query(connection, `SELECT id FROM ${database}.buffer_data ORDER BY id`)).toEqual(expected);
  expect(await client.query(connection, `SELECT id FROM ${database}.buffered ORDER BY id`)).toEqual(expected);
  expect(await pendingBufferRows(connection)).toEqual([]);
}

async function waitForRunningRefresh(): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if ((await refreshStates(target)).refresh_mv === 'Running') return;
    await delay(25);
  }
  throw new Error('The refresh did not start.');
}

const expectedRows = [{ id: 1, value: 7, computed: 999 }, { id: 2, value: 8, computed: 888 }];

beforeAll(async () => {
  directory = await mkdtemp(join(tmpdir(), 'clickhouse-tool-e2e-'));
  sourceServer = await startTestServer();
  targetServer = await startTestServer();
  client = createClickHouseClient(loadConfig({ DATA_DIR: join(directory, 'fixtures') }));
  function connection(port: number): ConnectedConnection {
    return { profile: { id: randomUUID(), name: 'Fixture setup', host: '127.0.0.1', port, username: 'default', tls: false }, host: '127.0.0.1', port, password: '', serverId: '', serverVersion: '' };
  }
  source = connection(sourceServer.port);
  target = connection(targetServer.port);
});

beforeEach(async () => {
  database = `tool_e2e_${randomUUID().replaceAll('-', '')}`;
  afterImport = undefined;
  afterExport = undefined;
  beforeExecute = undefined;
  app = await createApp(loadConfig({ DATA_DIR: join(directory, database) }), { client: {
    ...client,
    async execute(connection, sql, options) {
      await beforeExecute?.(sql);
      await client.execute(connection, sql, options);
    },
    async importTable(connection, database, table, input, context) {
      await client.importTable(connection, database, table, input, context);
      await afterImport?.(context, table);
    },
    async exportTable(connection, database, table, output, context) {
      await client.exportTable(connection, database, table, output, context);
      await afterExport?.(table);
    },
  } });
  baseUrl = await app.listen({ host: '127.0.0.1', port: 0 });
  sourceProfile = await connect(sourceServer.port);
  targetProfile = await connect(targetServer.port);
  await client.execute(source, `CREATE DATABASE ${database} ENGINE = Atomic`);
  await client.execute(source, `CREATE TABLE ${database}.raw (id UInt32, value UInt32 DEFAULT id + 1, computed UInt32 MATERIALIZED value * 10, alias_value UInt32 ALIAS computed + 1) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE TABLE ${database}.agg (total AggregateFunction(sum, UInt32)) ENGINE = AggregatingMergeTree ORDER BY tuple()`);
  await client.execute(source, `CREATE TABLE ${database}.empty (id UInt32) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.mv TO ${database}.agg AS SELECT sumState(raw_data.value) AS total FROM ${database}.raw AS raw_data`);
  await client.execute(source, `CREATE VIEW ${database}.v AS SELECT id, value FROM ${database}.raw`);
  await client.execute(source, `INSERT INTO ${database}.raw (id, value, computed) SETTINGS insert_allow_materialized_columns=1 VALUES (1, 7, 999), (2, 8, 888)`);
  await client.execute(source, `CREATE TABLE ${database}.refreshed (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE TABLE ${database}.appended (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.refresh_mv REFRESH EVERY 1 DAY TO ${database}.refreshed EMPTY AS
    SELECT id, value FROM (
      SELECT source_data.id AS id, source_data.value AS value FROM ${database}.v AS source_data
      INNER JOIN ${database}.raw AS raw_data ON source_data.id = raw_data.id
      UNION ALL SELECT id, value FROM ${database}.raw WHERE id = 0)
    WHERE sleepEachRow(0.01) = 0`);
  await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.append_mv REFRESH AFTER 1 DAY DEPENDS ON ${database}.refresh_mv APPEND TO ${database}.appended EMPTY AS SELECT id, value FROM ${database}.refreshed`);
  await client.execute(source, `SYSTEM STOP VIEW ${database}.refresh_mv`);
  await client.execute(source, `SYSTEM STOP VIEW ${database}.append_mv`);
  // Stored snapshots deliberately differ from the SELECT results, exposing an accidental refresh.
  await client.execute(source, `INSERT INTO ${database}.refreshed VALUES (90, 900)`);
  await client.execute(source, `INSERT INTO ${database}.appended VALUES (80, 800)`);
  await client.execute(source, `CREATE TABLE ${database}.buffer_data (id UInt32) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE TABLE ${database}.buffered AS ${database}.buffer_data ENGINE = Buffer('${database}', 'buffer_data', 1, 86400, 86400, 1000000, 1000000, 1000000000, 1000000000)`);
  await client.execute(source, `INSERT INTO ${database}.buffer_data VALUES (1)`);
  await client.execute(source, `INSERT INTO ${database}.buffered VALUES (2)`);
  backup = (await runJob('/backups', { sourceConnectionId: sourceProfile.id, database })).result as BackupSummary;
  expect(await client.query(source, `SELECT id FROM ${database}.buffer_data ORDER BY id`)).toEqual([{ id: 1 }]);
  expect(await pendingBufferRows(source)).toEqual([{ id: 2 }]);
  expect(await client.query(source, `SELECT id FROM ${database}.buffered ORDER BY id`)).toEqual([{ id: 1 }, { id: 2 }]);
});

afterEach(async () => {
  await app?.close();
  if (database) {
    await client.execute(source, `DROP DATABASE IF EXISTS ${database} SYNC`);
    await client.execute(target, `DROP DATABASE IF EXISTS ${database} SYNC`);
  }
});

afterAll(async () => {
  await Promise.all([sourceServer?.close(), targetServer?.close()]);
  if (directory) await rm(directory, { recursive: true, force: true });
});

it('backs up to a file and restores it to independent destinations after deleting the source profile', async () => {
  expect((await json<{ ready: boolean }>('GET', '/health')).ready).toBe(true);
  expect(await client.query(target, `SELECT name FROM system.databases WHERE name = '${database}'`)).toEqual([]);
  await json('POST', `/connections/${sourceProfile.id}/disconnect`, undefined, 204);
  await json('DELETE', `/connections/${sourceProfile.id}`, undefined, 204);

  const file = await fetch(`${baseUrl}/api/backups/${backup.id}/file`);
  expect(file.status).toBe(200);
  const form = new FormData();
  form.append('file', new Blob([await file.arrayBuffer()]), 'saved.chbackup');
  const uploaded = await fetch(`${baseUrl}/api/backups/import`, { method: 'POST', headers: { 'X-Clickhouse-Tool': '1' }, body: form });
  expect(uploaded.status).toBe(202);
  const imported = await uploaded.json() as { backup: BackupSummary };
  await restore('create-new', targetProfile.id, imported.backup.id);
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);
  expect(Object.values(await refreshStates(target))).not.toContain('Disabled');

  await client.execute(source, `DROP DATABASE ${database} SYNC`);
  const anotherTarget = await connect(sourceServer.port);
  await restore('create-new', anotherTarget.id, imported.backup.id);
  expect(await rows(source)).toEqual(expectedRows);
  expect(await total(source)).toBe(15);
  await expectSavedRefreshData(source);
  await expectRestoredBufferData(source);
});

it('replaces data, clears empty tables and restores aggregates without double filling', async () => {
  await restore('create-new');
  await client.execute(target, `INSERT INTO ${database}.raw (id, value) VALUES (99, 1000)`);
  await client.execute(target, `INSERT INTO ${database}.empty VALUES (99)`);
  await client.execute(target, `INSERT INTO ${database}.buffered VALUES (99)`);
  expect(await pendingBufferRows(target)).toEqual([{ id: 99 }]);
  await client.execute(target, `SYSTEM STOP VIEW ${database}.append_mv`);
  let checkedBufferBarrier = false;
  beforeExecute = async sql => {
    if (!sql.startsWith('TRUNCATE TABLE ')) return;
    expect(sql).not.toContain('.`buffered`');
    expect(await pendingBufferRows(target)).toEqual([]);
    checkedBufferBarrier = true;
  };
  await restore('replace-data');
  beforeExecute = undefined;
  expect(checkedBufferBarrier).toBe(true);
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  expect(await client.query(target, `SELECT * FROM ${database}.empty`)).toEqual([]);
  expect(await client.query(target, `SELECT id FROM ${database}.v ORDER BY id`)).toEqual([{ id: 1 }, { id: 2 }]);
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);
  await client.execute(target, `OPTIMIZE TABLE ${database}.buffered`);
  await expectRestoredBufferData(target);
  const states = await refreshStates(target);
  expect(states.refresh_mv).not.toBe('Disabled');
  expect(states.append_mv).toBe('Disabled');
  await client.execute(target, `SYSTEM REFRESH VIEW ${database}.refresh_mv`);
  await client.execute(target, `SYSTEM WAIT VIEW ${database}.refresh_mv`);
  expect(await client.query(target, `SELECT id, value FROM ${database}.refreshed ORDER BY id`)).toEqual([{ id: 1, value: 7 }, { id: 2, value: 8 }]);
  await expect(client.query(target, `SELECT id, value FROM ${database}.appended ORDER BY id`)).resolves.toEqual([{ id: 80, value: 800 }]);
  await client.execute(target, `INSERT INTO ${database}.raw (id, value) VALUES (3, 3)`);
  expect(await total(target)).toBe(18);
});

it('rejects damaged backups, unsupported schemas, Buffer flushes and schema mismatches before changing destination data', async () => {
  await restore('create-new');
  const original = await readFile(backup.path);
  await writeFile(backup.path, original.subarray(0, original.length - 50));
  const request = { backupId: backup.id, targetConnectionId: targetProfile.id, mode: 'replace-data' };
  const damaged = await json<{ jobId: string }>('POST', '/restores/check', request, 202);
  expect((await waitForJob(damaged.jobId)).status).toBe('failed');
  const blocked = await json<{ error: { code: string } }>('POST', '/restores', {
    checkJobId: damaged.jobId, confirmationName: database, targetWritesPaused: true,
  }, 409);
  expect(blocked.error.code).toBe('RESTORE_CHECK_REQUIRED');
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);

  await writeFile(backup.path, original);
  const versions = await client.query<{ version: string }>(source, 'SELECT version() AS version');
  const [major = 0, minor = 0] = versions[0]!.version.split('.').map(Number);
  // Older supported servers cannot create the cursor-based variant at all.
  if (major > 26 || (major === 26 && minor >= 9)) {
    await client.execute(source, `CREATE TABLE ${database}.cursor_raw (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1`);
    await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.cursor_mv REFRESH AFTER 1 DAY APPEND INCREMENTAL TO ${database}.appended EMPTY AS SELECT id, value FROM ${database}.cursor_raw`);
    const cursorBackup = await json<{ jobId: string }>('POST', '/backups', { sourceConnectionId: sourceProfile.id, database }, 202);
    expect((await waitForJob(cursorBackup.jobId)).error?.code).toBe('UNSUPPORTED_SCHEMA');
    await client.execute(source, `DROP TABLE ${database}.cursor_mv SYNC`);
    await client.execute(source, `DROP TABLE ${database}.cursor_raw SYNC`);
  }

  await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.hidden_mv ENGINE = MergeTree ORDER BY id AS SELECT id FROM ${database}.raw`);
  const hiddenBackup = await json<{ jobId: string }>('POST', '/backups', { sourceConnectionId: sourceProfile.id, database }, 202);
  expect((await waitForJob(hiddenBackup.jobId)).error?.code).toBe('UNSUPPORTED_SCHEMA');
  await client.execute(source, `DROP TABLE ${database}.hidden_mv SYNC`);

  await client.execute(source, `CREATE TABLE ${database}.shadow_buffer (id UInt32, _table String ALIAS 'shadow') ENGINE = Buffer('${database}', 'buffer_data', 1, 86400, 86400, 1000000, 1000000, 1000000000, 1000000000)`);
  const shadowBackup = await json<{ jobId: string }>('POST', '/backups', { sourceConnectionId: sourceProfile.id, database }, 202);
  expect((await waitForJob(shadowBackup.jobId)).error?.code).toBe('UNSUPPORTED_SCHEMA');
  await client.execute(source, `DROP TABLE ${database}.shadow_buffer SYNC`);

  afterExport = async table => {
    if (table.engine !== 'Buffer') return;
    afterExport = undefined;
    await client.execute(source, `OPTIMIZE TABLE ${database}.buffered`);
  };
  const flushingBackup = await json<{ jobId: string }>('POST', '/backups', { sourceConnectionId: sourceProfile.id, database }, 202);
  const flushed = await waitForJob(flushingBackup.jobId);
  expect(flushed.status).toBe('failed');
  expect(flushed.error?.code).toBe('SOURCE_BUFFER_FLUSHED');
  expect((await json<BackupSummary[]>('GET', '/backups')).map(item => item.id)).toEqual([backup.id]);
  expect(await client.query(source, `SELECT id FROM ${database}.buffer_data ORDER BY id`)).toEqual([{ id: 1 }, { id: 2 }]);
  expect(await pendingBufferRows(source)).toEqual([]);

  await client.execute(target, `ALTER TABLE ${database}.raw ADD COLUMN extra UInt8`);
  const mismatch = await json<{ jobId: string }>('POST', '/restores/check', request, 202);
  const failed = await waitForJob(mismatch.jobId);
  expect(failed.error?.code).toBe('SCHEMA_MISMATCH');
  expect(failed.changes).toEqual([]);
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);
});

it('keeps refreshes stopped after cancellation and failure, then recovers the original schedule state', async () => {
  afterImport = async (_context, table) => {
    if (table.engine === 'Buffer') throw new Error('Controlled buffer import failure');
  };
  const creationCheck = await runJob('/restores/check', { backupId: backup.id, targetConnectionId: targetProfile.id, mode: 'create-new' });
  const creation = await json<{ jobId: string }>('POST', '/restores', {
    checkJobId: creationCheck.id, confirmationName: database, targetWritesPaused: true,
  }, 202);
  const failedCreation = await waitForJob(creation.jobId);
  expect(failedCreation.status).toBe('failed');
  expect(failedCreation.error?.message).toContain('Controlled buffer import failure');
  expect(failedCreation.requiresRecovery).toBe(true);
  expect(await client.query(target,
    'SELECT name FROM system.tables WHERE database = {database:String} AND engine = \'MaterializedView\'',
    { parameters: { database } })).toEqual([]);

  afterImport = undefined;
  const creationRecovery = await runJob('/restores/check', {
    backupId: backup.id, targetConnectionId: targetProfile.id, mode: 'create-new', recoveryJobId: failedCreation.id,
  });
  await runJob('/restores', { checkJobId: creationRecovery.id, confirmationName: database, targetWritesPaused: true });
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);
  expect(Object.values(await refreshStates(target))).not.toContain('Disabled');
  await client.execute(target, `SYSTEM STOP VIEW ${database}.append_mv`);
  await client.execute(target, `INSERT INTO ${database}.raw (id, value) SELECT toUInt32(number + 100), toUInt32(number) FROM numbers(250)`);

  const request = { backupId: backup.id, targetConnectionId: targetProfile.id, mode: 'replace-data' };
  const check = await runJob('/restores/check', request);
  let importReached: () => void = () => {};
  const imported = new Promise<void>(resolve => { importReached = resolve; });
  afterImport = async context => {
    expect(Object.values(await refreshStates(target))).toEqual(['Disabled', 'Disabled']);
    importReached();
    await delay(60_000, undefined, { signal: context.signal });
  };
  let observedRunningRefresh = false;
  let checkedTruncations = 0;
  beforeExecute = async sql => {
    if (!observedRunningRefresh && sql.startsWith('SYSTEM STOP VIEW ') && sql.includes('refresh_mv')) {
      // Start just before STOP reaches the server, so a slow preflight cannot make this check vacuous.
      await client.execute(target, `SYSTEM REFRESH VIEW ${database}.refresh_mv`);
      await waitForRunningRefresh();
      observedRunningRefresh = true;
    }
    if (sql.startsWith('TRUNCATE TABLE ')) {
      expect(observedRunningRefresh).toBe(true);
      expect(Object.values(await refreshStates(target))).toEqual(['Disabled', 'Disabled']);
      checkedTruncations++;
    }
  };
  const { jobId } = await json<{ jobId: string }>('POST', '/restores', {
    checkJobId: check.id, confirmationName: database, targetWritesPaused: true,
  }, 202);
  await Promise.race([imported, waitForJob(jobId).then(job => { throw new Error(job.error?.message ?? 'Restore ended before the import was paused.'); })]);
  await json('POST', `/jobs/${jobId}/cancel`);
  const cancelled = await waitForJob(jobId);
  expect(cancelled.status).toBe('cancelled');
  expect(cancelled.requiresRecovery).toBe(true);
  expect(observedRunningRefresh).toBe(true);
  expect(checkedTruncations).toBeGreaterThan(0);
  expect(Object.values(await refreshStates(target))).toEqual(['Disabled', 'Disabled']);
  expect(await rows(target)).toEqual([]);

  beforeExecute = undefined;
  afterImport = async () => { throw new Error('Controlled import failure'); };
  const retryCheck = await runJob('/restores/check', { ...request, recoveryJobId: cancelled.id });
  const retry = await json<{ jobId: string }>('POST', '/restores', {
    checkJobId: retryCheck.id, confirmationName: database, targetWritesPaused: true,
  }, 202);
  const failed = await waitForJob(retry.jobId);
  expect(failed.status).toBe('failed');
  expect(failed.error?.message).toContain('Controlled import failure');
  expect(failed.requiresRecovery).toBe(true);
  expect(Object.values(await refreshStates(target))).toEqual(['Disabled', 'Disabled']);

  afterImport = undefined;
  const finalCheck = await runJob('/restores/check', { ...request, recoveryJobId: failed.id });
  await runJob('/restores', { checkJobId: finalCheck.id, confirmationName: database, targetWritesPaused: true });
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  await expectSavedRefreshData(target);
  await expectRestoredBufferData(target);
  const states = await refreshStates(target);
  expect(states.refresh_mv).not.toBe('Disabled');
  expect(states.append_mv).toBe('Disabled');
});
