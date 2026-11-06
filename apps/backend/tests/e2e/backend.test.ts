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
import type { ClickHouseClient, ConnectedConnection, ConnectionProfile, RestoreMode } from '../../src/types.js';
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
  app = await createApp(loadConfig({ DATA_DIR: join(directory, database) }));
  baseUrl = await app.listen({ host: '127.0.0.1', port: 0 });
  sourceProfile = await connect(sourceServer.port);
  targetProfile = await connect(targetServer.port);
  await client.execute(source, `CREATE DATABASE ${database} ENGINE = Atomic`);
  await client.execute(source, `CREATE TABLE ${database}.raw (id UInt32, value UInt32 DEFAULT id + 1, computed UInt32 MATERIALIZED value * 10, alias_value UInt32 ALIAS computed + 1) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE TABLE ${database}.agg (total AggregateFunction(sum, UInt32)) ENGINE = AggregatingMergeTree ORDER BY tuple()`);
  await client.execute(source, `CREATE TABLE ${database}.empty (id UInt32) ENGINE = MergeTree ORDER BY id`);
  await client.execute(source, `CREATE MATERIALIZED VIEW ${database}.mv TO ${database}.agg AS SELECT sumState(value) AS total FROM ${database}.raw`);
  await client.execute(source, `CREATE VIEW ${database}.v AS SELECT id, value FROM ${database}.raw`);
  await client.execute(source, `INSERT INTO ${database}.raw (id, value, computed) SETTINGS insert_allow_materialized_columns=1 VALUES (1, 7, 999), (2, 8, 888)`);
  backup = (await runJob('/backups', { sourceConnectionId: sourceProfile.id, database })).result as BackupSummary;
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

  await client.execute(source, `DROP DATABASE ${database} SYNC`);
  const anotherTarget = await connect(sourceServer.port);
  await restore('create-new', anotherTarget.id, imported.backup.id);
  expect(await rows(source)).toEqual(expectedRows);
  expect(await total(source)).toBe(15);
});

it('replaces data, clears empty tables and restores aggregates without double filling', async () => {
  await restore('create-new');
  await client.execute(target, `INSERT INTO ${database}.raw (id, value) VALUES (99, 1000)`);
  await client.execute(target, `INSERT INTO ${database}.empty VALUES (99)`);
  await restore('replace-data');
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
  expect(await client.query(target, `SELECT * FROM ${database}.empty`)).toEqual([]);
  expect(await client.query(target, `SELECT id FROM ${database}.v ORDER BY id`)).toEqual([{ id: 1 }, { id: 2 }]);
  await client.execute(target, `INSERT INTO ${database}.raw (id, value) VALUES (3, 3)`);
  expect(await total(target)).toBe(18);
});

it('rejects a damaged backup and a schema mismatch before changing destination data', async () => {
  await restore('create-new');
  const original = await readFile(backup.path);
  await writeFile(backup.path, original.subarray(0, original.length - 50));
  const request = { backupId: backup.id, targetConnectionId: targetProfile.id, mode: 'replace-data' };
  const damaged = await json<{ jobId: string }>('POST', '/restores/check', request, 202);
  expect((await waitForJob(damaged.jobId)).status).toBe('failed');
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);

  await writeFile(backup.path, original);
  await client.execute(target, `ALTER TABLE ${database}.raw ADD COLUMN extra UInt8`);
  const mismatch = await json<{ jobId: string }>('POST', '/restores/check', request, 202);
  const failed = await waitForJob(mismatch.jobId);
  expect(failed.error?.code).toBe('SCHEMA_MISMATCH');
  expect(failed.changes).toEqual([]);
  expect(await rows(target)).toEqual(expectedRows);
  expect(await total(target)).toBe(15);
});
