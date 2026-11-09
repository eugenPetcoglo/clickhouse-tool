import { randomUUID } from 'node:crypto';
import { checkBufferVersion, getBufferTables, getDataTables, readDatabaseSchema, schemaFingerprint, validateSupportedSchema } from '../clickhouse/metadata.js';
import { checkCreateStatements, checkParsedCreateStatements } from '../clickhouse/ddl.js';
import { AppError } from '../errors.js';
import type { ClickHouseClient, ConnectedConnection, OperationContext } from '../types.js';
import { createArchiveBuilder } from './archive.js';
import type { BackupFile, BackupSummary, Manifest } from './types.js';

interface CreateBackupInput {
  client: ClickHouseClient;
  connection: ConnectedConnection;
  database: string;
  dataDir: string;
}

export async function createBackup(input: CreateBackupInput, context: OperationContext): Promise<BackupSummary> {
  const { client, connection, database, dataDir } = input;
  const createdAt = new Date().toISOString();
  await context.setPhase('checking-connection');
  const serverVersion = await checkSourceConnection(client, connection, context);
  await context.setPhase('reading-schema');
  const schema = await readDatabaseSchema(client, connection, database, context);
  checkBufferVersion(schema, serverVersion);
  validateSupportedSchema(schema);
  checkCreateStatements(schema);
  await checkParsedCreateStatements(client, connection, schema, context);
  const id = randomUUID();
  const archive = await createArchiveBuilder(dataDir, id, context.signal);
  const files: BackupFile[] = [];
  const tableExports: Manifest['tableExports'] = [];
  try {
    files.push(await archive.addText('schema/database.sql', schema.ddl));
    for (const object of schema.objects) {
      context.signal.throwIfAborted();
      files.push(await archive.addText(`schema/${object.id}.sql`, object.ddl));
    }
    const buffers = getBufferTables(schema);
    const flushCount = buffers.length ? await readBufferFlushCount(client, connection, context) : undefined;
    // Read every buffer before durable tables. A preexisting flush holds its RAM
    // lock until the destination write finishes; later flushes invalidate the copy.
    const tables = [...buffers, ...getDataTables(schema).filter(table => table.engine !== 'Buffer')];
    for (const table of tables) {
      context.signal.throwIfAborted();
      await context.setPhase('exporting-table', table.name);
      const startedAt = new Date().toISOString();
      files.push(await archive.addData(`data/${table.id}.native`, output => client.exportTable(connection, database, table, output, context)));
      tableExports.push({ objectId: table.id, startedAt, finishedAt: new Date().toISOString() });
      await context.completeObject?.(table.name);
    }
    if (flushCount !== undefined && flushCount !== await readBufferFlushCount(client, connection, context)) {
      throw new AppError('SOURCE_BUFFER_FLUSHED', 'A Buffer table flushed during export. The backup was discarded to prevent missing or duplicate rows. Retry during a quiet period; pause source writes if necessary.', 409);
    }
    await context.setPhase('checking-schema');
    const current = await readDatabaseSchema(client, connection, database, context);
    if (schemaFingerprint(schema) !== schemaFingerprint(current)) {
      throw new AppError('SOURCE_SCHEMA_CHANGED', 'Database structure changed during backup. The backup was not published.', 409);
    }
    await checkSourceConnection(client, connection, context);
    context.signal.throwIfAborted();
    await context.setPhase('finalizing-backup');
    const manifest: Manifest = {
      formatVersion: 1,
      id,
      createdAt,
      finishedAt: new Date().toISOString(),
      source: { database, serverVersion, clientVersion: await client.version() },
      schema,
      files,
      tableExports,
    };
    return await archive.finish(manifest);
  } catch (error) {
    await archive.abort();
    throw error;
  }
}

async function readBufferFlushCount(client: ClickHouseClient, connection: ConnectedConnection, context: OperationContext): Promise<string> {
  const [row] = await client.query<{ flushes: string }>(connection,
    "SELECT toString(sum(value)) AS flushes FROM system.events WHERE event = 'StorageBufferFlush'", { context });
  if (!row || typeof row.flushes !== 'string' || !/^\d+$/.test(row.flushes)) {
    throw new AppError('UNSUPPORTED_VERSION', 'ClickHouse did not return the Buffer flush counter required for safe backup.');
  }
  return row.flushes;
}

async function checkSourceConnection(client: ClickHouseClient, connection: ConnectedConnection, context: OperationContext): Promise<string> {
  const [identity] = await client.query<{ version: string; serverId: string }>(
    connection, 'SELECT version() AS version, toString(serverUUID()) AS serverId', { context },
  );
  if (!identity?.version || !identity.serverId || !/^\d+\.\d+\./.test(identity.version)) {
    throw new AppError('UNSUPPORTED_VERSION', 'ClickHouse did not return the required source identity.');
  }
  const family = (version: string) => version.split('.').slice(0, 2).join('.');
  if (identity.serverId !== connection.serverId || family(identity.version) !== family(connection.serverVersion)) {
    throw new AppError('SOURCE_CHANGED', 'Source server or ClickHouse version changed. Connect again before creating a backup.', 409);
  }
  return identity.version;
}
