import { createHash } from 'node:crypto';
import { checkBackup } from '../backups/archive.js';
import { checkBufferVersion, getBufferTables, getCreateTables, getDataTables, orderViews, readDatabaseSchema, schemaFingerprint, validateSupportedSchema } from '../clickhouse/metadata.js';
import { AppError } from '../errors.js';
import type { DatabaseSchema, OperationContext, SchemaObject } from '../types.js';
import { checkCreateStatements, checkParsedCreateStatements, isRefreshableView } from '../clickhouse/ddl.js';
import type { RestoreCheck, RestoreInput } from './types.js';

export type { RestoreCheck, RestoreInput, RestoreRecovery } from './types.js';

export async function checkRestore(input: RestoreInput, context?: OperationContext): Promise<RestoreCheck> {
  const prepared = await prepareRestore(input, context);
  await prepared.backup.close();
  return prepared.check;
}

/** Execution keeps the verified archive open, so data is read from that same file. */
export async function prepareRestore(input: RestoreInput, context?: OperationContext) {
  const backup = await checkBackup(input.dataDir, input.backupId, context);
  try {
    const schema = backup.schema;
    // Bind confirmation and recovery to the data hashes as well as the DDL.
    // A different valid archive with the same schema still needs a new check.
    const backupFingerprint = createHash('sha256').update(JSON.stringify(backup.manifest)).digest('hex');
    validateSupportedSchema(schema);
    checkCreateStatements(schema);
    await context?.setPhase('checking-destination');
    const options = { context, signal: context?.signal };
    const [identity] = await input.client.query<{ version: string; serverId: string }>(
      input.target, 'SELECT version() AS version, toString(serverUUID()) AS serverId', options,
    );
    if (!identity?.version || !identity.serverId) {
      throw new AppError('CONNECTION_REQUIRED', 'Cannot identify the destination server.', 409);
    }
    if (identity.serverId !== input.target.serverId) {
      throw new AppError('DESTINATION_CHANGED', 'Destination server changed. Reconnect and check the restore again.', 409);
    }
    const sourceVersion = backup.manifest.source.serverVersion;
    if (versionFamily(identity.version) !== versionFamily(sourceVersion)) {
      throw new AppError('VERSION_MISMATCH', 'Restore requires the same ClickHouse major/minor version as the backup.', 422);
    }
    checkBufferVersion(schema, identity.version);
    await checkParsedCreateStatements(input.client, input.target, schema, context);
    if (schema.objects.some(isRefreshableView)) {
      // Check runtime metadata before creating a database or clearing any data.
      await input.client.query(input.target,
        'SELECT view, status FROM system.view_refreshes WHERE database = {database:String}',
        { ...options, parameters: { database: schema.name } });
    }
    const databases = await input.client.query<{ name: string }>(input.target,
      'SELECT name FROM system.databases WHERE name = {database:String}',
      { ...options, parameters: { database: schema.name } });
    const exists = databases.some(database => database.name === schema.name);
    checkRecoveryIdentity(input, identity.serverId, schema, backupFingerprint);

    if (input.mode === 'replace-data' && !exists) {
      throw new AppError('DATABASE_NOT_FOUND', 'The destination database must already exist for replace-data mode.', 422);
    }
    const ownedNewDatabase = input.recovery?.changes.some(change =>
      change.action === 'create-database' && change.object === schema.name && change.state === 'completed') ?? false;
    if (input.mode === 'create-new' && exists && !ownedNewDatabase) {
      throw new AppError('DATABASE_EXISTS', 'The database already exists. Choose replace-data or a different destination.', 409);
    }

    let current: DatabaseSchema | undefined;
    let detachedViews: string[] = [];
    let missingObjects: SchemaObject[] = [...schema.objects];
    if (exists) {
      current = await readDatabaseSchema(input.client, input.target, schema.name, context);
      detachedViews = await readRecoverableViews(input, schema, context);
      const merged = mergeRecoverySchema(current, schema, detachedViews, input.mode === 'create-new' && ownedNewDatabase);
      validateSupportedSchema(merged);
      if (schemaFingerprint(merged) !== schemaFingerprint(schema)) {
        throw new AppError('SCHEMA_MISMATCH', 'Destination objects or definitions differ from the backup. No data was changed.', 422);
      }
      const names = new Set([...current.objects.map(object => object.name), ...detachedViews]);
      missingObjects = schema.objects.filter(object => !names.has(object.name));
    }
    await checkStoragePolicies(input, schema, context);
    const targetFingerprint = current ? schemaFingerprint(current) : undefined;
    const actions = restoreActions(input.mode, schema, !exists, missingObjects);
    const check: RestoreCheck = {
      backupId: input.backupId,
      database: schema.name,
      mode: input.mode,
      targetConnectionId: input.target.profile.id,
      targetServerId: identity.serverId,
      targetServerVersion: identity.version,
      sourceServerVersion: sourceVersion,
      backupFingerprint,
      targetFingerprint,
      fingerprint: createHash('sha256').update(JSON.stringify({
        backupId: input.backupId, backupFingerprint, schema: schemaFingerprint(schema), targetFingerprint,
        target: input.target.profile.id, serverId: identity.serverId, version: identity.version,
        mode: input.mode, detachedViews: [...detachedViews].sort(),
      })).digest('hex'),
      actions,
      warnings: [
        'Pause application writes and migrations on the destination before restoring.',
        'Logical backups contain per-table snapshots. TTL and MergeTree merges still apply.',
        ...(getBufferTables(schema).length ? ['Destination buffers are drained before tables are cleared. Saved buffer rows are restored directly into their durable target tables.'] : []),
        ...(schema.objects.some(isRefreshableView) ? ['Refresh schedules resume after loading. Scheduled refreshes can then update the restored data.'] : []),
        ...(detachedViews.length ? ['Detached view definitions will be temporarily attached, verified and detached again before any table is cleared.'] : []),
      ],
    };
    if (input.expectedCheck && input.expectedCheck.fingerprint !== check.fingerprint) {
      throw new AppError('RESTORE_CHECK_EXPIRED', 'Backup or destination changed. Check and confirm the restore again.', 409);
    }
    return { backup, check, databaseExists: exists, detachedViews, missingObjects };
  } catch (error) {
    await backup.close();
    throw error;
  }
}

function versionFamily(version: string): string {
  const match = /^(\d+)\.(\d+)(?:\.|$)/.exec(version);
  if (!match) throw new AppError('UNSUPPORTED_VERSION', `Unrecognized ClickHouse version: ${version}`, 422);
  return `${match[1]}.${match[2]}`;
}

function checkRecoveryIdentity(input: RestoreInput, serverId: string, schema: DatabaseSchema, backupFingerprint: string): void {
  const recovery = input.recovery;
  if (recovery && (recovery.targetServerId !== serverId || recovery.database !== schema.name
    || recovery.backupId !== input.backupId || recovery.backupFingerprint !== backupFingerprint)) {
    throw new AppError('RECOVERY_MISMATCH', 'Recovery journal belongs to a different backup or destination.', 409);
  }
}

async function readRecoverableViews(input: RestoreInput, schema: DatabaseSchema, context?: OperationContext): Promise<string[]> {
  if (!input.recovery) return [];
  const detached = await input.client.query<{ name: string }>(input.target,
    'SELECT table AS name FROM system.detached_tables WHERE database = {database:String} AND is_permanently = 1',
    { context, signal: context?.signal, parameters: { database: schema.name } });
  const viewNames = new Set(schema.objects.filter(object => object.kind !== 'table').map(object => object.name));
  const journalNames = new Set(input.recovery.changes.filter(change => change.action === 'detach-view').map(change => change.object));
  const names = detached.filter(view => viewNames.has(view.name) && journalNames.has(view.name)).map(view => view.name);
  if (schema.objects.some(object => names.includes(object.name) && isRefreshableView(object))) {
    throw new AppError('UNSAFE_VIEW_RECOVERY', 'A refreshable view is detached. Reattaching it can replay saved data; inspect the destination before retrying.', 409);
  }
  return names;
}

function mergeRecoverySchema(current: DatabaseSchema, expected: DatabaseSchema, detached: string[], allowMissing: boolean): DatabaseSchema {
  const names = new Set(current.objects.map(object => object.name));
  const additions = expected.objects.filter(object => !names.has(object.name)
    && (detached.includes(object.name) || allowMissing));
  return { ...current, objects: [...current.objects, ...additions] };
}

async function checkStoragePolicies(input: RestoreInput, schema: DatabaseSchema, context?: OperationContext): Promise<void> {
  const required = new Set(getDataTables(schema).map(object => object.storagePolicy).filter(Boolean));
  if (required.size === 0) return;
  const policies = await input.client.query<{ name: string }>(input.target,
    'SELECT policy_name AS name FROM system.storage_policies', { context, signal: context?.signal });
  const available = new Set(policies.map(policy => policy.name));
  const missing = [...required].filter(name => !available.has(name));
  if (missing.length) {
    throw new AppError('MISSING_STORAGE_POLICY', `Destination storage policies are missing: ${missing.join(', ')}.`, 422);
  }
}

function restoreActions(mode: RestoreInput['mode'], schema: DatabaseSchema, createDatabase: boolean, missing: SchemaObject[]): string[] {
  const actions: string[] = [];
  const missingNames = new Set(missing.map(object => object.name));
  const tables = getCreateTables(schema);
  const views = orderViews(schema.objects);
  if (mode === 'create-new') {
    if (createDatabase) actions.push(`Create database ${schema.name}`);
    for (const table of tables.filter(table => missingNames.has(table.name))) actions.push(`Create table ${table.name}`);
  }
  for (const view of [...views].reverse().filter(view => !missingNames.has(view.name))) {
    actions.push(isRefreshableView(view)
      ? `Stop refreshable view ${view.name} and wait for its refresh to finish`
      : `Temporarily detach view ${view.name}`);
  }
  for (const buffer of getBufferTables(schema)) actions.push(`Drain buffer ${buffer.name} and wait for its queued writes before clearing targets`);
  for (const table of tables) {
    actions.push(table.engine === 'Buffer'
      ? `Restore saved memory rows from buffer ${table.name} into table ${table.targetTable}`
      : `Truncate and restore table ${table.name}`);
  }
  for (const view of views) {
    if (missingNames.has(view.name)) {
      actions.push(isRefreshableView(view) ? `Create refreshable view ${view.name} without an initial refresh` : `Create ${view.kind} ${view.name}`);
    } else if (!isRefreshableView(view)) {
      actions.push(`Reattach view ${view.name}`);
    }
  }
  if (views.some(isRefreshableView)) actions.push('Restore the original refresh schedule states after all data imports succeed');
  return actions;
}
