import type { CheckedBackup } from '../backups/archive.js';
import { quoteIdentifier } from '../clickhouse/client.js';
import { getBufferTables, getCreateTables } from '../clickhouse/metadata.js';
import { AppError } from '../errors.js';
import type { OperationContext } from '../types.js';
import type { RestoreInput } from './types.js';

/** Call only after views are disabled and all physical tables exist. */
export async function loadBackupData(input: RestoreInput, backup: CheckedBackup, context: OperationContext): Promise<void> {
  const database = backup.schema.name;
  const tables = getCreateTables(backup.schema);
  const options = { context, signal: context.signal };

  // Buffer cannot be truncated. Drain old queued rows before clearing targets.
  // Reading its RAM rows also waits for a flush that was already in progress.
  for (const buffer of getBufferTables(backup.schema)) {
    context.signal.throwIfAborted();
    await context.setPhase('draining-buffers', buffer.name);
    await context.recordChange('flush-buffer', buffer.name);
    await input.client.execute(input.target, `OPTIMIZE TABLE ${quoteIdentifier(database)}.${quoteIdentifier(buffer.name)}`, options);
    const [row] = await input.client.query<{ rows: string }>(input.target,
      `SELECT toString(count()) AS rows FROM ${quoteIdentifier(database)}.${quoteIdentifier(buffer.name)} WHERE _table = {bufferTable:String} SETTINGS optimize_trivial_count_query=0`,
      { ...options, parameters: { bufferTable: buffer.name } });
    if (row?.rows !== '0') {
      throw new AppError('BUFFER_NOT_EMPTY', `Buffer table ${buffer.name} still contains queued rows. Keep destination writes paused before retrying.`, 409);
    }
    await context.recordChange('flush-buffer', buffer.name, true);
  }

  for (const table of tables.filter(table => table.engine !== 'Buffer')) {
    context.signal.throwIfAborted();
    await context.setPhase('clearing-tables', table.name);
    await context.recordChange('truncate-table', table.name);
    await input.client.execute(input.target, `TRUNCATE TABLE ${quoteIdentifier(database)}.${quoteIdentifier(table.name)}`, options);
    await context.recordChange('truncate-table', table.name, true);
  }

  for (const table of tables) {
    context.signal.throwIfAborted();
    await context.setPhase('importing-data', table.name);
    await context.recordChange('import-table', table.name);
    // SELECT FORMAT Native emits no block at all for an empty table. Its
    // verified zero-byte entry means TRUNCATE already completed the restore.
    const dataFile = backup.manifest.files.find(file => file.path === `data/${table.id}.native`);
    if (dataFile?.size === 0) {
      await context.recordChange('import-table', table.name, true);
      await context.completeObject?.(table.name);
      continue;
    }
    const stream = await backup.readData(table.id);
    try {
      // Keep saved RAM rows durable instead of restoring volatile Buffer state.
      const destination = table.engine === 'Buffer' ? { ...table, name: table.targetTable! } : table;
      await input.client.importTable(input.target, database, destination, stream, context);
    } finally {
      stream.destroy();
    }
    await context.recordChange('import-table', table.name, true);
    await context.completeObject?.(table.name);
  }
}
