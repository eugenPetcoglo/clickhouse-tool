import type { CheckedBackup } from '../backups/archive.js';
import { quoteIdentifier } from '../clickhouse/client.js';
import { getDataTables } from '../clickhouse/metadata.js';
import type { OperationContext } from '../types.js';
import type { RestoreInput } from './types.js';

/** Call only after views are disabled and all physical tables exist. */
export async function loadBackupData(input: RestoreInput, backup: CheckedBackup, context: OperationContext): Promise<void> {
  const database = backup.schema.name;
  const tables = getDataTables(backup.schema);
  const options = { context, signal: context.signal };

  for (const table of tables) {
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
      await input.client.importTable(input.target, database, table, stream, context);
    } finally {
      stream.destroy();
    }
    await context.recordChange('import-table', table.name, true);
    await context.completeObject?.(table.name);
  }
}
