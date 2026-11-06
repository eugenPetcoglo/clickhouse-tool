import type { CheckedBackup } from '../backups/archive.js';
import { quoteIdentifier } from '../clickhouse/client.js';
import { orderViews } from '../clickhouse/metadata.js';
import type { OperationContext } from '../types.js';
import type { RestoreInput, RestoreResult } from './types.js';

/** Both restore modes use exactly the same data path once an empty compatible schema exists. */
export async function loadBackupData(input: RestoreInput, backup: CheckedBackup, detachedViews: string[], context: OperationContext): Promise<RestoreResult> {
  const database = backup.schema.name;
  const views = orderViews(backup.schema.objects);
  const tables = backup.schema.objects.filter(object => object.kind === 'table').sort((left, right) => left.name.localeCompare(right.name));
  const options = { context, signal: context.signal };

  // Keep triggers disabled until ALL raw and aggregated tables have been imported.
  for (const view of [...views].reverse()) {
    context.signal.throwIfAborted();
    await context.setPhase('detaching-views', view.name);
    if (!detachedViews.includes(view.name)) {
      await context.recordChange('detach-view', view.name);
      await input.client.execute(input.target, `DETACH TABLE ${quoteIdentifier(database)}.${quoteIdentifier(view.name)} PERMANENTLY SYNC`, options);
    }
    await context.recordChange('detach-view', view.name, true);
  }

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

  for (const view of views) {
    context.signal.throwIfAborted();
    await context.setPhase('attaching-views', view.name);
    await context.recordChange('attach-view', view.name);
    await input.client.execute(input.target, `ATTACH TABLE ${quoteIdentifier(database)}.${quoteIdentifier(view.name)}`, options);
    await context.recordChange('attach-view', view.name, true);
  }
  await context.setPhase('completed');
  return { database, mode: input.mode, tablesRestored: tables.length, viewsRestored: views.length };
}
