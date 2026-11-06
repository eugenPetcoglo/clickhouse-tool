import { orderViews } from '../clickhouse/metadata.js';
import { AppError } from '../errors.js';
import type { OperationContext } from '../types.js';
import { prepareRestore } from './check.js';
import { loadBackupData } from './load.js';
import { verifyDetachedViews } from './recovery.js';
import type { RestoreInput, RestoreResult } from './types.js';

export async function restoreNewDatabase(input: RestoreInput, context: OperationContext): Promise<RestoreResult> {
  if (input.mode !== 'create-new') throw new AppError('INVALID_RESTORE_MODE', 'Expected create-new mode.');
  const prepared = await prepareRestore(input, context);
  const options = { context, signal: context.signal };
  try {
    if (!prepared.databaseExists) {
      context.signal.throwIfAborted();
      await context.setPhase('creating-database');
      await context.recordChange('create-database', prepared.backup.schema.name);
      await input.client.execute(input.target, prepared.backup.schema.ddl, options);
      await context.recordChange('create-database', prepared.backup.schema.name, true);
    }
    const missingNames = new Set(prepared.missingObjects.map(object => object.name));
    const tables = prepared.backup.schema.objects.filter(object => object.kind === 'table').sort((left, right) => left.name.localeCompare(right.name));
    const objects = [...tables, ...orderViews(prepared.backup.schema.objects)];
    // Views are created against empty tables. Even a saved POPULATE clause cannot double-fill aggregates.
    for (const object of objects) {
      if (!missingNames.has(object.name)) continue;
      context.signal.throwIfAborted();
      await context.setPhase('creating-schema', object.name);
      await context.recordChange('create-object', object.name);
      await input.client.execute(input.target, object.ddl, options);
      await context.recordChange('create-object', object.name, true);
    }
    await verifyDetachedViews(input, prepared.backup.schema, prepared.detachedViews, context);
    return await loadBackupData(input, prepared.backup, prepared.detachedViews, context);
  } finally {
    await prepared.backup.close();
  }
}
