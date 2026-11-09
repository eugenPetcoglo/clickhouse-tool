import { getCreateTables } from '../clickhouse/metadata.js';
import { AppError, errorMessage } from '../errors.js';
import type { OperationContext } from '../types.js';
import { prepareRestore } from './check.js';
import { loadBackupData } from './load.js';
import { verifyDetachedViews } from './recovery.js';
import type { RestoreInput, RestoreResult } from './types.js';
import { detachRestoreViews, finishRestoreViews, pauseRefreshableViews, stopRestoreViewsAfterFailure } from './views.js';

export async function restoreNewDatabase(input: RestoreInput, context: OperationContext): Promise<RestoreResult> {
  if (input.mode !== 'create-new') throw new AppError('INVALID_RESTORE_MODE', 'Expected create-new mode.');
  const prepared = await prepareRestore(input, context);
  const schema = prepared.backup.schema;
  const missingViews = prepared.missingObjects.filter(object => object.kind !== 'table').map(object => object.name);
  const options = { context, signal: context.signal };
  let viewsPrepared = false;
  try {
    if (!prepared.databaseExists) {
      context.signal.throwIfAborted();
      await context.setPhase('creating-database');
      await context.recordChange('create-database', schema.name);
      await input.client.execute(input.target, schema.ddl, options);
      await context.recordChange('create-database', schema.name, true);
    }
    const missingNames = new Set(prepared.missingObjects.map(object => object.name));
    const tables = getCreateTables(schema);
    // Create views only after all saved data is in place.
    for (const object of tables) {
      if (!missingNames.has(object.name)) continue;
      context.signal.throwIfAborted();
      await context.setPhase('creating-schema', object.name);
      await context.recordChange('create-object', object.name);
      await input.client.execute(input.target, object.ddl, options);
      await context.recordChange('create-object', object.name, true);
    }
    const enabledViews = await pauseRefreshableViews(input, schema, missingViews, context);
    viewsPrepared = true;
    await verifyDetachedViews(input, schema, prepared.detachedViews, context, missingViews);
    await detachRestoreViews(input, schema, prepared.detachedViews, missingViews, context);
    await loadBackupData(input, prepared.backup, context);
    await finishRestoreViews(input, schema, missingViews, enabledViews, context);
    await context.setPhase('completed');
    return {
      database: schema.name, mode: input.mode,
      tablesRestored: tables.length,
      viewsRestored: schema.objects.filter(object => object.kind !== 'table').length,
    };
  } catch (error) {
    const cleanupErrors = viewsPrepared ? await stopRestoreViewsAfterFailure(input, schema, context) : [];
    if (cleanupErrors.length) {
      throw new AppError('RESTORE_CLEANUP_FAILED', `${errorMessage(error)} Refresh cleanup failed: ${cleanupErrors.join('; ')}`, 500);
    }
    throw error;
  } finally {
    await prepared.backup.close();
  }
}
