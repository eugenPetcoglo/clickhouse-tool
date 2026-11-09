import { getDataTables } from '../clickhouse/metadata.js';
import { AppError, errorMessage } from '../errors.js';
import type { OperationContext } from '../types.js';
import { prepareRestore } from './check.js';
import { loadBackupData } from './load.js';
import { verifyDetachedViews } from './recovery.js';
import type { RestoreInput, RestoreResult } from './types.js';
import { detachRestoreViews, finishRestoreViews, pauseRefreshableViews, stopRestoreViewsAfterFailure } from './views.js';

export async function restoreExistingDatabase(input: RestoreInput, context: OperationContext): Promise<RestoreResult> {
  if (input.mode !== 'replace-data') throw new AppError('INVALID_RESTORE_MODE', 'Expected replace-data mode.');
  const prepared = await prepareRestore(input, context);
  const schema = prepared.backup.schema;
  let viewsPrepared = false;
  try {
    const enabledViews = await pauseRefreshableViews(input, schema, [], context);
    viewsPrepared = true;
    await verifyDetachedViews(input, schema, prepared.detachedViews, context);
    await detachRestoreViews(input, schema, prepared.detachedViews, [], context);
    await loadBackupData(input, prepared.backup, context);
    await finishRestoreViews(input, schema, [], enabledViews, context);
    await context.setPhase('completed');
    return {
      database: schema.name, mode: input.mode,
      tablesRestored: getDataTables(schema).length,
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
