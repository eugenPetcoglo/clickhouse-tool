import { AppError } from '../errors.js';
import type { OperationContext } from '../types.js';
import { prepareRestore } from './check.js';
import { loadBackupData } from './load.js';
import { verifyDetachedViews } from './recovery.js';
import type { RestoreInput, RestoreResult } from './types.js';

export async function restoreExistingDatabase(input: RestoreInput, context: OperationContext): Promise<RestoreResult> {
  if (input.mode !== 'replace-data') throw new AppError('INVALID_RESTORE_MODE', 'Expected replace-data mode.');
  const prepared = await prepareRestore(input, context);
  try {
    await verifyDetachedViews(input, prepared.backup.schema, prepared.detachedViews, context);
    return await loadBackupData(input, prepared.backup, prepared.detachedViews, context);
  } finally {
    await prepared.backup.close();
  }
}
