import { quoteIdentifier } from '../clickhouse/client.js';
import { orderViews, readDatabaseSchema, schemaFingerprint } from '../clickhouse/metadata.js';
import { AppError } from '../errors.js';
import type { DatabaseSchema, OperationContext } from '../types.js';
import type { RestoreInput } from './types.js';

/** Detached definitions cannot be read with SHOW CREATE. Verify them before clearing data. */
export async function verifyDetachedViews(input: RestoreInput, schema: DatabaseSchema, detached: string[], context: OperationContext): Promise<void> {
  if (detached.length === 0) return;
  const views = orderViews(schema.objects).filter(view => detached.includes(view.name));
  const attached: string[] = [];
  try {
    for (const view of views) {
      context.signal.throwIfAborted();
      await context.setPhase('checking-detached-views', view.name);
      await context.recordChange('attach-view', view.name);
      await input.client.execute(input.target, `ATTACH TABLE ${quoteIdentifier(schema.name)}.${quoteIdentifier(view.name)}`, { context });
      attached.push(view.name);
      await context.recordChange('attach-view', view.name, true);
    }
    const current = await readDatabaseSchema(input.client, input.target, schema.name, context);
    if (schemaFingerprint(current) !== schemaFingerprint(schema)) {
      throw new AppError('SCHEMA_MISMATCH', 'Detached view definitions changed. Destination data was not cleared.', 422);
    }
  } finally {
    // Cancellation must still leave successfully attached triggers disabled.
    const cleanupSignal = new AbortController().signal;
    for (const name of attached.reverse()) {
      await context.recordChange('detach-view', name);
      await input.client.execute(input.target, `DETACH TABLE ${quoteIdentifier(schema.name)}.${quoteIdentifier(name)} PERMANENTLY SYNC`, { context, signal: cleanupSignal });
      await context.recordChange('detach-view', name, true);
    }
  }
}
