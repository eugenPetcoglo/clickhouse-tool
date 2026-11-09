import { setTimeout as delay } from 'node:timers/promises';
import { quoteIdentifier } from '../clickhouse/client.js';
import { createViewStatement, isRefreshableView } from '../clickhouse/ddl.js';
import { orderViews } from '../clickhouse/metadata.js';
import { AppError, errorMessage } from '../errors.js';
import type { DatabaseSchema, OperationContext } from '../types.js';
import type { RestoreInput } from './types.js';

interface RefreshStatus {
  view: string;
  status: string;
}

export async function pauseRefreshableViews(
  input: RestoreInput,
  schema: DatabaseSchema,
  missingViewNames: string[],
  context: OperationContext,
): Promise<string[]> {
  const views = orderViews(schema.objects).filter(isRefreshableView);
  if (views.length === 0) return [];
  const missing = new Set(missingViewNames);
  const existing = views.filter(view => !missing.has(view.name));
  const statuses = existing.length ? await readRefreshStatuses(input, schema.name, context, context.signal) : [];
  for (const view of existing) requireRefreshStatus(statuses, view.name);

  const enabled: string[] = [];
  // Save every original state before stopping any view. Recovery must not mistake
  // a view stopped by an earlier attempt for a view disabled by its owner.
  for (const view of views) {
    const saved = input.recovery?.changes.find(change => change.object === view.name
      && change.state === 'completed' && ['refresh-enabled', 'refresh-disabled'].includes(change.action));
    const wasEnabled = saved ? saved.action === 'refresh-enabled'
      : missing.has(view.name) || requireRefreshStatus(statuses, view.name).status !== 'Disabled';
    if (wasEnabled) enabled.push(view.name);
    await context.recordChange(wasEnabled ? 'refresh-enabled' : 'refresh-disabled', view.name, true);
  }

  try {
    for (const view of [...existing].reverse()) {
      context.signal.throwIfAborted();
      await context.setPhase('stopping-refreshes', view.name);
      await context.recordChange('stop-refresh', view.name);
      await stopRefresh(input, schema.name, view.name, context, context.signal);
      await context.recordChange('stop-refresh', view.name, true);
    }
  } catch (error) {
    const cleanupErrors = await stopRestoreViewsAfterFailure(input, schema, context);
    if (cleanupErrors.length) {
      throw new AppError('RESTORE_CLEANUP_FAILED', `${errorMessage(error)} Cleanup also failed: ${cleanupErrors.join(' ')}`, 500);
    }
    throw error;
  }
  return enabled;
}

export async function detachRestoreViews(
  input: RestoreInput,
  schema: DatabaseSchema,
  detachedViews: string[],
  missingViewNames: string[],
  context: OperationContext,
): Promise<void> {
  const detached = new Set(detachedViews);
  const missing = new Set(missingViewNames);
  for (const view of orderViews(schema.objects).reverse()) {
    if (isRefreshableView(view) || missing.has(view.name)) continue;
    context.signal.throwIfAborted();
    await context.setPhase('detaching-views', view.name);
    if (!detached.has(view.name)) {
      await context.recordChange('detach-view', view.name);
      await input.client.execute(input.target,
        `DETACH TABLE ${qualifiedName(schema.name, view.name)} PERMANENTLY SYNC`, { context });
    }
    await context.recordChange('detach-view', view.name, true);
  }
}

export async function finishRestoreViews(
  input: RestoreInput,
  schema: DatabaseSchema,
  missingViewNames: string[],
  enabledRefreshableViews: string[],
  context: OperationContext,
): Promise<void> {
  const missing = new Set(missingViewNames);
  const views = orderViews(schema.objects);
  for (const view of views) {
    context.signal.throwIfAborted();
    if (missing.has(view.name)) {
      await context.setPhase('creating-schema', view.name);
      await context.recordChange('create-object', view.name);
      await input.client.execute(input.target, createViewStatement(view.ddl), { context });
      await context.recordChange('create-object', view.name, true);
      if (isRefreshableView(view) && !enabledRefreshableViews.includes(view.name)) {
        await context.recordChange('stop-refresh', view.name);
        await stopRefresh(input, schema.name, view.name, context, context.signal);
        await context.recordChange('stop-refresh', view.name, true);
      }
    } else if (!isRefreshableView(view)) {
      await context.setPhase('attaching-views', view.name);
      await context.recordChange('attach-view', view.name);
      await input.client.execute(input.target, `ATTACH TABLE ${qualifiedName(schema.name, view.name)}`, { context });
      await context.recordChange('attach-view', view.name, true);
    }
  }
  for (const view of views.filter(isRefreshableView)) {
    if (!enabledRefreshableViews.includes(view.name)) continue;
    context.signal.throwIfAborted();
    await context.setPhase('starting-refreshes', view.name);
    await context.recordChange('start-refresh', view.name);
    await input.client.execute(input.target, `SYSTEM START VIEW ${qualifiedName(schema.name, view.name)}`, { context });
    await context.recordChange('start-refresh', view.name, true);
  }
}

/** Keep refreshes stopped after failure, even when the job was cancelled. */
export async function stopRestoreViewsAfterFailure(
  input: RestoreInput,
  schema: DatabaseSchema,
  context: OperationContext,
): Promise<string[]> {
  const views = orderViews(schema.objects).filter(isRefreshableView).reverse();
  if (views.length === 0) return [];
  const signal = new AbortController().signal;
  const errors: string[] = [];
  let existing: Set<string> | undefined;
  try {
    const rows = await input.client.query<{ name: string }>(input.target,
      'SELECT name FROM system.tables WHERE database = {database:String} AND engine = \'MaterializedView\'',
      { context, signal, parameters: { database: schema.name } });
    existing = new Set(rows.map(row => row.name));
  } catch (error) {
    errors.push(`Cannot identify refreshable views during cleanup: ${errorMessage(error)}`);
  }
  for (const view of views) {
    if (existing && !existing.has(view.name)) continue;
    try { await context.recordChange('stop-refresh', view.name); }
    catch (error) { errors.push(`Cannot journal stopping ${view.name}: ${errorMessage(error)}`); }
    try {
      await stopRefresh(input, schema.name, view.name, context, signal);
    } catch (error) {
      errors.push(`Cannot stop refreshable view ${view.name}: ${errorMessage(error)}`);
      continue;
    }
    try { await context.recordChange('stop-refresh', view.name, true); }
    catch (error) { errors.push(`Cannot journal stopped view ${view.name}: ${errorMessage(error)}`); }
  }
  return errors;
}

async function stopRefresh(input: RestoreInput, database: string, view: string, context: OperationContext, signal: AbortSignal): Promise<void> {
  const timeout = AbortSignal.timeout(60_000);
  const stopSignal = AbortSignal.any([signal, timeout]);
  try {
    await input.client.execute(input.target, `SYSTEM STOP VIEW ${qualifiedName(database, view)}`, { context, signal: stopSignal });
    while (true) {
      stopSignal.throwIfAborted();
      const statuses = await readRefreshStatuses(input, database, context, stopSignal);
      if (requireRefreshStatus(statuses, view).status === 'Disabled') return;
      await delay(250, undefined, { signal: stopSignal });
    }
  } catch (error) {
    if (timeout.aborted && !signal.aborted) {
      throw new AppError('REFRESH_STOP_TIMEOUT', `Refreshable view ${view} did not stop within 60 seconds. Restore cannot safely continue while it is refreshing.`, 504);
    }
    throw error;
  }
}

async function readRefreshStatuses(input: RestoreInput, database: string, context: OperationContext, signal: AbortSignal): Promise<RefreshStatus[]> {
  return input.client.query<RefreshStatus>(input.target,
    'SELECT view, status FROM system.view_refreshes WHERE database = {database:String}',
    { context, signal, parameters: { database } });
}

function requireRefreshStatus(statuses: RefreshStatus[], view: string): RefreshStatus {
  const matches = statuses.filter(status => status.view === view);
  if (matches.length !== 1 || typeof matches[0]?.status !== 'string' || !matches[0].status) {
    throw new AppError('REFRESH_METADATA_UNAVAILABLE', `Cannot read the refresh state of materialized view ${view}. Restore cannot safely continue.`, 422);
  }
  return matches[0];
}

function qualifiedName(database: string, view: string): string {
  return `${quoteIdentifier(database)}.${quoteIdentifier(view)}`;
}
