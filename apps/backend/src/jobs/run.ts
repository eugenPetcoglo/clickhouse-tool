import { randomUUID } from 'node:crypto';
import { AppError, errorMessage } from '../errors.js';
import type { OperationContext } from '../types.js';
import { createJobStore } from './store.js';
import type { Job, JobKind } from './types.js';

export type JobTask = (context: OperationContext, job: Job) => Promise<unknown>;

export async function createJobManager(dataDir: string) {
  const store = createJobStore(dataDir);
  const jobs = new Map((await store.load()).map(job => [job.id, job]));
  let active: { job: Job; controller: AbortController; done: Promise<void>; save: () => Promise<void> } | undefined;

  function get(id: string): Job {
    const job = jobs.get(id);
    if (!job) throw new AppError('JOB_NOT_FOUND', 'Job not found.', 404);
    return structuredClone(job);
  }

  async function start(kind: JobKind, input: Record<string, unknown>, task: JobTask, connectionId?: string): Promise<Job> {
    if (active && ['succeeded', 'failed', 'cancelled'].includes(active.job.status)) await active.done;
    if (active) throw new AppError('BACKEND_BUSY', 'Another operation is running. Wait for it to finish.', 409);
    const now = new Date().toISOString();
    const job: Job = {
      id: randomUUID(), kind, input, connectionId, status: 'running', phase: 'starting',
      createdAt: now, updatedAt: now, bytes: 0, completedObjects: [], changes: [], requiresRecovery: false,
    };
    const controller = new AbortController();
    // Reserve the slot before any await so concurrent HTTP requests cannot both start.
    const slot = { job, controller, done: Promise.resolve(), save: () => save() };
    active = slot;
    jobs.set(job.id, job);
    let writes = Promise.resolve();
    function save(): Promise<void> {
      job.updatedAt = new Date().toISOString();
      writes = writes.catch(() => undefined).then(() => store.save(job));
      return writes;
    }

    const context: OperationContext = {
      signal: controller.signal,
      async setPhase(phase, table) {
        controller.signal.throwIfAborted();
        job.phase = phase;
        job.table = table;
        await save();
      },
      addBytes(bytes) { job.bytes += bytes; },
      async setQueryId(queryId) { job.queryId = queryId; await save(); },
      async recordChange(action, object, completed = false) {
        job.requiresRecovery = true;
        job.changes.push({ action, object, state: completed ? 'completed' : 'started', at: new Date().toISOString() });
        await save();
      },
      async completeObject(name) {
        if (!job.completedObjects.includes(name)) job.completedObjects.push(name);
        await save();
      },
    };

    try { await save(); } catch (error) { active = undefined; jobs.delete(job.id); throw error; }
    slot.done = (async () => {
      const progress = setInterval(() => { void save().catch(() => undefined); }, 1000);
      progress.unref();
      try {
        job.result = await task(context, job);
        controller.signal.throwIfAborted();
        job.status = 'succeeded';
        job.phase = 'completed';
        job.requiresRecovery = false;
      } catch (error) {
        job.status = controller.signal.aborted ? 'cancelled' : 'failed';
        job.phase = job.status;
        job.error = { code: error instanceof AppError ? error.code : 'OPERATION_FAILED', message: errorMessage(error) };
      } finally {
        clearInterval(progress);
        try {
          await save();
        } catch (error) {
          job.status = 'failed';
          job.requiresRecovery = job.changes.length > 0;
          job.error = { code: 'JOURNAL_WRITE_FAILED', message: errorMessage(error) };
        } finally {
          if (active === slot) active = undefined;
        }
      }
    })();
    // The task owns its error status; a final journal failure must not become an unhandled rejection.
    void slot.done.catch(() => undefined);
    return get(job.id);
  }

  async function cancel(id: string): Promise<Job> {
    const job = get(id);
    if (!active || active.job.id !== id) return job;
    const current = active;
    if (['succeeded', 'failed', 'cancelled'].includes(current.job.status)) {
      await current.done;
      return get(id);
    }
    current.job.status = 'cancelling';
    current.controller.abort(new AppError('OPERATION_CANCELLED', 'The operation was cancelled.'));
    await current.save();
    return get(id);
  }

  function assertConnectionIdle(id: string): void {
    if (active?.job.connectionId === id) throw new AppError('CONNECTION_BUSY', 'This connection is used by the active operation.', 409);
  }

  async function close(): Promise<void> {
    if (!active) return;
    const current = active;
    await cancel(current.job.id);
    await current.done;
  }

  async function wait(id: string): Promise<Job> {
    if (active?.job.id === id) await active.done;
    return get(id);
  }

  return { start, get, cancel, close, wait, assertConnectionIdle, list: () => [...jobs.values()].map(job => structuredClone(job)) };
}

export type JobManager = Awaited<ReturnType<typeof createJobManager>>;
