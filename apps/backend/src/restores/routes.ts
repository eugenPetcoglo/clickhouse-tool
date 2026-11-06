import { Type } from '@sinclair/typebox';
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import type { FastifyInstance } from 'fastify';
import type { Config } from '../config.js';
import type { createConnectionService } from '../connections/service.js';
import { AppError } from '../errors.js';
import type { JobManager } from '../jobs/run.js';
import type { ClickHouseClient } from '../types.js';
import { checkRestore } from './check.js';
import { restoreExistingDatabase } from './existing.js';
import { restoreNewDatabase } from './new.js';
import type { RestoreCheck, RestoreRecovery } from './types.js';

export function registerRestoreRoutes(app: FastifyInstance, config: Config, client: ClickHouseClient, connections: ReturnType<typeof createConnectionService>, jobs: JobManager): void {
  const server = app.withTypeProvider<TypeBoxTypeProvider>();
  server.post('/api/restores/check', {
    schema: { tags: ['restores'], body: Type.Object({
      backupId: Type.String({ format: 'uuid' }),
      targetConnectionId: Type.String({ format: 'uuid' }),
      mode: Type.Union([Type.Literal('replace-data'), Type.Literal('create-new')]),
      recoveryJobId: Type.Optional(Type.String({ format: 'uuid' })),
    }, { additionalProperties: false }) },
  }, async (request, reply) => {
    const input = request.body;
    const target = connections.require(input.targetConnectionId);
    const recovery = readRecovery(jobs, input.recoveryJobId, input.backupId, input.targetConnectionId, input.mode);
    const job = await jobs.start('restore-check', input, context => checkRestore({
      client, target, dataDir: config.dataDir, backupId: input.backupId, mode: input.mode, recovery,
    }, context), input.targetConnectionId);
    return reply.code(202).send({ jobId: job.id });
  });

  server.post('/api/restores', {
    schema: { tags: ['restores'], body: Type.Object({
      checkJobId: Type.String({ format: 'uuid' }),
      confirmationName: Type.String({ minLength: 1, maxLength: 255 }),
      targetWritesPaused: Type.Literal(true),
    }, { additionalProperties: false }) },
  }, async (request, reply) => {
    const checked = jobs.get(request.body.checkJobId);
    if (checked.kind !== 'restore-check' || checked.status !== 'succeeded') {
      throw new AppError('RESTORE_CHECK_REQUIRED', 'Complete a restore check before confirming the operation.', 409);
    }
    const check = checked.result as RestoreCheck;
    if (request.body.confirmationName !== check.database) {
      throw new AppError('CONFIRMATION_MISMATCH', 'Type the exact destination database name to confirm.');
    }
    const target = connections.require(check.targetConnectionId);
    const recoveryJobId = typeof checked.input.recoveryJobId === 'string' ? checked.input.recoveryJobId : undefined;
    const recovery = readRecovery(jobs, recoveryJobId, check.backupId, check.targetConnectionId, check.mode);
    const input = { client, target, dataDir: config.dataDir, backupId: check.backupId, mode: check.mode, expectedCheck: check, recovery };
    const job = await jobs.start('restore', {
      backupId: check.backupId, targetConnectionId: check.targetConnectionId, mode: check.mode, check, recoveryJobId,
    }, context => check.mode === 'replace-data' ? restoreExistingDatabase(input, context) : restoreNewDatabase(input, context), check.targetConnectionId);
    return reply.code(202).send({ jobId: job.id });
  });
}

function readRecovery(jobs: JobManager, id: string | undefined, backupId: string, targetId: string, mode: string, visited = new Set<string>()): RestoreRecovery | undefined {
  if (!id) return undefined;
  if (visited.has(id) || visited.size >= 100) throw new AppError('INVALID_RECOVERY_JOB', 'The recovery journal chain is invalid.', 409);
  visited.add(id);
  const job = jobs.get(id);
  if (job.kind !== 'restore' || !job.requiresRecovery || !['failed', 'cancelled', 'interrupted'].includes(job.status)
    || job.input.backupId !== backupId || job.input.targetConnectionId !== targetId || job.input.mode !== mode) {
    throw new AppError('INVALID_RECOVERY_JOB', 'Select the interrupted restore for this backup and destination.', 409);
  }
  const check = job.input.check as RestoreCheck;
  if (!check?.database || !check.targetServerId || !check.backupFingerprint) {
    throw new AppError('INVALID_RECOVERY_JOB', 'The recovery journal has no verified destination.', 409);
  }
  const previousId = typeof job.input.recoveryJobId === 'string' ? job.input.recoveryJobId : undefined;
  const previous = readRecovery(jobs, previousId, backupId, targetId, mode, visited);
  if (previous && (previous.targetServerId !== check.targetServerId || previous.backupFingerprint !== check.backupFingerprint)) {
    throw new AppError('INVALID_RECOVERY_JOB', 'Recovery journals refer to different backups or servers.', 409);
  }
  return { backupId, targetServerId: check.targetServerId, database: check.database, backupFingerprint: check.backupFingerprint, changes: [...(previous?.changes ?? []), ...job.changes] };
}
