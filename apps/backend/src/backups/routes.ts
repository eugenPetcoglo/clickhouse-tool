import { createReadStream } from 'node:fs';
import { Type } from '@sinclair/typebox';
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import type { FastifyInstance } from 'fastify';
import type { ClickHouseClient } from '../types.js';
import type { Config } from '../config.js';
import type { JobManager } from '../jobs/run.js';
import type { createConnectionService } from '../connections/service.js';
import { AppError } from '../errors.js';
import { createBackup } from './create.js';
import { checkBackup, getBackupPath, importBackup, listBackups } from './archive.js';
import { readBackupUpload } from './upload.js';

export function registerBackupRoutes(app: FastifyInstance, config: Config, client: ClickHouseClient, connections: Awaited<ReturnType<typeof createConnectionService>>, jobs: JobManager): void {
  const server = app.withTypeProvider<TypeBoxTypeProvider>();
  server.post('/api/backups', {
    schema: { tags: ['backups'], body: Type.Object({ sourceConnectionId: Type.String({ format: 'uuid' }), database: Type.String({ minLength: 1, maxLength: 255 }) }, { additionalProperties: false }) },
  }, async (request, reply) => {
    const connection = connections.require(request.body.sourceConnectionId);
    const job = await jobs.start('backup', request.body, context => createBackup({ client, connection, database: request.body.database, dataDir: config.dataDir }, context), request.body.sourceConnectionId);
    return reply.code(202).send({ jobId: job.id });
  });
  server.get('/api/backups', { schema: { tags: ['backups'] } }, () => listBackups(config.dataDir));
  server.get('/api/backups/:id/file', { schema: { tags: ['backups'], params: Type.Object({ id: Type.String({ format: 'uuid' }) }) } }, async (request, reply) => {
    const backup = await checkBackup(config.dataDir, request.params.id);
    await backup.close();
    return reply.type('application/octet-stream').header('Content-Disposition', `attachment; filename="${request.params.id}.chbackup"`).send(createReadStream(getBackupPath(config.dataDir, request.params.id)));
  });
  server.post('/api/backups/import', { schema: { tags: ['backups'], consumes: ['multipart/form-data'] } }, async (request, reply) => {
    const upload = await readBackupUpload(request.raw, config.maxUploadBytes);
    let jobId: string | undefined;
    const aborted = () => { if (jobId) void jobs.cancel(jobId).catch(() => undefined); };
    request.raw.once('aborted', aborted);
    try {
      const job = await jobs.start('import', {}, context => importBackup(config.dataDir, upload.stream, config.maxUploadBytes, context));
      jobId = job.id;
      // Keep the upload request open until its body is consumed; otherwise Fastify can close the stream.
      const completed = await jobs.wait(job.id);
      if (completed.status !== 'succeeded') throw new AppError(completed.error?.code ?? 'INVALID_BACKUP', completed.error?.message ?? 'Import failed.', completed.error?.code === 'UPLOAD_TOO_LARGE' ? 413 : 422);
      return reply.code(202).send({ jobId: job.id, backup: completed.result });
    } finally {
      request.raw.removeListener('aborted', aborted);
      upload.discard();
    }
  });
}
