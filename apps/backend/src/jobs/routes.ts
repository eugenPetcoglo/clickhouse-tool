import { Type } from '@sinclair/typebox';
import type { FastifyInstance } from 'fastify';
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import type { JobManager } from './run.js';

export function registerJobRoutes(app: FastifyInstance, jobs: JobManager): void {
  const server = app.withTypeProvider<TypeBoxTypeProvider>();
  const params = Type.Object({ id: Type.String({ format: 'uuid' }) });
  server.get('/api/jobs', { schema: { tags: ['jobs'] } }, () => jobs.list());
  server.get('/api/jobs/:id', { schema: { tags: ['jobs'], params } }, request => jobs.get(request.params.id));
  server.post('/api/jobs/:id/cancel', { schema: { tags: ['jobs'], params } }, request => jobs.cancel(request.params.id));
}
