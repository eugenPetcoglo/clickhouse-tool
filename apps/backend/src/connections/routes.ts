import { Type } from '@sinclair/typebox';
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import type { FastifyInstance } from 'fastify';
import type { createConnectionService } from './service.js';
import type { JobManager } from '../jobs/run.js';

const Profile = Type.Object({
  name: Type.String({ minLength: 1, maxLength: 100 }),
  host: Type.String({ minLength: 1, maxLength: 255 }),
  port: Type.Integer({ minimum: 1, maximum: 65535, default: 9000 }),
  username: Type.String({ minLength: 1, maxLength: 255, default: 'default' }),
  tls: Type.Boolean({ default: false }),
  tlsServerName: Type.Optional(Type.String({ minLength: 1, maxLength: 255 })),
  ssh: Type.Optional(Type.Object({
    host: Type.String({ minLength: 1, maxLength: 255 }),
    port: Type.Integer({ minimum: 1, maximum: 65535, default: 22 }),
    username: Type.String({ minLength: 1, maxLength: 255 }),
    identityFile: Type.Optional(Type.String({ minLength: 1, maxLength: 4096 })),
  }, { additionalProperties: false })),
}, { additionalProperties: false });

const Id = Type.Object({ id: Type.String({ format: 'uuid' }) });

export function registerConnectionRoutes(app: FastifyInstance, connections: Awaited<ReturnType<typeof createConnectionService>>, jobs: JobManager): void {
  const server = app.withTypeProvider<TypeBoxTypeProvider>();
  server.get('/api/connections', { schema: { tags: ['connections'] } }, () => connections.list());
  server.post('/api/connections', { schema: { tags: ['connections'], body: Profile } }, async (request, reply) => {
    return reply.code(201).send(await connections.create(request.body));
  });
  server.patch('/api/connections/:id', { schema: { tags: ['connections'], params: Id, body: Type.Partial(Profile, { additionalProperties: false }) } }, request => {
    jobs.assertConnectionIdle(request.params.id);
    return connections.update(request.params.id, request.body);
  });
  server.delete('/api/connections/:id', { schema: { tags: ['connections'], params: Id } }, async (request, reply) => {
    jobs.assertConnectionIdle(request.params.id);
    await connections.remove(request.params.id);
    return reply.code(204).send();
  });
  server.post('/api/connections/:id/connect', {
    schema: { tags: ['connections'], params: Id, body: Type.Object({ password: Type.Optional(Type.String({ maxLength: 8192 })) }, { additionalProperties: false }) },
  }, request => {
    jobs.assertConnectionIdle(request.params.id);
    return connections.connect(request.params.id, request.body.password ?? '');
  });
  server.post('/api/connections/:id/disconnect', { schema: { tags: ['connections'], params: Id } }, async (request, reply) => {
    jobs.assertConnectionIdle(request.params.id);
    await connections.disconnect(request.params.id);
    return reply.code(204).send();
  });
}
