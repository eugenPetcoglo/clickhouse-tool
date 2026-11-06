import Fastify from 'fastify';
import multipart from '@fastify/multipart';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import type { Config } from './config.js';
import { AppError } from './errors.js';
import { errorHandler } from './error-handler.js';
import { createClickHouseClient } from './clickhouse/client.js';
import { createConnectionService } from './connections/service.js';
import { registerConnectionRoutes } from './connections/routes.js';
import { registerBackupRoutes } from './backups/routes.js';
import { registerRestoreRoutes } from './restores/routes.js';
import { createJobManager } from './jobs/run.js';
import { registerJobRoutes } from './jobs/routes.js';
import type { ClickHouseClient } from './types.js';

export async function createApp(config: Config, dependencies: { client?: ClickHouseClient } = {}) {
  const app = Fastify({
    logger: false,
    bodyLimit: 64 * 1024,
    requestTimeout: 0,
    ajv: { customOptions: { removeAdditional: false } },
  });
  const client = dependencies.client ?? createClickHouseClient(config);
  const connections = createConnectionService(config, client);
  const jobs = await createJobManager(config.dataDir);
  app.setErrorHandler(errorHandler);

  app.addHook('onRequest', async (request, reply) => {
    const host = request.headers.host ?? '';
    const hostname = URL.parse(`http://${host}`)?.hostname;
    if (!hostname || !['localhost', '127.0.0.1', '[::1]'].includes(hostname)) throw new AppError('INVALID_HOST', 'Only local application requests are allowed.', 403);
    const origin = request.headers.origin;
    if (origin && origin !== `http://${host}` && !config.allowedOrigins.includes(origin)) {
      throw new AppError('INVALID_ORIGIN', 'This browser origin is not allowed.', 403);
    }
    if (origin) {
      reply.header('Access-Control-Allow-Origin', origin).header('Vary', 'Origin');
    }
    if (request.method === 'OPTIONS') {
      return reply.header('Access-Control-Allow-Methods', 'GET,POST,PATCH,DELETE,OPTIONS')
        .header('Access-Control-Allow-Headers', 'Content-Type,X-Clickhouse-Tool').code(204).send();
    }
    if (request.url.startsWith('/api/') && !['GET', 'HEAD'].includes(request.method) && request.headers['x-clickhouse-tool'] !== '1') {
      throw new AppError('APP_HEADER_REQUIRED', 'Send X-Clickhouse-Tool: 1 for application commands.', 403);
    }
  });
  await app.register(multipart, { limits: { files: 1, fields: 0, fileSize: config.maxUploadBytes } });
  await app.register(swagger, {
    transform({ schema, url }) {
      if (url !== '/api/backups/import') return { schema, url };
      // Describe the streamed form in OpenAPI without asking AJV to buffer it.
      return { schema: { ...schema, body: { type: 'object', required: ['file'], properties: { file: { type: 'string', format: 'binary' } } } }, url };
    },
    openapi: {
      info: { title: 'Clickhouse-tool API', version: '0.1.0', description: 'Local manual ClickHouse backup and restore. Set the application header to 1 for commands.' },
      components: { securitySchemes: { application: { type: 'apiKey', in: 'header', name: 'X-Clickhouse-Tool' } } },
      security: [{ application: [] }],
    },
  });
  await app.register(swaggerUi, { routePrefix: '/docs' });

  app.get('/api/health', { schema: { tags: ['health'] } }, async () => ({ ready: true, cliVersion: await client.version() }));
  app.get('/api/openapi.json', { schema: { hide: true } }, () => app.swagger());
  app.get('/api/connections/:id/databases', {
    schema: { tags: ['connections'], params: { type: 'object', properties: { id: { type: 'string', format: 'uuid' } }, required: ['id'] } },
  }, async request => {
    const { id } = request.params as { id: string };
    const connection = connections.require(id);
    return client.query<{ name: string }>(connection, "SELECT name FROM system.databases WHERE name NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY name");
  });
  registerConnectionRoutes(app, connections, jobs);
  registerBackupRoutes(app, config, client, connections, jobs);
  registerRestoreRoutes(app, config, client, connections, jobs);
  registerJobRoutes(app, jobs);
  app.addHook('preClose', async () => { await jobs.close(); });
  app.addHook('onClose', async () => { await connections.close(); });
  return app;
}
