import type { FastifyError, FastifyReply, FastifyRequest } from 'fastify';
import { AppError } from './errors.js';

/** Fastify forwards thrown errors and rejected request promises here. */
export function errorHandler(error: FastifyError, request: FastifyRequest, reply: FastifyReply): FastifyReply {
  if (request.routeOptions.url === '/api/health' && error instanceof AppError && error.code === 'CLI_UNAVAILABLE') {
    return reply.code(503).send({ ready: false, error: 'Install clickhouse-client and check CLICKHOUSE_CLIENT_PATH.' });
  }
  if (error instanceof AppError) {
    return reply.code(error.statusCode).send({ error: { code: error.code, message: error.message } });
  }
  if (error.validation) {
    return reply.code(400).send({ error: { code: 'INVALID_REQUEST', message: error.message } });
  }
  if (error.statusCode && error.statusCode < 500) {
    return reply.code(error.statusCode).send({ error: { code: error.code, message: error.message } });
  }
  return reply.code(500).send({ error: { code: 'INTERNAL_ERROR', message: 'The operation failed. Check local storage and application configuration.' } });
}
