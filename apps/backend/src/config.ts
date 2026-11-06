import { homedir } from 'node:os';
import { join, resolve } from 'node:path';
import { AppError } from './errors.js';

export interface Config {
  host: string;
  port: number;
  dataDir: string;
  cliPath: string;
  allowedOrigins: string[];
  maxUploadBytes: number;
  metadataLimitBytes: number;
}

export function loadConfig(environment: NodeJS.ProcessEnv = process.env): Config {
  const host = environment.HOST ?? '127.0.0.1';
  if (!['127.0.0.1', '::1', 'localhost'].includes(host)) {
    throw new AppError('INVALID_CONFIG', 'HOST must be a loopback address.');
  }
  return {
    host,
    port: positiveNumber(environment.PORT, 3001, 'PORT', 65535),
    dataDir: resolve(environment.DATA_DIR ?? join(environment.XDG_DATA_HOME ?? join(homedir(), '.local/share'), 'clickhouse-tool')),
    cliPath: environment.CLICKHOUSE_CLIENT_PATH ?? 'clickhouse-client',
    allowedOrigins: (environment.ALLOWED_ORIGINS ?? 'http://localhost:5173,http://127.0.0.1:5173').split(',').filter(Boolean),
    maxUploadBytes: positiveNumber(environment.MAX_BACKUP_BYTES, 1024 ** 4, 'MAX_BACKUP_BYTES'),
    metadataLimitBytes: 32 * 1024 ** 2,
  };
}

function positiveNumber(value: string | undefined, fallback: number, name: string, maximum = Number.MAX_SAFE_INTEGER): number {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number < 1 || number > maximum) {
    throw new AppError('INVALID_CONFIG', `${name} must be a positive integer no larger than ${maximum}.`);
  }
  return number;
}
