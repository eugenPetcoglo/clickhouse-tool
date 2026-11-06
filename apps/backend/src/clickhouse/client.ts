import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Transform, type Readable, type Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { StringDecoder } from 'node:string_decoder';
import type { Config } from '../config.js';
import { AppError, errorMessage } from '../errors.js';
import { ensureDirectory } from '../files.js';
import type { ClickHouseClient, ConnectedConnection, QueryOptions, SchemaObject } from '../types.js';

const STDERR_LIMIT = 16 * 1024;

export function quoteIdentifier(value: string): string {
  if (!value || value.includes('\0')) throw new AppError('INVALID_IDENTIFIER', 'An SQL identifier is empty or contains a null byte.');
  return '`' + value.replaceAll('\\', '\\\\').replaceAll('`', '\\`').replaceAll('\n', '\\n').replaceAll('\r', '\\r').replaceAll('\t', '\\t') + '`';
}

export function storedColumns(table: SchemaObject): string[] {
  return table.columns
    .filter((column) => !['ALIAS', 'EPHEMERAL'].includes(column.defaultKind))
    .sort((a, b) => a.position - b.position)
    .map((column) => column.name);
}

export function createClickHouseClient(config: Config): ClickHouseClient {
  async function run(
    connection: ConnectedConnection,
    sql: string,
    options: QueryOptions = {},
    streams: { input?: Readable; output?: Writable; outputFormat?: 'JSONEachRow' } = {},
    timeoutMs = 30_000,
    cancelServerQuery = true,
  ): Promise<string> {
    const signal = options.signal ?? options.context?.signal;
    signal?.throwIfAborted();
    for (const name of Object.keys(options.parameters ?? {})) {
      if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) throw new AppError('INVALID_PARAMETER', 'Invalid query parameter name.');
    }
    const queryId = `clickhouse-tool-${randomUUID()}`;
    await options.context?.setQueryId(queryId);
    const runtime = join(config.dataDir, 'runtime');
    await ensureDirectory(runtime);
    const directory = await mkdtemp(join(runtime, 'cli-'));
    const configPath = join(directory, 'client.xml');
    const secure = connection.profile.tls;
    const clientConfig = `<config><host>${xml(connection.host)}</host><port>${connection.port}</port><user>${xml(connection.profile.username)}</user><password>${xml(connection.password)}</password><openSSL><client><verificationMode>strict</verificationMode><extendedVerification>true</extendedVerification><loadDefaultCAFile>true</loadDefaultCAFile><invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler></client></openSSL></config>`;
    try {
      await writeFile(configPath, clientConfig, { mode: 0o600 });
    } catch (error) {
      await rm(directory, { recursive: true, force: true });
      throw error;
    }
    const args = [
      '--config-file', configPath,
      '--host', connection.host, '--port', String(connection.port), '--user', connection.profile.username,
      '--query_id', queryId, '--query', sql,
      '--connect_timeout', '10', '--receive_timeout', '300', '--send_timeout', '300',
      '--progress', 'off', '--log-level', 'error',
      '--show_table_uuid_in_table_create_query_if_not_nil=0',
      secure ? '--secure' : '--no-secure',
    ];
    if (streams.outputFormat) args.push('--format', streams.outputFormat);
    if (secure) args.push('--tls-sni-override', connection.profile.tlsServerName ?? connection.profile.host);
    for (const [name, value] of Object.entries(options.parameters ?? {})) {
      args.push(`--param_${name}=${value}`);
    }

    let stderr = '';
    let output = '';
    let transferError: unknown;
    let spawnError: Error | undefined;
    let timedOut = false;
    let closed = false;
    let cancellation: Promise<void> | undefined;
    const child = spawn(config.cliPath, args, { shell: false, env: cleanEnvironment(), stdio: ['pipe', 'pipe', 'pipe'] });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk: string) => { stderr = (stderr + chunk).slice(-STDERR_LIMIT); });
    child.stdin.on('error', () => { /* pipeline() handles input failures; execute() has no input. */ });
    const completion = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
      child.once('error', (error) => { spawnError = error; });
      child.once('close', (code, processSignal) => { closed = true; resolve({ code, signal: processSignal }); });
    });
    const stop = () => {
      if (closed) return;
      child.kill('SIGTERM');
      const force = setTimeout(() => { if (!closed) child.kill('SIGKILL'); }, 2_000);
      force.unref();
    };
    const cancel = () => {
      // Closing the client is not proof that an INSERT did not commit anything.
      if (cancelServerQuery) cancellation ??= cancelQuery(connection, queryId).catch(() => undefined);
      stop();
    };
    const abort = () => { cancel(); };
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    const timer = timeoutMs > 0 ? setTimeout(() => { timedOut = true; cancel(); }, timeoutMs) : undefined;
    timer?.unref();
    const trackFailure = async (task: Promise<unknown>) => {
      try { await task; } catch (error) { transferError ??= error; stop(); }
    };
    const outputTask = streams.output
      ? trackFailure(pipeline(child.stdout, streams.output))
      : trackFailure((async () => {
        let bytes = 0;
        const decoder = new StringDecoder('utf8');
        for await (const chunk of child.stdout) {
          const buffer = Buffer.from(chunk as Uint8Array);
          bytes += buffer.length;
          if (bytes > config.metadataLimitBytes) throw new AppError('METADATA_TOO_LARGE', 'ClickHouse returned too much metadata.');
          output += decoder.write(buffer);
        }
        output += decoder.end();
      })());
    const inputTask = streams.input ? trackFailure(pipeline(streams.input, child.stdin)) : Promise.resolve(child.stdin.end()).then(() => undefined);
    try {
      const result = await completion;
      await Promise.all([inputTask, outputTask]);
      await cancellation;
      if (signal?.aborted) throw new AppError('OPERATION_CANCELLED', 'The operation was cancelled.', 409);
      if (timedOut) throw new AppError('CLICKHOUSE_TIMEOUT', 'ClickHouse did not finish the request before the timeout.', 504);
      if (spawnError) throw new AppError('CLI_UNAVAILABLE', `Cannot start clickhouse-client: ${spawnError.message}`, 503);
      if (transferError instanceof AppError) throw transferError;
      if (result.code !== 0) {
        const safeError = connection.password ? stderr.replaceAll(connection.password, '[redacted]') : stderr;
        throw new AppError('CLICKHOUSE_FAILED', safeError.trim() || `clickhouse-client exited with code ${result.code ?? result.signal}.`, 502);
      }
      if (transferError) throw new AppError('TRANSFER_FAILED', errorMessage(transferError), 500);
      return output;
    } finally {
      if (timer) clearTimeout(timer);
      signal?.removeEventListener('abort', abort);
      stop();
      await rm(directory, { recursive: true, force: true });
    }
  }

  async function cancelQuery(connection: ConnectedConnection, queryId: string): Promise<void> {
    await run(connection, 'KILL QUERY WHERE query_id = {queryId:String} SYNC', { parameters: { queryId } }, {}, 10_000, false);
  }

  return {
    async version() {
      return new Promise<string>((resolve, reject) => {
        const child = spawn(config.cliPath, ['--version'], { shell: false, stdio: ['ignore', 'pipe', 'ignore'] });
        let output = '';
        const timer = setTimeout(() => { child.kill('SIGKILL'); }, 5_000);
        child.stdout.on('data', (chunk: Buffer) => { output = (output + chunk.toString()).slice(0, 4_096); });
        child.once('error', () => reject(new AppError('CLI_UNAVAILABLE', 'clickhouse-client is not available. Set CLICKHOUSE_CLIENT_PATH.', 503)));
        child.once('close', (code) => {
          clearTimeout(timer);
          const version = output.match(/\b(\d+\.\d+\.\d+(?:\.\d+)?)\b/)?.[1];
          if (code === 0 && version) resolve(version);
          else reject(new AppError('CLI_UNAVAILABLE', 'Could not determine clickhouse-client version.', 503));
        });
      });
    },
    async query<T>(connection: ConnectedConnection, sql: string, options?: QueryOptions): Promise<T[]> {
      // A CLI output format also works for EXPLAIN AST CREATE, where a trailing
      // FORMAT clause would belong to the inner CREATE rather than EXPLAIN.
      const output = await run(connection, sql.trim().replace(/;$/, ''), options, { outputFormat: 'JSONEachRow' });
      try {
        return output.split('\n').filter((line) => line.trim()).map((line) => JSON.parse(line) as T);
      } catch {
        throw new AppError('INVALID_CLICKHOUSE_RESPONSE', 'ClickHouse returned invalid JSON metadata.', 502);
      }
    },
    async execute(connection, sql, options) {
      await run(connection, sql, options, {}, 60_000);
    },
    async exportTable(connection, database, table, output, context) {
      const columns = storedColumns(table);
      if (!columns.length) throw new AppError('UNSUPPORTED_SCHEMA', `Table ${table.name} has no stored columns.`);
      const sql = `SELECT ${columns.map(quoteIdentifier).join(', ')} FROM ${quoteIdentifier(database)}.${quoteIdentifier(table.name)} FORMAT Native`;
      const counter = new Transform({ transform(chunk: Buffer, _encoding, callback) { context.addBytes(chunk.length); callback(null, chunk); } });
      const writing = pipeline(counter, output);
      const exporting = run(connection, sql, { context }, { output: counter }, 0).catch(error => {
        counter.destroy(error instanceof Error ? error : new Error(String(error)));
        throw error;
      });
      const results = await Promise.allSettled([exporting, writing]);
      const failure = results.find(result => result.status === 'rejected');
      if (failure?.status === 'rejected') throw failure.reason;
    },
    async importTable(connection, database, table, input, context) {
      const columns = storedColumns(table);
      const counter = new Transform({ transform(chunk: Buffer, _encoding, callback) { context.addBytes(chunk.length); callback(null, chunk); } });
      const sql = `INSERT INTO ${quoteIdentifier(database)}.${quoteIdentifier(table.name)} (${columns.map(quoteIdentifier).join(', ')}) SETTINGS insert_allow_materialized_columns=1, async_insert=0, optimize_on_insert=0 FORMAT Native`;
      // Keep both errors observable: failure of the CLI must destroy the input too.
      const inputPipeline = pipeline(input, counter).catch((error: unknown) => { counter.destroy(error instanceof Error ? error : new Error(String(error))); });
      try { await run(connection, sql, { context }, { input: counter }, 0); }
      finally { input.destroy(); counter.destroy(); await inputPipeline; }
    },
    cancelQuery,
  };
}

function xml(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&apos;');
}

function cleanEnvironment(): NodeJS.ProcessEnv {
  const environment = { ...process.env };
  for (const key of Object.keys(environment)) if (key.startsWith('CLICKHOUSE_')) delete environment[key];
  return environment;
}
