import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

export interface TestServer {
  port: number;
  directory: string;
  close(): Promise<void>;
}

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('No free test port');
  await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  return address.port;
}

async function stop(child: ChildProcess): Promise<void> {
  if (child.pid === undefined || child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>(resolve => child.once('close', () => resolve()));
  child.kill('SIGTERM');
  const force = setTimeout(() => child.kill('SIGKILL'), 5000);
  try { await closed; } finally { clearTimeout(force); }
}

export async function startTestServer(): Promise<TestServer> {
  const directory = await mkdtemp(join(tmpdir(), 'clickhouse-tool-e2e-server-'));
  const port = await freePort();
  const httpPort = await freePort();
  const configPath = join(directory, 'config.xml');
  await mkdir(join(directory, 'data'));
  await writeFile(configPath, `<clickhouse>
  <logger><level>warning</level><log>${directory}/server.log</log><errorlog>${directory}/error.log</errorlog><console>false</console></logger>
  <listen_host>127.0.0.1</listen_host><tcp_port>${port}</tcp_port><http_port>${httpPort}</http_port>
  <path>${directory}/data/</path><tmp_path>${directory}/tmp/</tmp_path><user_files_path>${directory}/user_files/</user_files_path><format_schema_path>${directory}/format_schemas/</format_schema_path>
  <max_thread_pool_size>128</max_thread_pool_size><max_connections>64</max_connections>
  <background_schedule_pool_size>4</background_schedule_pool_size><background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size><background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size><background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <background_pool_size>4</background_pool_size><background_move_pool_size>2</background_move_pool_size><background_fetches_pool_size>2</background_fetches_pool_size><background_common_pool_size>2</background_common_pool_size>
  <merge_tree><number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition></merge_tree>
  <mark_cache_size>16777216</mark_cache_size><uncompressed_cache_size>16777216</uncompressed_cache_size><mlock_executable>false</mlock_executable>
  <profiles><default><max_threads>1</max_threads><max_memory_usage>268435456</max_memory_usage></default></profiles>
  <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
  <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>`);
  const child = spawn(process.env.CLICKHOUSE_SERVER_PATH ?? 'clickhouse', ['server', '--config-file', configPath], { stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  let spawnError: Error | undefined;
  child.stderr?.on('data', (chunk: Buffer) => { stderr = (stderr + chunk.toString()).slice(-12000); });
  child.on('error', error => { spawnError = error; });
  try {
    for (let attempt = 0; attempt < 150; attempt++) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null || child.signalCode !== null) throw new Error('ClickHouse test server exited');
      try {
        const response = await fetch(`http://127.0.0.1:${httpPort}/ping`, { signal: AbortSignal.timeout(300) });
        if (response.ok) return { port, directory, async close() { await stop(child); await rm(directory, { recursive: true, force: true }); } };
      } catch { /* Readiness check only; each test server owns its temporary data. */ }
      await delay(100);
    }
    throw new Error('ClickHouse test server did not become ready');
  } catch (error) {
    await stop(child);
    const log = await readFile(join(directory, 'error.log'), 'utf8').catch(() => '');
    await rm(directory, { recursive: true, force: true });
    throw new Error(`${error instanceof Error ? error.message : String(error)}\n${stderr}\n${log.slice(-12000)}`);
  }
}
