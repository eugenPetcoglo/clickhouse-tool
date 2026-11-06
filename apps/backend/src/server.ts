import { createApp } from './app.js';
import { loadConfig } from './config.js';

async function main(): Promise<void> {
  const config = loadConfig();
  const app = await createApp(config);
  let stopping = false;
  async function stop(): Promise<void> {
    if (stopping) return;
    stopping = true;
    await app.close();
  }
  process.once('SIGINT', () => { void stop(); });
  process.once('SIGTERM', () => { void stop(); });
  await app.listen({ host: config.host, port: config.port });
  console.log(`Clickhouse-tool: http://${config.host}:${config.port}/docs`);
}

main().catch(() => {
  console.error('Clickhouse-tool could not start. Check configuration, the local port and DATA_DIR permissions.');
  process.exitCode = 1;
});
