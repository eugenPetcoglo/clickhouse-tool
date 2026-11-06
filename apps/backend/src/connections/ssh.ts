import { spawn } from 'node:child_process';
import { createConnection, createServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { AppError } from '../errors.js';
import type { ConnectionProfile } from '../types.js';

export interface SshTunnel {
  host: string;
  port: number;
  alive(): boolean;
  close(): Promise<void>;
}

export async function openSshTunnel(profile: ConnectionProfile): Promise<SshTunnel> {
  const ssh = profile.ssh;
  if (!ssh) throw new AppError('INVALID_SSH', 'SSH settings are missing.');
  const port = await availablePort();
  const forwardHost = profile.host.includes(':') ? `[${profile.host}]` : profile.host;
  const args = [
    '-N', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
    '-o', 'ExitOnForwardFailure=yes', '-o', 'ConnectTimeout=10',
    '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
    '-L', `127.0.0.1:${port}:${forwardHost}:${profile.port}`,
    '-p', String(ssh.port), '-l', ssh.username,
  ];
  if (ssh.identityFile) args.push('-i', ssh.identityFile);
  args.push(ssh.host);
  const child = spawn('ssh', args, { shell: false, stdio: ['ignore', 'ignore', 'pipe'] });
  let stderr = '';
  let spawnError: Error | undefined;
  let closed = false;
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk: string) => { stderr = (stderr + chunk).slice(-8_192); });
  child.once('error', (error) => { spawnError = error; });
  const completion = new Promise<void>((resolve) => { child.once('close', () => { closed = true; resolve(); }); });
  const close = async () => {
    if (closed) return;
    child.kill('SIGTERM');
    const force = setTimeout(() => { if (!closed) child.kill('SIGKILL'); }, 2_000);
    force.unref();
    await completion;
    clearTimeout(force);
  };
  try {
    const deadline = Date.now() + 12_000;
    while (Date.now() < deadline) {
      if (closed || spawnError) throw new AppError('SSH_FAILED', stderr.trim() || spawnError?.message || 'SSH tunnel exited before it was ready.', 502);
      if (await acceptsConnection(port)) return { host: '127.0.0.1', port, alive: () => !closed && !child.killed, close };
      await delay(50);
    }
    throw new AppError('SSH_TIMEOUT', 'SSH tunnel did not become ready. Check the SSH key, agent and known_hosts.', 504);
  } catch (error) {
    await close();
    throw error;
  }
}

async function availablePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const address = server.address();
  if (!address || typeof address === 'string') throw new AppError('SSH_FAILED', 'Could not reserve a local SSH port.', 500);
  const port = address.port;
  await new Promise<void>((resolve, reject) => { server.close((error) => error ? reject(error) : resolve()); });
  return port;
}

async function acceptsConnection(port: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const socket = createConnection({ host: '127.0.0.1', port });
    socket.setTimeout(200);
    const finish = (ready: boolean) => { socket.destroy(); resolve(ready); };
    socket.once('connect', () => finish(true));
    socket.once('error', () => finish(false));
    socket.once('timeout', () => finish(false));
  });
}
