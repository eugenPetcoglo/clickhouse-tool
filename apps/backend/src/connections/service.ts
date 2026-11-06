import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import type { Config } from '../config.js';
import { AppError } from '../errors.js';
import { readJson, writeJson } from '../files.js';
import type { ClickHouseClient, ConnectedConnection, ConnectionProfile, SshSettings } from '../types.js';
import { openSshTunnel, type SshTunnel } from './ssh.js';

type ProfileInput = Omit<ConnectionProfile, 'id'>;
interface Session { connection: ConnectedConnection; tunnel?: SshTunnel }

export function createConnectionService(config: Config, client: ClickHouseClient) {
  const path = join(config.dataDir, 'connections.json');
  const sessions = new Map<string, Session>();
  const connecting = new Map<string, Promise<void>>();
  let closing = false;
  let writes = Promise.resolve();

  async function readProfiles(): Promise<ConnectionProfile[]> {
    const profiles = await readJson<ConnectionProfile[]>(path, []);
    if (!Array.isArray(profiles)) throw new AppError('INVALID_PROFILES', 'Saved connection profiles are invalid.', 500);
    return profiles.map((profile) => cleanProfile(profile.id, profile));
  }

  async function change<T>(update: (profiles: ConnectionProfile[]) => { profiles: ConnectionProfile[]; result: T }): Promise<T> {
    const task = writes.then(async () => {
      const updated = update(await readProfiles());
      await writeJson(path, updated.profiles);
      return updated.result;
    });
    writes = task.then(() => undefined, () => undefined);
    return task;
  }

  function assertDisconnected(id: string): void {
    if (sessions.has(id) || connecting.has(id)) throw new AppError('CONNECTION_IN_USE', 'Disconnect this profile before changing it.', 409);
  }

  async function getProfile(id: string): Promise<ConnectionProfile> {
    const profile = (await readProfiles()).find((candidate) => candidate.id === id);
    if (!profile) throw new AppError('CONNECTION_NOT_FOUND', 'Connection profile was not found.', 404);
    return profile;
  }

  return {
    async list(): Promise<Array<ConnectionProfile & { connected: boolean }>> {
      return (await readProfiles()).map((profile) => {
        const session = sessions.get(profile.id);
        const connected = !!session && (!session.tunnel || session.tunnel.alive());
        if (!connected) sessions.delete(profile.id);
        return { ...profile, connected };
      });
    },
    async create(input: ProfileInput): Promise<ConnectionProfile> {
      const profile = cleanProfile(randomUUID(), input);
      return change((profiles) => ({ profiles: [...profiles, profile], result: profile }));
    },
    async update(id: string, patch: Partial<ProfileInput>): Promise<ConnectionProfile> {
      assertDisconnected(id);
      return change((profiles) => {
        assertDisconnected(id);
        const current = profiles.find((profile) => profile.id === id);
        if (!current) throw new AppError('CONNECTION_NOT_FOUND', 'Connection profile was not found.', 404);
        const updated = cleanProfile(id, { ...current, ...patch });
        return { profiles: profiles.map((profile) => profile.id === id ? updated : profile), result: updated };
      });
    },
    async remove(id: string): Promise<void> {
      assertDisconnected(id);
      await change((profiles) => {
        assertDisconnected(id);
        if (!profiles.some((profile) => profile.id === id)) throw new AppError('CONNECTION_NOT_FOUND', 'Connection profile was not found.', 404);
        return { profiles: profiles.filter((profile) => profile.id !== id), result: undefined };
      });
    },
    async connect(id: string, password: string): Promise<{ serverVersion: string; serverId: string }> {
      if (closing) throw new AppError('APP_CLOSING', 'The application is shutting down.', 503);
      if (sessions.has(id) || connecting.has(id)) throw new AppError('ALREADY_CONNECTED', 'This connection is already connected or connecting.', 409);
      if (typeof password !== 'string' || password.includes('\0')) throw new AppError('INVALID_PASSWORD', 'Invalid password.');
      let finishAttempt = () => {};
      connecting.set(id, new Promise<void>((resolve) => { finishAttempt = resolve; }));
      let tunnel: SshTunnel | undefined;
      try {
        const profile = await getProfile(id);
        await client.version();
        if (profile.ssh) tunnel = await openSshTunnel(profile);
        const connection: ConnectedConnection = {
          profile, password, host: tunnel?.host ?? profile.host, port: tunnel?.port ?? profile.port,
          serverVersion: '', serverId: '',
        };
        const result = (await client.query<{ version: string; serverId: string }>(connection, 'SELECT version() AS version, toString(serverUUID()) AS serverId'))[0];
        if (!result?.version || !result.serverId) throw new AppError('UNSUPPORTED_VERSION', 'ClickHouse did not return the required server identity.');
        const requiredSettings = ['insert_allow_materialized_columns', 'async_insert', 'optimize_on_insert', 'show_table_uuid_in_table_create_query_if_not_nil'];
        const settings = await client.query<{ name: string }>(connection, "SELECT name FROM system.settings WHERE name IN ('insert_allow_materialized_columns', 'async_insert', 'optimize_on_insert', 'show_table_uuid_in_table_create_query_if_not_nil')");
        if (requiredSettings.some((name) => !settings.some((setting) => setting.name === name))) {
          throw new AppError('UNSUPPORTED_VERSION', 'This ClickHouse version lacks the settings needed for safe backup and restore.');
        }
        const parsed = await client.query<{ explain: string }>(connection, 'EXPLAIN AST SELECT 1');
        if (!parsed.some(row => typeof row.explain === 'string' && row.explain.trimStart().startsWith('SelectWithUnionQuery'))) {
          throw new AppError('UNSUPPORTED_VERSION', 'This ClickHouse version cannot return the SQL syntax tree required for safe restore.');
        }
        connection.serverVersion = result.version;
        connection.serverId = result.serverId;
        if (closing) throw new AppError('APP_CLOSING', 'The application is shutting down.', 503);
        sessions.set(id, { connection, ...(tunnel ? { tunnel } : {}) });
        return { serverVersion: result.version, serverId: result.serverId };
      } catch (error) {
        await tunnel?.close();
        throw error;
      } finally {
        connecting.delete(id);
        finishAttempt();
      }
    },
    async disconnect(id: string): Promise<void> {
      if (connecting.has(id)) throw new AppError('CONNECTION_IN_USE', 'Wait for the connection attempt to finish.', 409);
      await getProfile(id);
      const session = sessions.get(id);
      sessions.delete(id);
      await session?.tunnel?.close();
    },
    require(id: string): ConnectedConnection {
      const session = sessions.get(id);
      if (!session || (session.tunnel && !session.tunnel.alive())) {
        sessions.delete(id);
        throw new AppError('CONNECTION_REQUIRED', 'Connect to ClickHouse before starting this operation.', 409);
      }
      return session.connection;
    },
    async close(): Promise<void> {
      closing = true;
      await Promise.all(connecting.values());
      const tunnels = [...sessions.values()].map((session) => session.tunnel);
      sessions.clear();
      await Promise.all(tunnels.map((tunnel) => tunnel?.close()));
    },
  };
}

function cleanProfile(id: string, input: ProfileInput): ConnectionProfile {
  if (!/^[a-f0-9-]{36}$/.test(id)) throw new AppError('INVALID_PROFILE', 'Invalid connection identifier.');
  if (typeof input.name !== 'string' || !input.name.trim() || input.name.length > 200) throw new AppError('INVALID_PROFILE', 'A connection name is required.');
  validateHost(input.host);
  validatePort(input.port);
  if (typeof input.username !== 'string' || !input.username || /[\0\r\n]/.test(input.username)) throw new AppError('INVALID_PROFILE', 'A valid ClickHouse username is required.');
  if (typeof input.tls !== 'boolean') throw new AppError('INVALID_PROFILE', 'TLS must be a boolean.');
  if (input.tlsServerName !== undefined) validateHost(input.tlsServerName);
  let ssh: SshSettings | undefined;
  if (input.ssh) {
    validateHost(input.ssh.host);
    validatePort(input.ssh.port);
    if (typeof input.ssh.username !== 'string' || !input.ssh.username || /[\0\r\n]/.test(input.ssh.username)) throw new AppError('INVALID_SSH', 'An SSH username is required.');
    if (input.ssh.identityFile !== undefined && (typeof input.ssh.identityFile !== 'string' || /[\0\r\n]/.test(input.ssh.identityFile))) throw new AppError('INVALID_SSH', 'Invalid SSH identity file path.');
    ssh = { host: input.ssh.host, port: input.ssh.port, username: input.ssh.username, ...(input.ssh.identityFile ? { identityFile: input.ssh.identityFile } : {}) };
  }
  // Pick known fields explicitly: accidental password fields never reach disk.
  return { id, name: input.name.trim(), host: input.host, port: input.port, username: input.username, tls: input.tls, ...(input.tlsServerName ? { tlsServerName: input.tlsServerName } : {}), ...(ssh ? { ssh } : {}) };
}

function validateHost(host: string): void {
  if (typeof host !== 'string' || !/^[A-Za-z0-9_:[\].-]+$/.test(host) || host.startsWith('-') || host.length > 255) throw new AppError('INVALID_PROFILE', 'Use a valid IP address or DNS hostname.');
}

function validatePort(port: number): void {
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new AppError('INVALID_PROFILE', 'Port must be between 1 and 65535.');
}
