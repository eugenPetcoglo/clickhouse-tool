# Clickhouse-tool

A local tool for manual ClickHouse backup and restore, following the pgAdmin workflow. The backend and `clickhouse-client` run on your laptop. The React frontend is planned.

Backup saves a `.chbackup` file locally. Restore is a separate action that sends that file to a selected server. The source connection is no longer needed, and the same file can be restored independently to localhost or dev.

## Start

Requirements: Linux, Node.js 24, npm, and `clickhouse-client` on your PATH.

From the project root:

```bash
npm install
npm run backend:dev
```

Open [Swagger UI](http://127.0.0.1:3001/docs/). Click **Authorize** and enter `1` for the application header. `GET /api/health` reports the CLI version or returns `503` if the CLI is unavailable.

For a production build, run `npm run backend:build`, then `npm run backend:start`. From `apps/backend`, development starts with `npm run start:dev`.

## Backup and restore

Use Swagger UI to send the requests below. Commands use JSON and require `X-Clickhouse-Tool: 1`; file imports use multipart instead. Replace the example IDs with values returned by the API.

1. Create a profile with `POST /api/connections`:

   ```json
   {"name":"Source","host":"127.0.0.1","port":9000,"username":"default","tls":false}
   ```

   Use the ClickHouse **native TCP port**, usually `9000`, or `9440` with TLS. Connect through `POST /api/connections/CONNECTION_ID/connect` with `{"password":"..."}`. List available databases through `GET /api/connections/CONNECTION_ID/databases`.

2. Start a backup with `POST /api/backups`:

   ```json
   {"sourceConnectionId":"CONNECTION_ID","database":"analytics"}
   ```

   Long operations return `202` with a `jobId`. Poll `GET /api/jobs/JOB_ID` about once per second until the status is `succeeded`, `failed`, `cancelled`, or `interrupted`. A successful backup result contains its ID and local path. Browse copies with `GET /api/backups` or download one through `GET /api/backups/BACKUP_ID/file`.

   To use a file saved earlier, upload it through `POST /api/backups/import` as one multipart file. This request stays open through upload and validation, then returns `jobId` and `backup`. Use the returned backup ID; incomplete uploads are discarded.

3. Connect the destination profile and select a restore mode:

   | Mode | Behavior |
   | --- | --- |
   | `replace-data` | Require an existing database with matching structure, clear its tables, and load the copy. |
   | `create-new` | Create the database under its original name, which must be available. Database renaming is not supported. |

   Start a check with `POST /api/restores/check`:

   ```json
   {"backupId":"BACKUP_ID","targetConnectionId":"TARGET_ID","mode":"replace-data"}
   ```

4. Wait for the check to succeed and inspect its result: destination server, database, and planned actions. Pause writes and migrations on the destination, then confirm with `POST /api/restores`:

   ```json
   {"checkJobId":"CHECK_JOB_ID","confirmationName":"analytics","targetWritesPaused":true}
   ```

   The database name must match exactly. The backend rechecks the file and destination before making changes. Restore is not transactional: a failure can leave partially restored data.

Views are detached during loading and reattached after every table succeeds, preventing materialized views from filling aggregates twice. Empty tables are cleared too. Stored computed columns and aggregate states are imported without recalculation or finalization.

## Jobs and recovery

One long operation runs at a time; another receives `409 BACKEND_BUSY`. Closing the browser does not stop an accepted background job. To cancel it, send `POST /api/jobs/JOB_ID/cancel`. The backend stops the CLI and attempts to kill its own server query.

Job status includes the phase, current table, completed objects, bytes, query ID, and change journal. After a restart, unfinished jobs become `interrupted`. `requiresRecovery: true` means the destination may have changed. Partial inserts are never retried automatically, and partially created databases are not deleted automatically.

To retry a restore with `requiresRecovery: true`:

1. Keep destination writes and migrations paused. Inspect the failed job and ensure its recorded query has stopped on the server.
2. Reconnect the destination. Submit the same backup, destination, and mode to `/api/restores/check`, adding `recoveryJobId` from the failed restore job.
3. Review the new check and confirm it through `/api/restores`.

A retry reloads the full table set. Its journal identifies detached views and compatible objects from a partial database creation. Detached view definitions are verified before tables are cleared. Keep the backup and job files until recovery finishes. If database creation succeeded without a completed journal record, an administrator must inspect that database before retrying.

## Configuration and storage

Set environment variables in your shell or IDE. `.env` files are not loaded automatically.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOST` | `127.0.0.1` | Loopback only; `localhost` and `::1` are also accepted. |
| `PORT` | `3001` | Backend port. |
| `DATA_DIR` | `~/.local/share/clickhouse-tool` | Profiles, jobs, backups, and temporary files. |
| `CLICKHOUSE_CLIENT_PATH` | `clickhouse-client` | CLI executable path. |
| `ALLOWED_ORIGINS` | `http://localhost:5173,http://127.0.0.1:5173` | Comma-separated frontend origins. |
| `MAX_BACKUP_BYTES` | `1099511627776` | Maximum imported file size, 1 TiB. |

When `XDG_DATA_HOME` is set, the default data directory is `$XDG_DATA_HOME/clickhouse-tool`. It contains `connections.json`, `jobs/`, `backups/`, and `runtime/`.

Profiles omit passwords. Each CLI command uses a temporary private configuration file with permissions `0600`. After a crash, stop the backend and remove any leftover `runtime/cli-*` directories. Protect backup files as you would the database data itself.

SSH requires OpenSSH, key or agent authentication, and a verified server entry in `known_hosts`. Add an optional `ssh` object to a connection profile:

```json
{"ssh":{"host":"dev.example.com","port":22,"username":"developer","identityFile":"/home/user/.ssh/id_ed25519"}}
```

The profile's outer `host` and `port` identify ClickHouse as seen from the SSH server. TLS verifies certificates, and `tlsServerName` can specify the certificate hostname when tunneling.

## Format and supported scope

A `.chbackup` is an uncompressed ZIP64 archive:

```text
manifest.json
schema/database.sql
schema/<object-id>.sql
data/<table-id>.native
```

The version 1 manifest records the database, server and CLI versions, object definitions and dependencies, table export intervals, file sizes, and SHA-256 hashes. Import and restore validate archive contents, sizes, and hashes before any destination changes. DDL omits source UUIDs. Native data is streamed without converting rows to JavaScript objects. Files are published only after completion; unfinished `.partial` files are not backups.

Supported: standalone servers, Atomic/Ordinary databases, `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, ordinary views, and incremental materialized views with an explicit `TO` table in the same database. Restore requires the same ClickHouse major/minor version as the source. Required storage policies must already exist on the destination.

Unsupported: clusters, replicated/shared engines, refreshable views, materialized views with hidden inner tables, cross-database dependencies, table functions, Dictionary/Join-engine dependencies, and named CTEs. View dependencies cannot use database or object names containing dots or newlines. Nested SELECT queries are supported. These restrictions are checked using metadata and `EXPLAIN AST` during backup and restore.

The source may keep receiving writes. Tables are exported sequentially, without a database-wide snapshot; detected schema changes prevent publication. Destination TTL rules and merges still apply, so matching row counts alone do not prove correctness. Server configuration, users, roles, and external resources are outside the backup.

The ClickHouse user needs access to relevant `system` metadata and `SHOW CREATE`, plus `SELECT` for backup. Restore needs `INSERT`, `TRUNCATE`, view `ATTACH`/`DETACH`, and creation privileges for `create-new`. Cancellation needs permission to stop its own query. Scope grants to the intended database.

## Development

`apps/backend/src/app.ts` assembles Fastify; `server.ts` starts it. The `connections`, `clickhouse`, `backups`, `restores`, and `jobs` modules contain functions independent of HTTP. `error-handler.ts` handles request errors centrally; job failures and resource cleanup are handled where those operations run. `apps/frontend/` is reserved for React.

From `apps/backend`:

```bash
npm run typecheck
npm run lint
npm test
npm run build
```

The three e2e scenarios in `tests/e2e/` cover file backup and independent restores after source profile deletion, data replacement including computed columns and aggregates, and rejection of damaged files or schema mismatches before destination changes. They use real HTTP requests and temporary ClickHouse servers with isolated ports and directories. Running them requires the local `clickhouse` binary with `server` and `client` subcommands; existing databases are not used.
