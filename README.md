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

The app discovers table and view definitions, target tables, and dependencies automatically. Ordinary views and incremental materialized views are detached during loading and reattached after every table succeeds, preventing aggregates from filling twice. Refreshable materialized views stay attached: the app records their enabled state, stops them, and waits for the active refresh to finish before clearing any table. Stopping and waiting are limited to 60 seconds per view. After a successful restore, previously active schedules resume; previously stopped schedules remain stopped.

For a new database, physical tables and their data are restored first, then views are created in dependency order. Refreshable views are created with `EMPTY` to skip the initial refresh and prevent an extra `APPEND`. Their schedules are enabled by default. The saved SQL is preserved; `EMPTY` is added only to the restore command. Natural scheduled refreshes may run after all physical data has loaded. The app does not force a refresh.

Empty tables are cleared too. Stored computed columns and aggregate states are imported without recalculation or finalization.

Buffer tables require ClickHouse 26.8 or later and are supported when they write to a supported physical table in the same database and have matching stored column names and types. Older Buffer readers do not reliably distinguish pending rows through the virtual `_table` column, so the app rejects them before publishing a backup or changing destination data. Backup reads pending RAM rows separately from the durable destination; it does not flush or modify the source. During restore, pending rows from the file are inserted directly into the durable table, so the restored Buffer starts empty while reads return all saved rows. Before replacing data, the app flushes each destination Buffer with `OPTIMIZE TABLE` and confirms its RAM is empty before clearing any physical table. It does not truncate Buffer tables.

Buffer RAM exports happen before durable table exports. If a Buffer flush occurs during data export, the backup fails with `SOURCE_BUFFER_FLUSHED` and no file is published, preventing duplicate or missing rows across the two exports. The flush counter is server-wide, so a flush in another database also conservatively rejects the backup. Retry when Buffer flushing is quiet.

## Jobs and recovery

One long operation runs at a time; another receives `409 BACKEND_BUSY`. Closing the browser does not stop an accepted background job. To cancel it, send `POST /api/jobs/JOB_ID/cancel`. The backend stops the CLI and attempts to kill its own server query.

Job status includes the phase, current table, completed objects, bytes, query ID, and change journal. After a restart, unfinished jobs become `interrupted`. `requiresRecovery: true` means the destination may have changed. Partial inserts are never retried automatically, and partially created databases are not deleted automatically.

To retry a restore with `requiresRecovery: true`:

1. Keep destination writes and migrations paused. Inspect the failed job and ensure its recorded query has stopped on the server.
2. Reconnect the destination. Submit the same backup, destination, and mode to `/api/restores/check`, adding `recoveryJobId` from the failed restore job.
3. Review the new check and confirm it through `/api/restores`.

A retry reloads the full table set. Its journal identifies detached views and compatible objects from a partial database creation. Detached ordinary and incremental view definitions are verified before tables are cleared. Refreshable views are never detached by restore. After failure or cancellation they remain stopped, and a successful retry restores their original enabled state from the first attempt's journal. Cleanup errors are reported with the original failure. Keep the backup and job files until recovery finishes. If database creation succeeded without a completed journal record, an administrator must inspect that database before retrying.

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

Supported: standalone servers, Atomic/Ordinary databases, `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, Buffer tables with a compatible local physical destination, ordinary views, and incremental or refreshable materialized views with an explicit `TO` table in the same database. Refreshable views can use schedules, `APPEND`, and local `DEPENDS ON` chains. Nested SELECT, JOIN, UNION, and table aliases are supported. Each physical target table is copied once. Restore requires the same ClickHouse major/minor version as the source. Required storage policies must already exist on the destination.

Unsupported: clusters, replicated/shared engines, materialized views with hidden inner tables (without explicit `TO`), `APPEND INCREMENTAL` (its processing cursor is not saved), cross-database dependencies, table functions, Dictionary/Join-engine dependencies, and named CTEs. View dependencies cannot use database or object names containing dots or newlines. Names containing `" (alias "` are also rejected because ClickHouse uses that marker for aliases in its syntax tree. These restrictions are checked using metadata and `EXPLAIN AST` during backup and restore.

Buffer tables without a destination, with incompatible destination columns, or with a user-defined `_table` column on the Buffer or its destination are unsupported. The virtual `_table` column must remain available to distinguish pending RAM rows. Incremental materialized views reading a Buffer's physical destination are also rejected because flushing pending rows can change their aggregates. Incremental views reading the Buffer itself remain supported.

The source may keep receiving writes. Tables are exported sequentially, without a database-wide snapshot; detected schema changes prevent publication. Destination TTL rules and merges still apply, so matching row counts alone do not prove correctness. Server configuration, users, roles, and external resources are outside the backup.

The ClickHouse user needs access to relevant `system` metadata and `SHOW CREATE`, plus `SELECT` for backup. Buffer backup also reads the `StorageBufferFlush` counter in `system.events`. Restore needs `INSERT`, `TRUNCATE`, view `ATTACH`/`DETACH`, and creation privileges for `create-new`. Buffer restore requires `OPTIMIZE` to drain destination RAM before data replacement. Databases with refreshable views also require access to `system.view_refreshes` and the `SYSTEM VIEWS` privilege for stopping and starting their schedules. Cancellation needs permission to stop its own query. Scope grants to the intended database.

## Development

`apps/backend/src/app.ts` assembles Fastify; `server.ts` starts it. The `connections`, `clickhouse`, `backups`, `restores`, and `jobs` modules contain functions independent of HTTP. `error-handler.ts` handles request errors centrally; job failures and resource cleanup are handled where those operations run. `apps/frontend/` is reserved for React.

Restore coordinators keep the operation order explicit. Schema discovery and SQL checks belong to `clickhouse`; `restores/views.ts` manages view lifecycles; `restores/load.ts` drains destination buffers, clears durable tables, and imports data. Both restore modes use the same data-loading functions and persistent job journal.

```text
HTTP API -> Jobs and journal
              |-> Backup:  schema checks -> Native export -> .chbackup
              |-> Restore: archive/schema checks -> prepare views
                           -> drain buffers -> Native import -> create/attach views -> resume schedules
```

From `apps/backend`:

```bash
npm run typecheck
npm run lint
npm test
npm run build
```

The four e2e scenarios in `tests/e2e/` cover file backup and independent restores after source profile deletion; data replacement including computed columns, aggregates, refresh schedules, APPEND snapshots and pending Buffer rows; rejection of damaged files, unsupported schemas, concurrent Buffer flushes or schema mismatches; and cancellation, Buffer import failure and journal-based recovery. They use real HTTP requests and temporary ClickHouse servers with isolated ports and directories. Running them requires the local `clickhouse` binary with `server` and `client` subcommands; existing databases are not used.
