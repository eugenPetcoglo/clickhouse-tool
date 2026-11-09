import { createHash, randomUUID } from 'node:crypto';
import { AppError } from '../errors.js';
import type { ClickHouseClient, ColumnInfo, ConnectedConnection, DatabaseSchema, OperationContext, SchemaObject } from '../types.js';
import { quoteIdentifier } from './client.js';
import { isRefreshableView, readSqlDependencies, readViewHeader } from './ddl.js';

const TABLE_ENGINES = new Set(['MergeTree', 'ReplacingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree', 'Buffer']);
const DATABASE_ENGINES = new Set(['Atomic', 'Ordinary']);

interface TableRow {
  name: string;
  engine: string;
  uuid?: string;
  storage_policy?: string;
  loading_dependencies_database?: string[];
  loading_dependencies_table?: string[];
  loading_dependent_database?: string[];
  loading_dependent_table?: string[];
  dependencies_database?: string[];
  dependencies_table?: string[];
  target_database?: string;
  target_table?: string;
  has_own_data?: number;
}

interface ColumnRow {
  table: string;
  name: string;
  type: string;
  position: number | string;
  default_kind: string;
  default_expression: string;
}

export async function readDatabaseSchema(
  client: ClickHouseClient,
  connection: ConnectedConnection,
  database: string,
  context?: OperationContext,
): Promise<DatabaseSchema> {
  if (['system', 'information_schema'].includes(database.toLowerCase())) throw new AppError('UNSUPPORTED_DATABASE', 'System databases cannot be backed up or restored.');
  const options = { context, parameters: { database } };
  const databases = await client.query<{ engine: string }>(connection, 'SELECT engine FROM system.databases WHERE name = {database:String}', options);
  const foundDatabase = databases[0];
  if (!foundDatabase) throw new AppError('DATABASE_NOT_FOUND', `Database ${database} does not exist.`, 404);
  const available = new Set((await client.query<{ name: string }>(connection, "SELECT name FROM system.columns WHERE database = 'system' AND table = 'tables'", { context })).map((row) => row.name));
  const optionalColumns = ['uuid', 'storage_policy', 'loading_dependencies_database', 'loading_dependencies_table', 'loading_dependent_database', 'loading_dependent_table', 'dependencies_database', 'dependencies_table', 'target_database', 'target_table', 'has_own_data'].filter((name) => available.has(name));
  const rows = await client.query<TableRow>(connection, `SELECT name, engine${optionalColumns.map((name) => `, ${name}`).join('')} FROM system.tables WHERE database = {database:String} ORDER BY name`, options);
  const definitions = new Map<string, string>();
  const refreshTemporaryTables = new Set<string>();
  for (const row of rows.filter(row => row.engine === 'MaterializedView')) {
    const ddl = await readDdl(client, connection, `SHOW CREATE TABLE ${quoteIdentifier(database)}.${quoteIdentifier(row.name)}`, context);
    definitions.set(row.name, ddl);
    if (!readViewHeader(ddl, database).refreshable) continue;
    // ClickHouse builds non-APPEND refresh results in a table owned by this view.
    // Its exact name follows the view UUID, or its name in an Ordinary database.
    const innerName = row.uuid && row.uuid !== '00000000-0000-0000-0000-000000000000' ? `inner_id.${row.uuid}` : `inner.${row.name}`;
    refreshTemporaryTables.add(`.tmp.${innerName}`);
  }
  const tables = rows.filter(row => !refreshTemporaryTables.has(row.name));
  if (tables.some((row) => ['View', 'MaterializedView'].includes(row.engine)) && !available.has('loading_dependencies_table')) {
    throw new AppError('UNSUPPORTED_VERSION', 'This ClickHouse version does not expose the view dependencies required for safe restore.');
  }
  const allColumns = await client.query<ColumnRow>(connection, 'SELECT table, name, type, position, default_kind, default_expression FROM system.columns WHERE database = {database:String} ORDER BY table, position', options);
  const databaseDdl = await readDdl(client, connection, `SHOW CREATE DATABASE ${quoteIdentifier(database)}`, context);
  const objects: SchemaObject[] = [];
  for (const row of tables) {
    const ddl = definitions.get(row.name) ?? await readDdl(client, connection, `SHOW CREATE TABLE ${quoteIdentifier(database)}.${quoteIdentifier(row.name)}`, context);
    const header = readViewHeader(ddl, database);
    const kind = row.engine === 'View' ? 'view' : row.engine === 'MaterializedView' ? 'materialized-view' : 'table';
    const targetDatabase = row.target_database || header.targetDatabase;
    const targetTable = row.target_table || header.targetTable;
    if (kind === 'materialized-view' && targetDatabase && targetDatabase !== database) {
      throw new AppError('UNSUPPORTED_SCHEMA', `View ${row.name} writes to a different database.`);
    }
    const dependencies = localDependencies(database, row.loading_dependencies_database, row.loading_dependencies_table);
    // A source table records the views triggered by its inserts. Reverse these
    // edges so a view can be detached before its source and attached afterwards.
    localDependencies(database, row.loading_dependent_database, row.loading_dependent_table);
    localDependencies(database, row.dependencies_database, row.dependencies_table);
    if (kind === 'materialized-view' && targetTable) dependencies.push(targetTable);
    objects.push({
      id: randomUUID(), name: row.name, kind, engine: row.engine, ddl,
      columns: allColumns.filter((column) => column.table === row.name).map(columnInfo),
      dependencies: [...new Set(dependencies)].filter((name) => name !== row.name).sort(),
      storagePolicy: row.storage_policy ?? '',
      ...(kind === 'materialized-view' && targetTable ? { targetTable } : {}),
    });
  }
  for (const source of tables) {
    const dependents = localDependencies(database, source.dependencies_database, source.dependencies_table);
    for (const dependent of dependents) {
      const view = objects.find((object) => object.name === dependent && object.kind !== 'table');
      if (view && !view.dependencies.includes(source.name)) view.dependencies.push(source.name);
    }
  }
  const schema: DatabaseSchema = { name: database, engine: foundDatabase.engine, ddl: databaseDdl, objects, fingerprint: '' };
  // ClickHouse does not consistently list ordinary-view SELECT dependencies in
  // system.tables. Its SQL parser provides the authoritative local references.
  for (const object of objects.filter(object => object.kind !== 'table' || object.engine === 'Buffer')) {
    const dependencies = await readSqlDependencies(client, connection, schema, object.ddl, context, true);
    if (object.engine === 'Buffer') {
      if (dependencies.length !== 1) throw new AppError('UNSUPPORTED_SCHEMA', `Buffer table ${object.name} must have one local persistent destination.`);
      object.targetTable = dependencies[0]!;
    } else if (object.targetTable) dependencies.push(object.targetTable);
    object.dependencies = [...new Set(dependencies)].sort();
  }
  schema.fingerprint = schemaFingerprint(schema);
  return schema;
}

export function validateSupportedSchema(schema: DatabaseSchema): void {
  if (!DATABASE_ENGINES.has(schema.engine)) throw new AppError('UNSUPPORTED_SCHEMA', `Database engine ${schema.engine} is not supported.`);
  if (['system', 'information_schema'].includes(schema.name.toLowerCase())) throw new AppError('UNSUPPORTED_DATABASE', 'System databases are not supported.');
  const names = new Set<string>();
  for (const object of schema.objects) {
    if (names.has(object.name)) throw new AppError('UNSUPPORTED_SCHEMA', `Duplicate object ${object.name}.`);
    names.add(object.name);
    if (object.name.startsWith('.inner')) throw new AppError('UNSUPPORTED_SCHEMA', 'Materialized views with hidden inner tables are not supported.');
    if (object.kind === 'table' && !TABLE_ENGINES.has(object.engine)) throw new AppError('UNSUPPORTED_SCHEMA', `Table engine ${object.engine} in ${object.name} is not supported.`);
    if (object.kind === 'view' && object.engine !== 'View') throw new AppError('UNSUPPORTED_SCHEMA', `Invalid view ${object.name}.`);
    if (object.kind === 'materialized-view') {
      if (object.engine !== 'MaterializedView' || !object.targetTable || object.targetTable.startsWith('.inner')) {
        throw new AppError('UNSUPPORTED_SCHEMA', `Materialized view ${object.name} must have an explicit TO table in the same database.`);
      }
      readViewHeader(object.ddl, schema.name);
    }
    const columns = new Set<string>();
    const positions = new Set<number>();
    for (const column of object.columns) {
      if (!Number.isSafeInteger(column.position) || column.position < 1) throw new AppError('INVALID_SCHEMA', `Invalid column position in ${object.name}.`);
      if (!column.name || columns.has(column.name) || positions.has(column.position)) throw new AppError('INVALID_SCHEMA', `Duplicate or invalid column in ${object.name}.`);
      if (!['', 'DEFAULT', 'MATERIALIZED', 'ALIAS', 'EPHEMERAL'].includes(column.defaultKind)) throw new AppError('UNSUPPORTED_SCHEMA', `Unsupported column kind in ${object.name}.${column.name}.`);
      columns.add(column.name);
      positions.add(column.position);
    }
    if (object.kind === 'table' && !object.columns.some((column) => !['ALIAS', 'EPHEMERAL'].includes(column.defaultKind))) throw new AppError('UNSUPPORTED_SCHEMA', `Table ${object.name} has no stored columns.`);
  }
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const visit = (object: SchemaObject) => {
    if (visited.has(object.name)) return;
    if (visiting.has(object.name)) throw new AppError('UNSUPPORTED_SCHEMA', 'Cyclic object dependencies are not supported.');
    visiting.add(object.name);
    for (const dependency of object.dependencies) {
      const other = schema.objects.find((candidate) => candidate.name === dependency);
      if (!other) throw new AppError('UNSUPPORTED_SCHEMA', `Object ${object.name} has an unavailable dependency: ${dependency}.`);
      visit(other);
    }
    if (object.kind === 'materialized-view' && !schema.objects.some((candidate) => candidate.name === object.targetTable && candidate.kind === 'table' && candidate.engine !== 'Buffer')) {
      throw new AppError('UNSUPPORTED_SCHEMA', `Materialized view ${object.name} must target a supported physical table.`);
    }
    if (object.engine === 'Buffer') {
      const target = schema.objects.find(candidate => candidate.name === object.targetTable);
      if (!target || target.kind !== 'table' || target.engine === 'Buffer' || object.dependencies.length !== 1 || object.dependencies[0] !== target.name) {
        throw new AppError('UNSUPPORTED_SCHEMA', `Buffer table ${object.name} must target one supported persistent table in the same database.`);
      }
      const sourceNames = new Set([target.name]);
      for (const view of orderViews(schema.objects).filter(candidate => candidate.kind === 'view')) {
        if (view.dependencies.some(name => sourceNames.has(name))) sourceNames.add(view.name);
      }
      const downstream = schema.objects.find(candidate => candidate.kind === 'materialized-view' && !isRefreshableView(candidate)
        && candidate.dependencies.some(name => name !== candidate.targetTable && sourceNames.has(name)));
      if (downstream) {
        throw new AppError('UNSUPPORTED_SCHEMA', `Buffer table ${object.name} feeds ${target.name}, which incremental materialized view ${downstream.name} depends on. Restoring pending rows for this pipeline is not supported.`);
      }
      if ([...object.columns, ...target.columns].some(column => column.name === '_table')) {
        throw new AppError('UNSUPPORTED_SCHEMA', `Buffer table ${object.name} and its destination cannot define a column named _table.`);
      }
      const storedColumns = (table: SchemaObject) => table.columns.filter(column => !['ALIAS', 'EPHEMERAL'].includes(column.defaultKind))
        .map(column => ({ name: column.name, type: column.type })).sort((left, right) => left.name.localeCompare(right.name));
      if (JSON.stringify(storedColumns(object)) !== JSON.stringify(storedColumns(target))) {
        throw new AppError('UNSUPPORTED_SCHEMA', `Buffer table ${object.name} and its destination must have identical stored column names and types.`);
      }
    }
    visiting.delete(object.name);
    visited.add(object.name);
  };
  schema.objects.forEach(visit);
}

export function orderViews(objects: SchemaObject[]): SchemaObject[] {
  const views = objects.filter((object) => object.kind !== 'table');
  const result: SchemaObject[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();
  const visit = (view: SchemaObject) => {
    if (visited.has(view.name)) return;
    if (visiting.has(view.name)) throw new AppError('UNSUPPORTED_SCHEMA', 'Cyclic view dependencies are not supported.');
    visiting.add(view.name);
    for (const dependency of view.dependencies) {
      const parent = views.find((candidate) => candidate.name === dependency);
      if (parent) visit(parent);
    }
    visiting.delete(view.name);
    visited.add(view.name);
    result.push(view);
  };
  views.forEach(visit);
  return result;
}

export function getDataTables(schema: DatabaseSchema): SchemaObject[] {
  return schema.objects.filter(object => object.kind === 'table').sort((left, right) => left.name.localeCompare(right.name));
}

export function getBufferTables(schema: DatabaseSchema): SchemaObject[] {
  return getDataTables(schema).filter(object => object.engine === 'Buffer');
}

/** Older Buffer readers do not reliably identify memory rows with _table. */
export function checkBufferVersion(schema: DatabaseSchema, version: string): void {
  if (getBufferTables(schema).length === 0) return;
  const [major, minor] = version.split('.').map(Number);
  if (major === undefined || minor === undefined || !Number.isInteger(major) || !Number.isInteger(minor)
    || major < 26 || (major === 26 && minor < 8)) {
    throw new AppError('UNSUPPORTED_VERSION', `Safe Buffer backup and restore require ClickHouse 26.8 or later. Server version ${version} cannot reliably distinguish buffered rows from destination rows.`, 422);
  }
}

export function getCreateTables(schema: DatabaseSchema): SchemaObject[] {
  const tables = getDataTables(schema);
  return [...tables.filter(object => object.engine !== 'Buffer'), ...tables.filter(object => object.engine === 'Buffer')];
}

export function schemaFingerprint(schema: DatabaseSchema): string {
  const normalized = {
    name: schema.name, engine: schema.engine, ddl: schema.ddl.trim(),
    objects: [...schema.objects].sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0).map((object) => ({
      name: object.name, kind: object.kind, engine: object.engine, ddl: object.ddl.trim(),
      columns: [...object.columns].sort((a, b) => a.position - b.position),
      dependencies: [...object.dependencies].sort(), storagePolicy: object.storagePolicy,
      targetTable: object.targetTable,
    })),
  };
  return createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
}

async function readDdl(client: ClickHouseClient, connection: ConnectedConnection, sql: string, context?: OperationContext): Promise<string> {
  const rows = await client.query<Record<string, unknown>>(connection, sql, { context });
  const ddl = rows[0] ? Object.values(rows[0])[0] : undefined;
  if (typeof ddl !== 'string') throw new AppError('INVALID_CLICKHOUSE_RESPONSE', 'ClickHouse did not return an object definition.', 502);
  return ddl.trim();
}

function columnInfo(column: ColumnRow): ColumnInfo {
  return { name: column.name, type: column.type, position: Number(column.position), defaultKind: column.default_kind, defaultExpression: column.default_expression };
}

function localDependencies(database: string, databases: string[] = [], tables: string[] = []): string[] {
  if (databases.length !== tables.length) throw new AppError('INVALID_SCHEMA', 'ClickHouse returned inconsistent dependency metadata.');
  return tables.map((table, index) => {
    if (databases[index] && databases[index] !== database) throw new AppError('UNSUPPORTED_SCHEMA', `Cross-database dependency ${databases[index]}.${table} is not supported.`);
    return table;
  });
}
