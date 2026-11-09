import { Type } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';
import { AppError } from '../errors.js';
import { getDataTables } from '../clickhouse/metadata.js';
import type { Manifest } from './types.js';

const uuid = Type.String({ pattern: '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$' });
const text = Type.String();
const column = Type.Object({
  name: text,
  type: text,
  position: Type.Integer({ minimum: 1 }),
  defaultKind: text,
  defaultExpression: text,
}, { additionalProperties: false });

const object = Type.Object({
  id: uuid,
  name: Type.String({ minLength: 1 }),
  kind: Type.Union([Type.Literal('table'), Type.Literal('view'), Type.Literal('materialized-view')]),
  engine: text,
  ddl: Type.String({ minLength: 1 }),
  columns: Type.Array(column),
  dependencies: Type.Array(text),
  storagePolicy: text,
  targetTable: Type.Optional(text),
}, { additionalProperties: false });

export const manifestSchema = Type.Object({
  formatVersion: Type.Literal(1),
  id: uuid,
  createdAt: text,
  finishedAt: text,
  source: Type.Object({ database: text, serverVersion: text, clientVersion: text }, { additionalProperties: false }),
  schema: Type.Object({
    name: Type.String({ minLength: 1 }),
    engine: text,
    ddl: Type.String({ minLength: 1 }),
    objects: Type.Array(object),
    fingerprint: Type.String({ pattern: '^[a-f0-9]{64}$' }),
  }, { additionalProperties: false }),
  files: Type.Array(Type.Object({
    path: text,
    size: Type.Integer({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }),
    sha256: Type.String({ pattern: '^[a-f0-9]{64}$' }),
  }, { additionalProperties: false })),
  tableExports: Type.Array(Type.Object({ objectId: uuid, startedAt: text, finishedAt: text }, { additionalProperties: false })),
}, { additionalProperties: false });

export function parseManifest(content: string): Manifest {
  let value: unknown;
  try {
    value = JSON.parse(content);
  } catch {
    throw new AppError('INVALID_BACKUP', 'Backup manifest is not valid JSON.', 422);
  }
  if (!Value.Check(manifestSchema, value)) {
    throw new AppError('INVALID_BACKUP', 'Backup manifest has an unsupported format or invalid fields.', 422);
  }
  const manifest: Manifest = value;
  const dates = [manifest.createdAt, manifest.finishedAt, ...manifest.tableExports.flatMap(table => [table.startedAt, table.finishedAt])];
  if (dates.some(date => !Number.isFinite(Date.parse(date)))) {
    throw new AppError('INVALID_BACKUP', 'Backup contains an invalid timestamp.', 422);
  }
  if (manifest.source.database !== manifest.schema.name) {
    throw new AppError('INVALID_BACKUP', 'Backup database does not match its schema.', 422);
  }
  const ids = manifest.schema.objects.map(item => item.id);
  const names = manifest.schema.objects.map(item => item.name);
  if (new Set(ids).size !== ids.length || new Set(names).size !== names.length) {
    throw new AppError('INVALID_BACKUP', 'Backup contains duplicate objects.', 422);
  }
  const tableIds = getDataTables(manifest.schema).map(item => item.id).sort();
  const exports = manifest.tableExports.map(item => item.objectId).sort();
  if (JSON.stringify(tableIds) !== JSON.stringify(exports)) {
    throw new AppError('INVALID_BACKUP', 'Backup export records do not match its data tables.', 422);
  }
  return manifest;
}
