import { createHash } from 'node:crypto';
import { constants, createWriteStream } from 'node:fs';
import { lstat, open, readdir, rename, rm, type FileHandle } from 'node:fs/promises';
import { join } from 'node:path';
import { PassThrough, Readable, Transform, Writable } from 'node:stream';
import { finished, pipeline } from 'node:stream/promises';
import { Reader, TextReader, ZipReader, ZipWriter, type FileEntry } from '@zip.js/zip.js';
import { schemaFingerprint, validateSupportedSchema } from '../clickhouse/metadata.js';
import { AppError, errorMessage } from '../errors.js';
import { ensureDirectory, idPath } from '../files.js';
import type { OperationContext } from '../types.js';
import { parseManifest } from './manifest.js';
import type { BackupFile, BackupSummary, CheckedBackup, Manifest } from './types.js';

export type { BackupFile, BackupSummary, CheckedBackup, Manifest } from './types.js';

const MAX_ENTRIES = 20_000;
const MAX_MANIFEST_BYTES = 32 * 1024 ** 2;
const MAX_SQL_BYTES = 4 * 1024 ** 2;
const uuidPattern = '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}';
const allowedEntry = new RegExp(`^(manifest\\.json|schema/database\\.sql|schema/${uuidPattern}\\.sql|data/${uuidPattern}\\.native)$`);

export function getBackupPath(dataDir: string, id: string): string {
  return idPath(join(dataDir, 'backups'), id, '.chbackup');
}

export interface ArchiveBuilder {
  addText(path: string, content: string): Promise<BackupFile>;
  addData(path: string, produce: (output: Writable) => Promise<void>): Promise<BackupFile>;
  finish(manifest: Manifest): Promise<BackupSummary>;
  abort(): Promise<void>;
}

export async function createArchiveBuilder(dataDir: string, id: string, signal?: AbortSignal): Promise<ArchiveBuilder> {
  signal?.throwIfAborted();
  await ensureDirectory(join(dataDir, 'backups'));
  const finalPath = getBackupPath(dataDir, id);
  const partialPath = `${finalPath}.partial`;
  const output = createWriteStream(partialPath, { flags: 'wx', mode: 0o600 });
  const outputFinished = finished(output);
  // A disk error can arrive before the next awaited ZIP operation.
  void outputFinished.catch(() => undefined);
  const zip = new ZipWriter(Writable.toWeb(output), { zip64: true, level: 0, useWebWorkers: false });
  const names = new Set<string>();
  function reserveEntry(path: string): void {
    signal?.throwIfAborted();
    if (!allowedEntry.test(path) || names.has(path) || names.size >= MAX_ENTRIES) throw invalid('Invalid backup file inventory.');
    names.add(path);
  }

  return {
    async addText(path, content) {
      reserveEntry(path);
      const bytes = Buffer.from(content);
      if (bytes.length > MAX_SQL_BYTES) throw invalid(`SQL file is too large: ${path}.`);
      await zip.add(path, new TextReader(content), { level: 0, signal });
      return { path, size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
    },
    async addData(path, produce) {
      reserveEntry(path);
      const hash = createHash('sha256');
      let size = 0;
      const data = new Transform({
        transform(chunk: Buffer, _encoding, callback) {
          hash.update(chunk);
          size += chunk.length;
          callback(null, chunk);
        },
      });
      const production = produce(data).catch(error => {
        data.destroy(error instanceof Error ? error : new Error(String(error)));
        throw error;
      });
      // Node and DOM describe the same Web Stream with slightly different types.
      const readable = Readable.toWeb(data) as unknown as ReadableStream<Uint8Array>;
      const archiving = zip.add(path, readable, { level: 0, signal }).catch(error => {
        data.destroy(error instanceof Error ? error : new Error(String(error)));
        throw error;
      });
      const results = await Promise.allSettled([production, archiving]);
      const failure = results.find(result => result.status === 'rejected');
      if (failure?.status === 'rejected') throw failure.reason;
      return { path, size, sha256: hash.digest('hex') };
    },
    async finish(manifest) {
      reserveEntry('manifest.json');
      const content = JSON.stringify(manifest);
      if (Buffer.byteLength(content) > MAX_MANIFEST_BYTES) throw invalid('Backup manifest is too large.');
      parseManifest(content);
      await zip.add('manifest.json', new TextReader(content), { level: 0, signal });
      await zip.close();
      await outputFinished;
      signal?.throwIfAborted();
      await rename(partialPath, finalPath);
      return summary(id, finalPath, manifest);
    },
    async abort() {
      output.destroy();
      await outputFinished.catch(() => undefined);
      await zip.close().catch(() => undefined);
      await rm(partialPath, { force: true });
    },
  };
}

export async function checkBackup(dataDir: string, id: string, context?: OperationContext): Promise<CheckedBackup> {
  return openCheckedBackup(getBackupPath(dataDir, id), context);
}

export async function listBackups(dataDir: string): Promise<BackupSummary[]> {
  const directory = join(dataDir, 'backups');
  await ensureDirectory(directory);
  const names = await readdir(directory);
  const backups: BackupSummary[] = [];
  for (const name of names.sort()) {
    const match = new RegExp(`^(${uuidPattern})\\.chbackup$`).exec(name);
    if (!match?.[1]) continue;
    const id = match[1];
    const path = getBackupPath(dataDir, id);
    try {
      const opened = await openArchive(path);
      try {
        const manifest = await readManifest(opened.entries);
        backups.push(await summary(id, path, manifest));
      } finally {
        await opened.close();
      }
    } catch (error) {
      const info = await lstat(path);
      backups.push({ id, database: 'unknown', createdAt: info.birthtime.toISOString(), finishedAt: info.mtime.toISOString(), serverVersion: 'unknown', clientVersion: 'unknown', bytes: info.size, path, status: 'invalid', error: errorMessage(error) });
    }
  }
  return backups.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
}

export async function importBackup(dataDir: string, input: Readable, maxBytes: number, context: OperationContext): Promise<BackupSummary> {
  const { randomUUID } = await import('node:crypto');
  const id = randomUUID();
  await ensureDirectory(join(dataDir, 'backups'));
  const path = getBackupPath(dataDir, id);
  const partial = `${path}.partial`;
  let bytes = 0;
  const limit = new Transform({
    transform(chunk: Buffer, _encoding, callback) {
      bytes += chunk.length;
      context.addBytes(chunk.length);
      callback(bytes > maxBytes ? new AppError('BACKUP_TOO_LARGE', 'Backup exceeds MAX_BACKUP_BYTES.', 413) : null, chunk);
    },
  });
  try {
    await context.setPhase('uploading');
    await pipeline(input, limit, createWriteStream(partial, { flags: 'wx', mode: 0o600 }), { signal: context.signal });
    if ((input as Readable & { truncated?: boolean }).truncated) throw new AppError('BACKUP_TOO_LARGE', 'Backup upload was truncated.', 413);
    await context.setPhase('checking-backup');
    const checked = await openCheckedBackup(partial, context);
    const manifest = checked.manifest;
    await checked.close();
    context.signal.throwIfAborted();
    await rename(partial, path);
    return await summary(id, path, manifest);
  } catch (error) {
    await rm(partial, { force: true });
    throw error;
  }
}

async function openCheckedBackup(path: string, context?: OperationContext): Promise<CheckedBackup> {
  const opened = await openArchive(path);
  try {
    const manifest = await readManifest(opened.entries, context?.signal);
    validateInventory(manifest, opened.entries);
    validateSupportedSchema(manifest.schema);
    if (schemaFingerprint(manifest.schema) !== manifest.schema.fingerprint) throw invalid('Schema fingerprint is invalid.');

    for (const file of manifest.files) {
      context?.signal.throwIfAborted();
      const entry = opened.entries.get(file.path)!;
      const hash = createHash('sha256');
      let size = 0;
      await entry.getData(new WritableStream<Uint8Array>({
        write(chunk) {
          context?.signal.throwIfAborted();
          hash.update(chunk);
          size += chunk.length;
          if (size > file.size) throw invalid(`Unexpected file size: ${file.path}.`);
          context?.addBytes(chunk.length);
        },
      }), { signal: context?.signal });
      if (size !== file.size || hash.digest('hex') !== file.sha256) throw invalid(`Checksum mismatch: ${file.path}.`);
    }
    const sql = new Map<string, string>([['schema/database.sql', manifest.schema.ddl]]);
    for (const object of manifest.schema.objects) sql.set(`schema/${object.id}.sql`, object.ddl);
    for (const [name, expected] of sql) {
      if (await readText(opened.entries.get(name)!, MAX_SQL_BYTES, context?.signal) !== expected) throw invalid(`SQL does not match the manifest: ${name}.`);
    }

    let closed = false;
    const streams = new Set<Readable>();
    const extractions = new Set<Promise<unknown>>();
    return {
      path,
      manifest,
      schema: manifest.schema,
      async readData(objectId) {
        if (closed) throw invalid('Backup reader has been closed.');
        if (!manifest.schema.objects.some(object => object.id === objectId && object.kind === 'table')) throw invalid('Unknown data table.');
        const entry = opened.entries.get(`data/${objectId}.native`)!;
        const output = new PassThrough();
        streams.add(output);
        output.once('close', () => streams.delete(output));
        const extraction = entry.getData(Writable.toWeb(output), { signal: context?.signal });
        extractions.add(extraction);
        void extraction.then(
          () => extractions.delete(extraction),
          error => { extractions.delete(extraction); output.destroy(error instanceof Error ? error : new Error(String(error))); },
        );
        return output;
      },
      async close() {
        if (closed) return;
        closed = true;
        for (const stream of streams) stream.destroy();
        await Promise.allSettled(extractions);
        await opened.close();
      },
    };
  } catch (error) {
    await opened.close();
    if (error instanceof AppError || context?.signal.aborted) throw error;
    throw invalid(`Cannot read backup: ${errorMessage(error)}`);
  }
}

async function openArchive(path: string) {
  const info = await lstat(path).catch(() => { throw new AppError('BACKUP_NOT_FOUND', 'Backup file was not found.', 404); });
  if (!info.isFile() || info.isSymbolicLink()) throw invalid('Backup must be a regular file.');
  // Seek through a file handle. Passing a plain stream to ZipReader buffers the
  // entire archive; some Node versions also truncate openAsBlob sizes at 4 GiB.
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  const actual = await file.stat();
  if (!actual.isFile() || actual.size !== info.size || actual.mtimeMs !== info.mtimeMs) {
    await file.close();
    throw invalid('Backup file changed while it was being opened.');
  }
  const reader = new ZipReader(new BackupFileReader(file, actual.size, actual.mtimeMs), {
    useWebWorkers: false, strictness: 'strict', checkCrc32: true, checkOverlappingEntry: true,
  });
  async function close(): Promise<void> {
    try { await reader.close(); } finally { await file.close(); }
  }
  const entries = new Map<string, FileEntry>();
  try {
    for await (const entry of reader.getEntriesGenerator()) {
      if (entries.size >= MAX_ENTRIES) throw invalid('Backup contains too many files.');
      if (!allowedEntry.test(entry.filename) || entry.directory || entry.encrypted || entries.has(entry.filename)) throw invalid('Backup contains an invalid or duplicate entry.');
      if (!Number.isSafeInteger(entry.uncompressedSize) || entry.uncompressedSize < 0) throw invalid('Invalid ZIP entry size.');
      if (entry.compressionMethod !== 0) throw invalid('Only uncompressed Clickhouse-tool archives are supported.');
      const attributes = entry.externalFileAttributes;
      if (((attributes >>> 16) & 0o170000) === 0o120000) throw invalid('Symbolic links are not allowed in a backup.');
      entries.set(entry.filename, entry);
    }
    return { entries, close };
  } catch (error) {
    await close();
    if (error instanceof AppError) throw error;
    throw invalid(`Invalid ZIP archive: ${errorMessage(error)}`);
  }
}

class BackupFileReader extends Reader<FileHandle> {
  constructor(private readonly file: FileHandle, size: number, private readonly modifiedAt: number) {
    super(file);
    this.size = size;
  }

  async readUint8Array(offset: number, length: number): Promise<Uint8Array> {
    if (!Number.isSafeInteger(offset) || !Number.isSafeInteger(length) || offset < 0 || length < 0 || length > MAX_MANIFEST_BYTES || offset + length > this.size) {
      throw invalid('Invalid or oversized ZIP read range.');
    }
    const info = await this.file.stat();
    if (info.size !== this.size || info.mtimeMs !== this.modifiedAt) throw invalid('Backup file changed during reading.');
    const bytes = Buffer.allocUnsafe(length);
    let read = 0;
    while (read < length) {
      const chunk = await this.file.read(bytes, read, length - read, offset + read);
      if (chunk.bytesRead === 0) throw invalid('Backup ended unexpectedly.');
      read += chunk.bytesRead;
    }
    return bytes;
  }
}

async function readManifest(entries: Map<string, FileEntry>, signal?: AbortSignal): Promise<Manifest> {
  const entry = entries.get('manifest.json');
  if (!entry) throw invalid('Backup manifest is missing.');
  return parseManifest(await readText(entry, MAX_MANIFEST_BYTES, signal));
}

async function readText(entry: FileEntry, maximum: number, signal?: AbortSignal): Promise<string> {
  if (entry.uncompressedSize > maximum) throw invalid(`Metadata file is too large: ${entry.filename}.`);
  const chunks: Buffer[] = [];
  let bytes = 0;
  await entry.getData(new WritableStream<Uint8Array>({
    write(chunk) {
      signal?.throwIfAborted();
      bytes += chunk.length;
      if (bytes > maximum) throw invalid(`Metadata file is too large: ${entry.filename}.`);
      chunks.push(Buffer.from(chunk));
    },
  }), { signal });
  return new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks));
}

function validateInventory(manifest: Manifest, entries: Map<string, FileEntry>): void {
  const expected = new Set(['schema/database.sql']);
  for (const object of manifest.schema.objects) {
    expected.add(`schema/${object.id}.sql`);
    if (object.kind === 'table') expected.add(`data/${object.id}.native`);
  }
  const declared = new Set(manifest.files.map(file => file.path));
  if (declared.size !== manifest.files.length || declared.size !== expected.size || entries.size !== expected.size + 1) throw invalid('Backup file inventory does not match its objects.');
  for (const file of manifest.files) {
    const entry = entries.get(file.path);
    if (!expected.has(file.path) || !entry || entry.uncompressedSize !== file.size) throw invalid(`Missing or invalid backup file: ${file.path}.`);
  }
}

async function summary(id: string, path: string, manifest: Manifest): Promise<BackupSummary> {
  const info = await lstat(path);
  return { id, database: manifest.source.database, createdAt: manifest.createdAt, finishedAt: manifest.finishedAt, serverVersion: manifest.source.serverVersion, clientVersion: manifest.source.clientVersion, bytes: info.size, path, status: 'ready' };
}

function invalid(message: string): AppError {
  return new AppError('INVALID_BACKUP', message, 422);
}
