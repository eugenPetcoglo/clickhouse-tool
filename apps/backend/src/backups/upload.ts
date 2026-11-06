import { Busboy, type BusboyFileStream, type BusboyInstance } from '@fastify/busboy';
import type { IncomingMessage } from 'node:http';
import { PassThrough } from 'node:stream';
import { AppError } from '../errors.js';

interface BackupUpload {
  stream: PassThrough;
  discard(): void;
}

/** The stream ends only after the complete form, including its final boundary, is valid. */
export async function readBackupUpload(request: IncomingMessage, maxBytes: number): Promise<BackupUpload> {
  const contentType = request.headers['content-type'];
  if (!contentType?.startsWith('multipart/form-data;')) throw new AppError('INVALID_UPLOAD', 'Use multipart/form-data to upload a .chbackup file.', 415);
  let parser: BusboyInstance;
  try {
    parser = new Busboy({ headers: { ...request.headers, 'content-type': contentType }, highWaterMark: 64 * 1024, fileHwm: 64 * 1024, limits: { files: 1, fields: 0, fileSize: maxBytes, parts: 1, headerPairs: 30, headerSize: 16 * 1024 } });
  } catch {
    throw new AppError('INVALID_UPLOAD', 'The multipart boundary is missing or invalid.');
  }
  const stream = new PassThrough({ highWaterMark: 64 * 1024 });
  // Parsing can reject a buffered request before the route attaches its pipeline.
  stream.on('error', () => {});
  let file: BusboyFileStream | undefined;
  let finished = false;
  let failed = false;
  let resolveFile: (upload: BackupUpload) => void = () => {};
  let rejectFile: (error: Error) => void = () => {};
  const ready = new Promise<BackupUpload>((resolve, reject) => { resolveFile = resolve; rejectFile = reject; });

  function removeRequestListeners(): void {
    request.removeListener('aborted', aborted);
    request.removeListener('error', requestError);
  }

  function stopParsing(): void {
    request.unpipe(parser);
    file?.unpipe(stream);
    file?.destroy();
    parser.destroy();
    request.resume();
    removeRequestListeners();
  }

  function fail(error: AppError): void {
    if (failed || finished) return;
    failed = true;
    rejectFile(error);
    stream.destroy(error);
    stopParsing();
  }

  function aborted(): void { fail(new AppError('UPLOAD_ABORTED', 'The upload was interrupted.')); }
  function requestError(): void { fail(new AppError('INVALID_UPLOAD', 'The upload stream could not be read.')); }
  function discard(): void {
    if (finished || failed) return;
    failed = true;
    stream.destroy();
    stopParsing();
  }

  parser.on('file', (_name, current, filename) => {
    file = current;
    current.on('error', () => fail(new AppError('INVALID_UPLOAD', 'The multipart file ended unexpectedly.')));
    current.once('limit', () => fail(new AppError('UPLOAD_TOO_LARGE', 'The upload exceeded MAX_BACKUP_BYTES.', 413)));
    if (typeof filename !== 'string' || !filename.endsWith('.chbackup')) {
      fail(new AppError('INVALID_BACKUP', 'The file must have a .chbackup extension.'));
      return;
    }
    resolveFile({ stream, discard });
    current.pipe(stream, { end: false });
  });
  parser.on('filesLimit', () => fail(new AppError('INVALID_UPLOAD', 'Upload exactly one .chbackup file.')));
  parser.on('partsLimit', () => fail(new AppError('INVALID_UPLOAD', 'Upload exactly one file without additional fields.')));
  parser.on('fieldsLimit', () => fail(new AppError('INVALID_UPLOAD', 'Additional form fields are not allowed.')));
  parser.on('error', () => fail(new AppError('INVALID_UPLOAD', 'The multipart upload is incomplete or invalid.')));
  parser.once('finish', () => {
    if (failed) return;
    if (!file) { fail(new AppError('FILE_REQUIRED', 'Select a .chbackup file.')); return; }
    finished = true;
    removeRequestListeners();
    stream.end();
  });
  stream.once('close', discard);
  request.once('aborted', aborted);
  request.once('error', requestError);
  if (request.aborted) aborted();
  else request.pipe(parser);
  return ready;
}
