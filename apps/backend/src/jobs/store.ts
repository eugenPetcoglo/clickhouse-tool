import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { ensureDirectory, readJson, writeJson } from '../files.js';
import type { Job } from './types.js';

export function createJobStore(dataDir: string) {
  const directory = join(dataDir, 'jobs');

  async function load(): Promise<Job[]> {
    await ensureDirectory(directory);
    const jobs: Job[] = [];
    for (const filename of await readdir(directory)) {
      if (!/^[a-f0-9-]{36}\.json$/.test(filename)) continue;
      const job = await readJson<Job>(join(directory, filename));
      job.completedObjects ??= [];
      if (job.status === 'running' || job.status === 'cancelling') {
        job.status = 'interrupted';
        job.phase = 'interrupted';
        job.requiresRecovery = job.changes.length > 0;
        job.updatedAt = new Date().toISOString();
        job.error = { code: 'PROCESS_INTERRUPTED', message: 'The application stopped before the operation finished. Reconnect and check the recorded query before retrying.' };
        await save(job);
      }
      jobs.push(job);
    }
    return jobs;
  }

  async function save(job: Job): Promise<void> {
    await writeJson(join(directory, `${job.id}.json`), job);
  }

  return { load, save };
}
