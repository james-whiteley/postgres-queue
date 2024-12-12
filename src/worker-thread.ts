import { parentPort, workerData } from 'worker_threads';
import { Consumer, type IJob } from '.';

const consumer = new Consumer(workerData.queueName, workerData.callback, { processOrder: workerData.processOrder });

parentPort?.on('message', async (data: { action: string }) => {
  const { action } = data;
  if (action === 'start_job') {
    // Mark the worker as busy
    parentPort?.postMessage({ action: 'set_worker_busy' });

    const job: IJob | null = await consumer.getJob();

    if (!job) {
      parentPort?.postMessage({ action: 'no_more_jobs' });
      process.exit(1001);
    } else {
      try {
        if (job.attempts === (job.retries + 1)) {
          throw new Error('Job has no more tries left');
        }

        await consumer.delayJob(job);

        const { default: jobFunction } = await import(`${process.cwd()}/${workerData.callback}`);
        const result = await jobFunction(job);

        const response = await consumer.queryAndRelease('UPDATE jobs SET status = $1, progress = 100, updated_at = NOW() WHERE id = $2 AND queue = $3 RETURNING *', ['completed', job.id, workerData.queueName]);
        const completedJob = response.rows[0] as unknown as IJob;
        parentPort?.postMessage({ action: 'job_completed', job: completedJob, returnValue: result });
      } catch (error: unknown) {
        if (job.attempts === (job.retries + 1)) {
          const response = await consumer.queryAndRelease('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND queue = $3 RETURNING *', ['failed', job.id, workerData.queueName]);
          const failedJob = response.rows[0] as unknown as IJob;
          parentPort?.postMessage({ action: 'job_failed', job: failedJob, error: error });
        } else {
          const response = await consumer.queryAndRelease('UPDATE jobs SET attempts = $1, status = $2, updated_at = NOW() WHERE id = $3 AND queue = $4 RETURNING *', [Number(job.attempts) + 1, 'idle', job.id, workerData.queueName]);
          const retryJob = response.rows[0] as unknown as IJob;
          parentPort?.postMessage({ action: 'job_retry', job: retryJob, error: error });
        }
      }
    }

    // Mark the worker as idle
    parentPort?.postMessage({ action: 'set_worker_idle' });
  }
});
