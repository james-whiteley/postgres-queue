import { Pool, PoolClient, QueryArrayResult } from 'pg';
import { Worker, isMainThread } from 'worker_threads';
import { EventEmitter } from 'events';
import { PathLike } from 'fs';

export class DBConnection extends Pool {
  constructor() {
    super({
      user: process.env.POSTGRES_USER,
      host: process.env.POSTGRES_HOST,
      database: process.env.POSTGRES_DB,
      password: process.env.POSTGRES_PASSWORD,
      port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    });
  }

  async queryWithoutRelease(query: any, values?: any): Promise<PoolClient> {
    const client = await this.connect();
    try {
      await client.query(query, values);
      return client;
    } catch (error: any) {
      throw error;
    }
  }

  async query(query: any, values?: any): Promise<QueryArrayResult> {
    const client = await this.connect();
    try {
      return await client.query(query, values);
    } finally {
      client.release();
    }
  }

  async release() {
    await this.end();
  }
}

export class Queue extends DBConnection {
  queueName: string;
  queueEvents: EventEmitter;

  constructor(queueName: string) {
    super();
    this.queueName = queueName;
    this.queueEvents = new EventEmitter();
  }

  onQueueCompleted(listener: (data: any) => void) {
    this.queueEvents.on('queue_completed', listener);
  }

  onProgress(listener: (data: any) => void) {
    this.queueEvents.on('progress', listener);
  }

  async enqueue(job: { data: object, delay?: number, retries?: number, backoff_strategy?: 'linear' | 'exponential', priority?: number }) {
    const query = 'INSERT INTO jobs (queue, data, delay, retries, backoff_strategy, priority) VALUES ($1, $2, $3, $4, $5, $6)';
    const values = [this.queueName, job.data, job.delay, job.retries, job.backoff_strategy, job.priority];

    try {
      return await this.query(query, values);
    } catch (error: any) {
      throw new Error(error);
    }
  }

  async jobsInProgressCount() {
    try {
      const res = await this.query('SELECT COUNT(*) FROM jobs WHERE status = \'processing\'');
      return parseInt((res.rows[0] as unknown as { count: string }).count, 10);
    } catch (error: any) {
      throw new Error('Error querying jobs in progress', error);

    }
  }
}

interface ConsumerOptions {
  concurrency?: number;
  processOrder?: 'FIFO' | 'LIFO' | 'PRIORITY';
  parallelismMethod?: 'thread' | 'process';
}

type JobDataObject = Partial<Record<string, unknown>>;

export interface IJob {
  id: number,
  queue: string,
  data: JobDataObject,
  status: 'idle' | 'processing' | 'failed' | 'cancelled' | 'completed',
  attempts: number,
  retries: number,
  backoff_strategy: 'none' | 'linear' | 'exponential',
  delay: number,
  priority: number,
  created_at: Date,
  updated_at: Date,
  progress: number
}

export class Consumer extends DBConnection {
  workerStatus?: 'idle' | 'busy';
  workerEvents: EventEmitter;
  queueName: string;
  callback: Function | PathLike;
  concurrency?: number;
  processOrder?: 'FIFO' | 'LIFO' | 'PRIORITY';
  parallelismMethod?: 'thread' | 'process';
  workers?: Set<Worker>;
  jobListenerClient?: PoolClient;

  constructor(queueName: string, callback: Function | PathLike, { concurrency = 1, processOrder = 'FIFO', parallelismMethod = 'thread' }: Partial<ConsumerOptions> = {}) {
    super();
    this.workerStatus = 'idle';
    this.queueName = queueName;
    this.callback = callback;
    this.concurrency = concurrency;
    this.processOrder = processOrder;
    this.parallelismMethod = parallelismMethod;
    this.workerEvents = new EventEmitter;

    this.onQueueEmpty((data) => {
      if (data.queueName === this.queueName) {
        this.workerEvents.emit('worker_idle');
      }
    });

    this.onWorkerIdle(() => {
      this.workerStatus = 'idle';
      this.listenForJobNotifications();
    });

    this.onWorkerBusy(() => {
      this.workerStatus = 'busy';
      this.unlistenForJobNotifications();
    });

    if (parallelismMethod === 'thread' && concurrency && concurrency > 1) {
      this.workers = new Set<Worker>();
    }
  }

  async getJob(): Promise<IJob | null> {
    try {
      await this.query('BEGIN');

      let queryOrder = 'id ASC';
      if (this.processOrder === 'LIFO') {
        queryOrder = 'id DESC';
      } else if (this.processOrder === 'PRIORITY') {
        queryOrder = 'priority DESC';
      }

      const query = `UPDATE jobs SET status = 'processing', updated_at = NOW() WHERE id = (SELECT id FROM jobs WHERE queue = '${this.queueName}' AND status = 'idle' ORDER BY ${queryOrder} LIMIT 1 FOR UPDATE SKIP LOCKED) RETURNING *;`;
      const res = await this.query(query);

      if (res.rows.length === 0) {
        await this.query('ROLLBACK');
        return null;
      }

      await this.query('COMMIT');

      return res.rows[0] as unknown as IJob;
    } catch (error: any) {
      await this.query('ROLLBACK');
      throw new Error(error);
    }
  }

  async listenForJobNotifications() {
    if (!isMainThread) {
      return;
    }

    try {
      if (this.jobListenerClient) {
        await this.unlistenForJobNotifications();
      }

      this.jobListenerClient = await this.queryWithoutRelease('LISTEN new_job');

      // Listen for new job notifications
      this.jobListenerClient.on('notification', (msg) => {
        let payload: IJob | null = null;
        if (msg.payload) {
          payload = JSON.parse(msg.payload);
        }

        if (payload?.queue === this.queueName) {
          this.run();
        }
      });

      process.on('SIGINT', this.unlistenForJobNotifications.bind(this));
    } catch (err: any) {
      console.error('Error setting up LISTEN/NOTIFY', err.stack);
    }
  }

  async unlistenForJobNotifications() {
    if (!isMainThread) {
      return;
    }

    await this.jobListenerClient?.query('UNLISTEN new_job');
    this.jobListenerClient?.release();
    this.jobListenerClient = undefined;
  }

  async delayJob(job: IJob) {
    if (job.delay > 0 && job.backoff_strategy === 'none') {
      await new Promise(resolve => setTimeout(resolve, job.delay));
    }

    if (job.delay > 0 && job.backoff_strategy === 'exponential') {
      const delay = job.delay * Math.pow(2, job.attempts);
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    if (job.delay > 0 && job.backoff_strategy === 'linear') {
      const delay = job.delay * job.attempts;
      await new Promise(resolve => setTimeout(resolve, delay));
    }

    return;
  }

  async run(): Promise<void> {
    this.workerEvents.emit('worker_busy');

    if (typeof this.callback !== 'function') {
      return this.runSandboxed();
    }

    const job: IJob | null = await this.getJob();

    if (!job) {
      this.workerEvents.emit('worker_idle');
      return;
    }

    try {
      if (job.attempts === (job.retries + 1)) {
        throw new Error('Job has no more tries left');
      }

      await this.delayJob(job);

      const result = await this.callback(job);

      const res = await this.query('UPDATE jobs SET status = $1, progress = 100, updated_at = NOW() WHERE id = $2 AND queue = $3 RETURNING *', ['completed', job.id, this.queueName]);
      const completedJob = res.rows[0];
      this.workerEvents.emit('job_completed', { job: completedJob, returnValue: result });
    } catch (error: any) {
      if (job.attempts === (job.retries + 1)) {
        const failedJob = await this.query('UPDATE jobs SET status = $1, updated_at = NOW() WHERE id = $2 AND queue = $3 RETURNING *', ['failed', job.id, this.queueName]);
        this.workerEvents.emit('job_failed', failedJob.rows[0]);
      } else {
        const retryJob = await this.query('UPDATE jobs SET attempts = $1, status = $2, updated_at = NOW() WHERE id = $3 AND queue = $4 RETURNING *', [Number(job.attempts) + 1, 'idle', job.id, this.queueName]);
        this.workerEvents.emit('job_retry', retryJob.rows[0]);
      }
    }

    return this.run();
  }

  async runSandboxed(): Promise<void> {
    if (this.parallelismMethod === 'process') {
      console.log('Process mode is not currently supported');
    } else {
      const concurrency = (this.concurrency?? 1) - (this.workers?.size ?? 0);
      this.createIdleWorkerThreads(concurrency);
      this.startWorkerThreads();
    }
  }

  createIdleWorkerThreads(threadCount: number = 1) {;
    for (let i = 0; i < threadCount; i++) {
      const worker = this.createIdleWorkerThread();
      this.workers?.add(worker);
    }
  }

  createIdleWorkerThread() {
    const worker = new Worker(
      `${__dirname}/worker-thread.js`,
      {
        workerData: {
          queueName: this.queueName,
          callback: this.callback,
          processOrder: this.processOrder
        }
      }
    );

    worker.on('message', (data: any) => {
      // Handle worker activity
      const { action } = data;

      if (action === 'set_worker_busy') { }

      if (action === 'set_worker_idle') {
        // Worker is now idle, start a new job
        worker.postMessage({ action: 'start_job' });
      }

      if (action === 'no_more_jobs') {}

      if (action === 'job_completed') {
        // Job is complete
        this.workerEvents.emit('job_completed', data);
      }

      if (action === 'job_retry') {
        // Job failed, retry
        this.workerEvents.emit('job_retry', data);
      }

      if (action === 'job_failed') {
        // Job failed
        this.workerEvents.emit('job_failed', data);
      }
    });

    worker.on('error', (error: any) => {
      console.error('Worker error:', error);
    });

    worker.on('exit', async (code: any) => {
      worker.removeAllListeners();

      // Remove the worker from the active workers set
      this.workers?.delete(worker);

      if (code === 1001) {
        // No more jobs to process
        if (this.workers?.size === 0) {
          try {
            const res = await this.query(`SELECT status, COUNT(*) FROM jobs WHERE queue = '${this.queueName}' AND status IN ('completed', 'failed') GROUP BY status`);
            const counts = res.rows.reduce((acc: any, row: any) => {
              acc[row.status] = row.count;
              return acc;
            }, {});

            this.workerEvents.emit('queue_empty', { queueName: this.queueName, counts });
          } catch (err: any) {
            console.error('Error querying job counts', err.stack);
          }
        }
      } else if (code !== 0) {
        const newIdleWorker = this.createIdleWorkerThread();
        this.workers?.add(newIdleWorker);
        newIdleWorker.postMessage({ action: 'start_job' });
      } else {
        if (this.workers?.size === 0) {
          try {
            const res = await this.query(`SELECT status, COUNT(*) FROM jobs WHERE queue = '${this.queueName}' AND status IN ('completed', 'failed') GROUP BY status`);
            const counts = res.rows.reduce((acc: any, row: any) => {
              acc[row.status] = row.count;
              return acc;
            }, {});

            this.workerEvents.emit('queue_empty', { queueName: this.queueName, counts });
          } catch (err: any) {
            console.error('Error querying job counts', err.stack);
          }
        }
      }
    });

    return worker;
  };

  startWorkerThreads() {
    const workersArray: Worker[] = Array.from(this.workers ?? []);

    for (const worker of workersArray) {
      if (!worker) {
        console.log('No idle workers');
        return;
      }

      // Assign a task to the idle worker
      worker.postMessage({ action: 'start_job' });
    }
  }

  onWorkerIdle(listener: (data: any) => void) {
    this.workerEvents.on('worker_idle', listener);
  }

  onWorkerBusy(listener: () => void) {
    this.workerEvents.on('worker_busy', listener);
  }

  onNextJob(listener: () => void) {
    this.workerEvents.on('next_job', listener);
  }

  onJobCompleted<T = unknown>(listener: ({ job, returnValue }: { job: IJob, returnValue: T }) => void) {
    this.workerEvents.on('job_completed', listener);
  }

  onJobFailed<T = unknown>(listener: ({ job, error }: { job: IJob, error: T }) => void) {
    this.workerEvents.on('job_failed', listener);
  }

  onJobRetry<T = unknown>(listener: ({ job, error }: { job: IJob, error: T }) => void) {
    this.workerEvents.on('job_retry', listener);
  }

  onQueueEmpty(listener: (data: any) => void) {
    this.workerEvents.on('queue_empty', listener);
  }

  async cleanup() {
    process.removeListener('SIGINT', this.unlistenForJobNotifications.bind(this));
    await this.unlistenForJobNotifications();
  }
}

