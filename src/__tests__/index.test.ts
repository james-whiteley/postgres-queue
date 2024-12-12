jest.mock('worker_threads', () => {
  return {
    Worker: jest.fn().mockImplementation(() => ({
      on: jest.fn(),
      postMessage: jest.fn(),
    })),
  };
});

import { Queue, Consumer, IJob } from '../index';
import { EventEmitter } from 'events';

describe('Queue', () => {
  let queue: Queue;
  const queueName = 'testQueue';

  beforeEach(() => {
    queue = new Queue(queueName);
    jest.spyOn(queue, 'queryAndRelease').mockResolvedValue({
      rows: [],
      command: '',
      rowCount: 0,
      oid: 0,
      fields: []
    });
  });

  afterEach(async () => {
    jest.clearAllMocks();
  });

  it('should initialize with the correct queue name and event emitter', () => {
    expect(queue.queueName).toBe(queueName);
    expect(queue.queueEvents).toBeInstanceOf(EventEmitter);
  });

  it('should enqueue a job correctly', async () => {
    const job: { data: object, delay?: number, retries?: number, backoff_strategy?: 'linear' | 'exponential', priority?: number } = { data: { key: 'value' }, delay: 1000, retries: 3, backoff_strategy: 'linear', priority: 1 };
    await queue.enqueue(job);

    expect(queue.queryAndRelease).toHaveBeenCalledWith(
      'INSERT INTO jobs (queue, data, delay, retries, backoff_strategy, priority) VALUES ($1, $2, $3, $4, $5, $6)',
      [queueName, job.data, job.delay, job.retries, job.backoff_strategy, job.priority]
    );
  });

  it('should count jobs in progress correctly', async () => {
    jest.spyOn(queue, 'queryAndRelease').mockResolvedValueOnce({
      rows: [{ count: 3 }] as unknown as [{ count: string }][],
      command: '',
      rowCount: 3,
      oid: 0,
      fields: []
    });
    const count = await queue.jobsInProgressCount();
    expect(count).toBe(3);
  });
});

describe('Consumer', () => {
  let consumer: Consumer;
  const queueName = 'testQueue';
  const callback = jest.fn();

  const job: IJob = {
    id: 1,
    queue: queueName,
    attempts: 0,
    retries: 1,
    delay: 0,
    backoff_strategy: 'none',
    data: {},
    status: 'idle',
    priority: 1,
    created_at: new Date(),
    updated_at: new Date(),
    progress: 0
  };

  beforeEach(() => {
    consumer = new Consumer(queueName, callback, { concurrency: 2, processOrder: 'FIFO', parallelismMethod: 'thread' });
    jest.spyOn(consumer.workerEvents, 'emit');
    jest.spyOn(consumer, 'queryAndRelease').mockResolvedValue({
      rows: [],
      command: '',
      rowCount: 0,
      oid: 0,
      fields: []
    });
    jest.spyOn(consumer, 'query').mockResolvedValue({
      rows: [],
      command: '',
      rowCount: 0,
      oid: 0,
      fields: []
    } as never);
  });

  afterEach(async () => {
    jest.clearAllMocks();
    if (consumer) {
      await consumer.cleanup();
    }
  });

  test('should initialize with correct properties', () => {
    expect(consumer.queueName).toBe(queueName);
    expect(consumer.callback).toBe(callback);
    expect(consumer.concurrency).toBe(2);
    expect(consumer.processOrder).toBe('FIFO');
    expect(consumer.parallelismMethod).toBe('thread');
    expect(consumer.workerStatus).toBe('idle');
    expect(consumer.workerEvents).toBeInstanceOf(EventEmitter);
  });

  test('should listen for job notifications if main thread', async () => {
    const listenForJobNotificationsSpy = jest.spyOn(consumer, 'listenForJobNotifications');
    await consumer.listenForJobNotifications();
    expect(listenForJobNotificationsSpy).toHaveBeenCalled();
  });

  test('should create idle worker threads', () => {
    consumer.createIdleWorkerThreads(2);
    expect(consumer.workers?.size).toBe(2);
  });

  test('should handle job completion', async () => {
    const getJobSpy = jest.spyOn(consumer, 'getJob').mockResolvedValueOnce(job);
    const delayJobSpy = jest.spyOn(consumer, 'delayJob').mockResolvedValueOnce(void 0);
    const callbackSpy = jest.fn().mockResolvedValueOnce({ data: 'test' });
    consumer.callback = callbackSpy;

    await consumer.run();

    expect(getJobSpy).toHaveBeenCalled();
    expect(delayJobSpy).toHaveBeenCalled();
    expect(callbackSpy).toHaveBeenCalled();
    expect(consumer.workerEvents.emit).toHaveBeenCalledWith('job_completed', { job: undefined, returnValue: { data: 'test' } });
  });

  test('should handle job failure and retry', async () => {
    const retryJob = { ...job, retries: 1 };
    const getJobSpy = jest.spyOn(consumer, 'getJob').mockResolvedValueOnce(retryJob);
    const delayJobSpy = jest.spyOn(consumer, 'delayJob').mockResolvedValueOnce(undefined);
    const callbackSpy = jest.fn().mockRejectedValueOnce(new Error('Job failed'));
    consumer.callback = callbackSpy;

    await consumer.run();

    expect(getJobSpy).toHaveBeenCalled();
    expect(delayJobSpy).toHaveBeenCalledWith(job);
    expect(callbackSpy).toHaveBeenCalledWith(job);
    expect(consumer.workerEvents.emit).toHaveBeenCalledWith('job_retry', undefined);
  });

  test('should handle job failure with no retries left', async () => {
    const failJob = { ...job, attempts: 1, retries: 0 };
    const getJobSpy = jest.spyOn(consumer, 'getJob').mockResolvedValueOnce(failJob);
    const callbackSpy = jest.fn().mockRejectedValueOnce(new Error('Job failed'));
    consumer.callback = callbackSpy;

    await consumer.run();

    expect(getJobSpy).toHaveBeenCalled();
    expect(consumer.workerEvents.emit).toHaveBeenCalledWith('job_failed', undefined);
  });
});