# Postgres Queue

Job Queue written in Typescript and backed by PostgreSQL.

## Features

- Enqueue jobs
- Process jobs
- Retry jobs

## Requirements

- PostgreSQL 12 or higher

## Installation

```
# Yarn
yarn add @james-whiteley/job-queue

# NPM
npm install @james-whiteley/job-queue
```

## Usage

### Enqueue a Job

```
import { Queue } from '@james-whiteley/job-queue';

const queue = new Queue('test-queue');

queue
  .enqueue({
    data: { success: true },
    delay: 5000,
    retries: 0,
    backoff_strategy: 'linear',
    priority: priority
  })
  .then(() => console.log('Job added'));
```

### Process Jobs

#### Single thread

```
import { Consumer } from '@james-whiteley/job-queue';

const consumer = new Consumer(
  'test-queue',
  async (job) => {
    const { id, data } = job;
    data.success = Math.random() >= 0.5;

    if (!data.success) {
      throw new Error('Job failed');
    }

    return data;
  },
  {
    processOrder: 'PRIORITY'
  }
);

consumer.run();

// Events
consumer.onJobCompleted(({ job, returnValue }) => {
  console.log('Job completed:', job.id);
  console.log('Return value:', returnValue);
});

consumer.onJobFailed(({ job, error }) => {
  console.log('Job failed:', job.id);
  console.log(error);
});

consumer.onJobRetry(({ job, error }) => {
  console.log('Job retrying:', job.id);
  console.log(error);
});

consumer.onQueueEmpty((data) => {
  console.log('Queue empty');
  process.exit(0);
});
```

#### Concurrent worker threads

```
import { Consumer } from '@james-whiteley/job-queue';

const consumer = new Consumer(
  'test-queue',
  'job-function.js', // Path to separate file where function to be ran in job is exported
  {
    concurrency: 10,
    processOrder: 'FIFO'
  }
);

consumer.run();
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
