# @falcondev-oss/queue

Type-safe job queues built on BullMQ and Standard Schema.

## Installation

```bash
npm add @falcondev-oss/queue
```

## Usage

#### 1. Define your job schemas and handlers

```ts
const jobs = defineJobs({
  video: {
    process: defineJob({
      schema: z.object({
        path: z.string(),
        outputFormat: z.enum(['mp4', 'avi', 'mov']),
      }),
      async run(payload) {
        // process video
      },
      workerOptions: {
        // only one video processed at a time
        concurrency: 1,
      },
    }),
  },
  sendEmail: defineJob({
    schema: z.object({
      to: z.email(),
      subject: z.string(),
      body: z.string(),
    }),
    async run(payload) {
      // send email to `payload.to` with `payload.subject` and `payload.body`
    },
  }),
})
```

#### 2. Start workers

```ts
await startWorkers(jobs)
```

#### 3. Queue jobs

Single jobs:

```ts
await jobs.video.process.queue({
  // payload type inferred from schema
  outputFormat: 'mp4',
  path: '/path/to/video.mov',
})
await jobs.sendEmail.queue({
  to: 'user@example.com',
  subject: 'Hello',
  body: 'This is a test email',
})
```

Bulk jobs:

```ts
await jobs.sendEmail.queueBulk([
  {
    to: 'user@example.com',
    subject: 'Hello',
    body: 'This is a test email',
  },
  {
    to: 'user2@example.com',
    subject: 'Hello 2',
    body: 'This is another test email',
  },
])
```
