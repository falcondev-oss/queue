# @falcondev-oss/queue

Type-safe job queues built on BullMQ and Standard Schema.

## Installation

```bash
npm add @falcondev-oss/queue
```

## Usage

#### 1. Define your job schemas and handlers

```ts
const jobs = {
  video: {
    process: defineJob({
      schema: z.object({
        path: z.string(),
        outputFormat: z.enum(['mp4', 'avi', 'mov']),
      }),
      async run(payload) {
        await processVideo(payload.path, payload.outputFormat)
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
      await sendEmail(payload.to, payload.subject, payload.body)
    },
  }),
}
```

#### 2. Create queue client

You only need the type of your job definitions, so it's easier to use across your entire backend stack without any runtime dependencies.

```ts
const queue = createQueueClient<typeof jobs>()
```

#### 2. Queue jobs

Single jobs:

```ts
await queue.video.process.add({
  outputFormat: 'mp4',
  path: '/path/to/video.mov',
})
await queue.sendEmail.add({
  to: 'user@example.com',
  subject: 'Hello',
  body: 'This is a test email',
})
```

Bulk jobs:

```ts
await queue.sendEmail.addBulk([
  {
    payload: {
      to: 'user@example.com',
      subject: 'Hello',
      body: 'This is a test email',
    },
  },
  {
    payload: {
      to: 'user2@example.com',
      subject: 'Hello 2',
      body: 'This is another test email',
    },
    opts: {
      attempts: 5, // override default job options
    },
  },
])
```

#### 3. Start workers

```ts
await startWorkers(jobs)
```
