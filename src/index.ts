import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Job as BullJob, QueueOptions as BullQueueOptions } from 'bullmq'
import type { SuperJSONResult } from 'superjson'
import type {
  DefineJobOptions,
  Job,
  JobAccessor,
  JobDefinitions,
  QueueClient,
  WorkerOptions,
} from './types'
import { Queue as BullQueue, Worker as BullWorker, UnrecoverableError } from 'bullmq'
import IORedis from 'ioredis'
import superjson from 'superjson'
import { jobSymbol } from './types'

export { RateLimitError, UnrecoverableError } from 'bullmq'

export function defineJob<Schema extends StandardSchemaV1, Output>(
  opts: DefineJobOptions<Schema, Output>,
): Job<Schema, Output> {
  return {
    ...opts,
    [jobSymbol]: true,
  }
}

export function createQueueClient<J extends JobDefinitions>(opts?: BullQueueOptions) {
  const connection =
    opts?.connection ??
    new IORedis({
      maxRetriesPerRequest: null,
    })

  const queues = new Map<string, BullQueue<BullJob<SuperJSONResult, any, string>>>()
  async function getQueue(jobName: string) {
    if (queues.has(jobName)) return queues.get(jobName)!

    const queue = new BullQueue<BullJob<SuperJSONResult, any, string>>(jobName, {
      ...opts,
      defaultJobOptions: {
        removeOnComplete: true,
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        ...opts?.defaultJobOptions,
      },
      connection,
    })
    queues.set(jobName, queue)
    await queue.waitUntilReady()

    return queue
  }

  function createProxy(path: PropertyKey[]) {
    return new Proxy(
      {},
      {
        get(_target, propertyKey) {
          if (propertyKey === 'add') {
            const jobName = path.join('-')
            return (async (payload, jobOpts) => {
              const queue = await getQueue(jobName)
              return queue.add(queue.name, superjson.serialize(payload), jobOpts)
            }) satisfies JobAccessor<any, any>['add']
          }

          if (propertyKey === 'addBulk') {
            const jobName = path.join('-')
            return (async (bulkJobs) => {
              const queue = await getQueue(jobName)
              return queue.addBulk(
                bulkJobs.map((job) => ({
                  name: jobName,
                  data: superjson.serialize(job.payload),
                  opts: job.opts,
                })),
              )
            }) satisfies JobAccessor<any, any>['addBulk']
          }

          return createProxy([...path, propertyKey])
        },
      },
    )
  }
  return createProxy([]) as QueueClient<J>
}

export async function startWorkers<J extends JobDefinitions>(
  jobDefinitions: J,
  defaultOpts?: WorkerOptions<any, any>,
) {
  const connection =
    defaultOpts?.connection ??
    new IORedis({
      maxRetriesPerRequest: null,
    })
  const workers = new Map<string, BullWorker>()

  function traverse(current: JobDefinitions, path: string[]) {
    for (const [key, jobDefinition] of Object.entries(current)) {
      if (!jobDefinition || typeof jobDefinition !== 'object') continue

      const fullPath = [...path, key]

      if (jobSymbol in jobDefinition) {
        const jobName = fullPath.join('-')

        const worker = new BullWorker<SuperJSONResult, unknown, string>(
          jobName,
          async (job) => {
            const parsedData = superjson.deserialize(job.data)
            const parsed = await jobDefinition.schema['~standard'].validate(parsedData)
            if (parsed.issues) throw new UnrecoverableError(parsed.issues[0]?.message)

            // eslint-disable-next-line ts/no-unsafe-return
            return jobDefinition.run(parsed.value, job)
          },
          {
            ...defaultOpts,
            ...jobDefinition.workerOptions,
            connection,
          },
        )

        const hooks = jobDefinition.workerOptions?.hooks ?? defaultOpts?.hooks
        if (hooks)
          for (const [hookName, hook] of Object.entries(hooks)) {
            worker.addListener(hookName, hook)
          }

        workers.set(jobName, worker)
      } else {
        traverse(jobDefinition, fullPath)
      }
    }
  }
  traverse(jobDefinitions, [])

  await Promise.all([...workers.values()].map(async (worker) => worker.waitUntilReady()))
  return workers
}
