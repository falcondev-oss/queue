import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Job as BullJob, QueueOptions as BullQueueOptions } from 'bullmq'
import type {
  DefineJobOptions,
  Job,
  JobDefinitionsObject,
  QueueJobProxy,
  QueueJobProxyAccessor,
  WorkerOptions,
} from './types'
import {} from '@standard-schema/spec'
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq'
import IORedis from 'ioredis'
import { internalSymbol, StandardSchemaV1Error } from './types'

export function defineJob<Schema extends StandardSchemaV1>(
  opts: DefineJobOptions<Schema>,
): Job<Schema> {
  return {
    ...opts,
    [internalSymbol]: {
      async addToQueue(queue: BullQueue, payload: unknown) {
        const parsed = await opts.schema['~standard'].validate(payload)
        if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

        return queue.add(queue.name, parsed.value)
      },
      async addToQueueBulk(queue: BullQueue, payloads: unknown[]) {
        return queue.addBulk(
          await Promise.all(
            payloads.map(async (payload) => {
              const parsed = await opts.schema['~standard'].validate(payload)
              if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

              return {
                name: queue.name,
                data: parsed.value,
              }
            }),
          ),
        )
      },
    },
  }
}

export function defineQueues<J extends JobDefinitionsObject>(jobs: J, opts?: BullQueueOptions) {
  const connection =
    opts?.connection ??
    new IORedis({
      maxRetriesPerRequest: null,
    })
  const queues = new Map<string, BullQueue>()

  async function getQueue(jobName: string) {
    let queue = queues.get(jobName)
    if (!queue) {
      queue = new BullQueue(jobName, {
        ...opts,
        connection,
      })
      queues.set(jobName, queue)
      await queue.waitUntilReady()
    }

    return queue
  }

  function createProxy(obj: JobDefinitionsObject, path: string[]) {
    return new Proxy(obj, {
      get(target, p, receiver) {
        if (typeof p !== 'string') return

        const fullPath = [...path, p]
        const jobOrJobs = Reflect.get(target, p, receiver)

        if (jobOrJobs && typeof jobOrJobs === 'object' && internalSymbol in jobOrJobs) {
          const jobName = fullPath.join('-')

          const accessor = async (payload: unknown) => {
            return jobOrJobs[internalSymbol].addToQueue(await getQueue(jobName), payload)
          }
          accessor.bulk = async (payloads: unknown[]) => {
            return jobOrJobs[internalSymbol].addToQueueBulk(await getQueue(jobName), payloads)
          }

          return accessor satisfies QueueJobProxyAccessor<any>
        }

        return createProxy(jobOrJobs, fullPath)
      },
    })
  }
  return createProxy(jobs, []) as unknown as QueueJobProxy<J>
}

export async function startWorkers<J extends JobDefinitionsObject>(jobs: J, opts?: WorkerOptions) {
  const connection =
    opts?.connection ??
    new IORedis({
      maxRetriesPerRequest: null,
    })
  const workers = new Map<string, BullWorker>()

  function traverse(current: JobDefinitionsObject, path: string[]) {
    for (const [key, value] of Object.entries(current)) {
      if (!value || typeof value !== 'object') continue

      const fullPath = [...path, key]

      if (typeof value === 'object' && internalSymbol in value) {
        const jobName = fullPath.join('-')

        const hooks = value.workerOptions?.hooks ?? opts?.hooks

        const worker = new BullWorker(
          jobName,
          async (job) => {
            return value.handler(job.data, job as BullJob<any, void, string>)
          },
          {
            ...opts,
            ...value.workerOptions,
            connection,
          },
        )

        if (hooks)
          for (const [hookName, hook] of Object.entries(hooks)) {
            worker.addListener(hookName, hook)
          }

        workers.set(jobName, worker)
      } else {
        traverse(value, fullPath)
      }
    }
  }
  traverse(jobs, [])

  await Promise.all([...workers.values()].map(async (worker) => worker.waitUntilReady()))
}
