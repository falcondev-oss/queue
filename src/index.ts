import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  FlowJob as BullFlowJob,
  Job as BullJob,
  QueueOptions as BullQueueOptions,
  FlowJob,
} from 'bullmq'
import type {
  DefineFlowOptions,
  DefineJobOptions,
  Flow,
  FlowAccessor,
  FlowStep,
  Job,
  JobAccessor,
  JobDefinitionsObject,
  Jobs,
  WorkerOptions,
} from './types'
import { Queue as BullQueue, Worker as BullWorker, FlowProducer } from 'bullmq'
import IORedis from 'ioredis'
import { mapValues } from 'remeda'
import { FlowBuilder, flowSymbol, jobSymbol, StandardSchemaV1Error } from './types'

export { RateLimitError, UnrecoverableError } from 'bullmq'

export function defineJob<Schema extends StandardSchemaV1, Output>(
  opts: DefineJobOptions<Schema, Output>,
): Job<Schema, Output> {
  return {
    ...opts,
    [jobSymbol]: <Job<Schema, Output>[typeof jobSymbol]>{
      async addToQueue(queue, payload, jobOpts) {
        const parsed = await opts.schema['~standard'].validate(payload)
        if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

        return queue.add(queue.name, parsed.value, jobOpts)
      },
      async addToQueueBulk(queue, payloads, jobOpts) {
        return queue.addBulk(
          await Promise.all(
            payloads.map(async (payload) => {
              const parsed = await opts.schema['~standard'].validate(payload)
              if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

              return {
                name: queue.name,
                data: parsed.value,
                opts: jobOpts,
              }
            }),
          ),
        )
      },
    },
  }
}

// const flowProducter = new FlowProducer()
function buildFlowJobStack(opts: {
  rootInputPayload: unknown
  steps: FlowStep<any, any>[]
  flowName: string
  flowOpts?: FlowJob['opts']
}) {
  if (opts.steps.length === 0) throw new Error(`Flow ${opts.flowName} has no steps`)
  const firstStep = opts.steps[0]!

  let currentStep: BullFlowJob = {
    name: `${opts.flowName}_${firstStep.name}`,
    queueName: `${opts.flowName}_${firstStep.name}`,
    data: opts.rootInputPayload,
    opts: opts.flowOpts,
  }

  for (const step of opts.steps.slice(1)) {
    currentStep = {
      name: `${opts.flowName}_${step.name}`,
      queueName: `${opts.flowName}_${step.name}`,
      children: [currentStep],
      opts: opts.flowOpts,
    }
  }

  return currentStep
}

export function defineFlow<Schema extends StandardSchemaV1, Output>(
  opts: DefineFlowOptions<Schema, Output>,
): Flow<Schema, Output> {
  const steps = opts.flow(new FlowBuilder()).steps
  return {
    ...opts,
    [flowSymbol]: <Flow<Schema, Output>[typeof flowSymbol]>{
      steps,
      async addToQueue(flowName, flowProducer, payload, flowOpts) {
        const parsed = await opts.schema['~standard'].validate(payload)
        if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

        return flowProducer.add(
          buildFlowJobStack({
            rootInputPayload: parsed.value,
            steps,
            flowName,
          }),
          flowOpts,
        )
      },
      async addToQueueBulk(flowName, flowProducer, payloads, flowOpts) {
        return flowProducer.addBulk(
          await Promise.all(
            payloads.map(async (payload) => {
              const parsed = await opts.schema['~standard'].validate(payload)
              if (parsed.issues) throw new StandardSchemaV1Error(parsed.issues)

              return buildFlowJobStack({
                flowName,
                rootInputPayload: parsed.value,
                steps,
                flowOpts,
              })
            }),
          ),
        )
      },
    },
  }
}

export function defineJobs<J extends JobDefinitionsObject>(jobs: J, opts?: BullQueueOptions) {
  const connection =
    opts?.connection ??
    new IORedis({
      maxRetriesPerRequest: null,
    })

  const queues = new Map<string, BullQueue<BullJob<any, any, string>>>()
  async function getQueue(jobName: string) {
    if (queues.has(jobName)) return queues.get(jobName)!

    const queue = new BullQueue<BullJob<any, any, string>>(jobName, {
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

  let flowProducer: FlowProducer | null = null
  async function getFlowProducer() {
    if (flowProducer) return flowProducer

    flowProducer = new FlowProducer({
      connection,
    })
    await flowProducer.waitUntilReady()
    return flowProducer
  }

  function traverse(obj: JobDefinitionsObject, path: string[]): Jobs<any> {
    return mapValues(obj, (jobOrJobs, p) => {
      if (typeof jobOrJobs !== 'object') throw new Error('Job definition must be an object')

      const fullPath = [...path, p]

      if (jobSymbol in jobOrJobs) {
        const jobName = fullPath.join('-')

        return {
          queue: async (payload: unknown) => {
            return jobOrJobs[jobSymbol].addToQueue(await getQueue(jobName), payload)
          },
          queueBulk: async (payloads: unknown[]) => {
            return jobOrJobs[jobSymbol].addToQueueBulk(await getQueue(jobName), payloads)
          },
        } satisfies JobAccessor<any, any>
      } else if (flowSymbol in jobOrJobs) {
        const flowName = fullPath.join('-')

        return {
          queue: async (payload: unknown) => {
            return jobOrJobs[flowSymbol].addToQueue(flowName, await getFlowProducer(), payload)
          },
          queueBulk: async (payloads: unknown[]) => {
            return jobOrJobs[flowSymbol].addToQueueBulk(flowName, await getFlowProducer(), payloads)
          },
        } satisfies FlowAccessor<any>
      }

      return traverse(jobOrJobs, fullPath)
    })
  }
  return traverse(jobs, []) as Jobs<J>
}

export async function startWorkers<J extends JobDefinitionsObject>(
  jobs: J,
  opts?: WorkerOptions<any, any>,
) {
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

      if (jobSymbol in value) {
        const jobName = fullPath.join('-')

        const worker = new BullWorker(
          jobName,
          async (job) => {
            // eslint-disable-next-line ts/no-unsafe-return
            return value.run(job.data, job)
          },
          {
            ...opts,
            ...value.workerOptions,
            connection,
          },
        )

        const hooks = value.workerOptions?.hooks ?? opts?.hooks
        if (hooks)
          for (const [hookName, hook] of Object.entries(hooks)) {
            worker.addListener(hookName, hook)
          }

        workers.set(jobName, worker)
      } else if (flowSymbol in value) {
        const flowName = fullPath.join('-')

        // add workers for each step
        for (const step of value[flowSymbol].steps) {
          const jobName = `${flowName}_${step.name}`
          const stepWorker = new BullWorker(
            jobName,
            async (job) => {
              // first step gets job data as input
              // eslint-disable-next-line ts/no-unsafe-return
              if (value[flowSymbol].steps.indexOf(step) === 0) return step.run(job.data, job)

              // eslint-disable-next-line ts/no-unsafe-return
              const results = await job.getChildrenValues().then((res) => Object.values(res))
              if (results.length !== 1)
                throw new Error('Flow job should have exactly one child job')

              // eslint-disable-next-line ts/no-unsafe-assignment
              const input = results[0]
              // eslint-disable-next-line ts/no-unsafe-return
              return step.run(input, job)
            },
            {
              ...opts,
              ...step.workerOptions,
              connection,
            },
          )

          const hooks = value.workerOptions?.hooks ?? opts?.hooks
          if (hooks)
            for (const [hookName, hook] of Object.entries(hooks)) {
              stepWorker.addListener(hookName, hook)
            }

          workers.set(jobName, stepWorker)
        }
      } else {
        traverse(value, fullPath)
      }
    }
  }
  traverse(jobs, [])

  await Promise.all([...workers.values()].map(async (worker) => worker.waitUntilReady()))
}
