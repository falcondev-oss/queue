import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  Job as BullJob,
  Queue as BullQueue,
  WorkerListener as BullWorkerListener,
  WorkerOptions as BullWorkerOptions,
} from 'bullmq'

export class StandardSchemaV1Error extends Error {
  public readonly issues: ReadonlyArray<StandardSchemaV1.Issue>

  constructor(issues: ReadonlyArray<StandardSchemaV1.Issue>) {
    super(issues[0]?.message)
    this.name = 'SchemaError'
    this.issues = issues
  }
}

export const internalSymbol = Symbol('internal')

export type DefineJobOptions<Schema extends StandardSchemaV1> = {
  schema: Schema
  handler: (
    payload: StandardSchemaV1.InferOutput<Schema>,
    job: BullJob<StandardSchemaV1.InferInput<Schema>, void, string>,
  ) => void | Promise<void>
  workerOptions?: WorkerOptions<StandardSchemaV1.InferOutput<Schema>>
}

export type Job<Schema extends StandardSchemaV1> = DefineJobOptions<Schema> & {
  [internalSymbol]: {
    addToQueue: (queue: BullQueue, payload: unknown) => Promise<BullJob>
    addToQueueBulk: (queue: BullQueue, payloads: unknown[]) => Promise<BullJob[]>
  }
}

export type JobDefinitionsObject = {
  [key: string]: Job<any> | JobDefinitionsObject
}

export type QueueJobProxyAccessor<S extends StandardSchemaV1> = ((
  payload: StandardSchemaV1.InferInput<S>,
) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, void, string>>) & {
  bulk: (
    payloads: StandardSchemaV1.InferInput<S>[],
  ) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, void, string>[]>
}
export type QueueJobProxy<J extends JobDefinitionsObject> = {
  [K in keyof J]: J[K] extends Job<infer S>
    ? QueueJobProxyAccessor<S>
    : J[K] extends JobDefinitionsObject
      ? QueueJobProxy<J[K]>
      : never
}

export type WorkerOptions<Data = unknown> = Omit<Partial<BullWorkerOptions>, 'autorun' | 'name'> & {
  hooks?: Partial<BullWorkerListener<Data, void, string>>
}
