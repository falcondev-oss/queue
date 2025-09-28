import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  BulkJobOptions,
  Job as BullJob,
  WorkerListener as BullWorkerListener,
  WorkerOptions as BullWorkerOptions,
  JobsOptions,
} from 'bullmq'

export const jobSymbol = Symbol('job')

export type DefineJobOptions<Schema extends StandardSchemaV1, Output> = {
  schema: Schema
  run: (
    payload: StandardSchemaV1.InferOutput<Schema>,
    job: BullJob<StandardSchemaV1.InferInput<Schema>, Output, string>,
  ) => Output
  workerOptions?: WorkerOptions<StandardSchemaV1.InferOutput<Schema>, Output>
}

export type Job<Schema extends StandardSchemaV1, Output> = DefineJobOptions<Schema, Output> & {
  [jobSymbol]: true
}

export type JobDefinitions = {
  [key: string]: Job<StandardSchemaV1<any, any>, any> | JobDefinitions
}

export type JobAccessor<S extends StandardSchemaV1, Output> = {
  add: (
    payload: StandardSchemaV1.InferInput<S>,
    opts?: JobsOptions,
  ) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, Output, string>>
  addBulk: (
    jobs: {
      payload: StandardSchemaV1.InferInput<S>
      opts?: BulkJobOptions
    }[],
  ) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, Output, string>[]>
}
export type QueueClient<J extends JobDefinitions> = {
  [K in keyof J]: J[K] extends Job<infer S, infer O>
    ? JobAccessor<S, O>
    : J[K] extends JobDefinitions
      ? QueueClient<J[K]>
      : never
}

export type WorkerOptions<Input, Output> = Omit<Partial<BullWorkerOptions>, 'autorun' | 'name'> & {
  hooks?: Partial<BullWorkerListener<Input, Output, string>>
}

export type inferJobInput<J> = J extends Job<infer S, any> ? StandardSchemaV1.InferInput<S> : never
export type inferJobPayload<J> =
  J extends Job<infer S, any> ? StandardSchemaV1.InferOutput<S> : never
export type inferJobOutput<J> = J extends Job<any, infer O> ? O : never
