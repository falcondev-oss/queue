import type { StandardSchemaV1 } from '@standard-schema/spec'
import type {
  Job as BullJob,
  Queue as BullQueue,
  WorkerListener as BullWorkerListener,
  WorkerOptions as BullWorkerOptions,
  FlowProducer,
  JobNode,
} from 'bullmq'

export class StandardSchemaV1Error extends Error {
  public readonly issues: ReadonlyArray<StandardSchemaV1.Issue>

  constructor(issues: ReadonlyArray<StandardSchemaV1.Issue>) {
    super(issues[0]?.message)
    this.name = 'SchemaError'
    this.issues = issues
  }
}

export const jobSymbol = Symbol('job')
export const flowSymbol = Symbol('flow')

export type DefineJobOptions<Schema extends StandardSchemaV1, Output> = {
  schema: Schema
  run: (
    payload: StandardSchemaV1.InferOutput<Schema>,
    job: BullJob<StandardSchemaV1.InferInput<Schema>, Output, string>,
  ) => Output
  workerOptions?: WorkerOptions<StandardSchemaV1.InferOutput<Schema>, Output>
}

export type Job<Schema extends StandardSchemaV1, Output> = DefineJobOptions<Schema, Output> & {
  [jobSymbol]: {
    addToQueue: (
      queue: BullQueue<BullJob<StandardSchemaV1.InferOutput<Schema>, Output, string>>,
      payload: StandardSchemaV1.InferInput<Schema>,
    ) => Promise<BullJob<StandardSchemaV1.InferOutput<Schema>, Output, string>>
    addToQueueBulk: (
      queue: BullQueue<BullJob<StandardSchemaV1.InferOutput<Schema>, Output, string>>,
      payloads: StandardSchemaV1.InferInput<Schema>[],
    ) => Promise<BullJob<StandardSchemaV1.InferOutput<Schema>, Output, string>[]>
  }
}

export type FlowStep<Schema extends StandardSchemaV1, Output> = Omit<
  DefineJobOptions<Schema, Output>,
  'schema'
> & { name: string }
// eslint-disable-next-line unused-imports/no-unused-vars
export type FlowBuilderResult<Output> = {
  steps: FlowStep<any, any>[]
}

export class FlowBuilder<Input> {
  private steps: FlowStep<any, any>[] = []

  step<Output>(
    name: string,
    run: DefineJobOptions<StandardSchemaV1<Input, Input>, Output>['run'],
    opts?: Omit<DefineJobOptions<StandardSchemaV1<Input, Input>, Output>, 'schema' | 'run'>,
  ) {
    this.steps.push({
      ...opts,
      run,
      name,
    })
    return this as FlowBuilder<Awaited<Output>>
  }

  build(): FlowBuilderResult<Awaited<Input>> {
    return { steps: this.steps }
  }
}

export type DefineFlowOptions<Schema extends StandardSchemaV1, Output> = {
  schema: Schema
  flow: (builder: FlowBuilder<StandardSchemaV1.InferOutput<Schema>>) => FlowBuilderResult<Output>
  workerOptions?: WorkerOptions<StandardSchemaV1.InferOutput<Schema>, Output>
}

export type Flow<Schema extends StandardSchemaV1, Output> = DefineFlowOptions<Schema, Output> & {
  [flowSymbol]: {
    steps: FlowStep<any, any>[]
    addToQueue: (
      flowName: string,
      flowProducer: FlowProducer,
      payload: StandardSchemaV1.InferInput<Schema>,
    ) => Promise<JobNode>
    addToQueueBulk: (
      flowName: string,
      flowProducer: FlowProducer,
      payloads: StandardSchemaV1.InferInput<Schema>[],
    ) => Promise<JobNode[]>
  }
}

export type JobDefinitionsObject = {
  [key: string]: Job<any, any> | Flow<any, any> | JobDefinitionsObject
}

export type QueueJobProxyAccessor<S extends StandardSchemaV1, Output> = {
  queue: (
    payload: StandardSchemaV1.InferInput<S>,
  ) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, Output, string>>
  queueBulk: (
    payloads: StandardSchemaV1.InferInput<S>[],
  ) => Promise<BullJob<StandardSchemaV1.InferOutput<S>, Output, string>[]>
}
export type QueueFlowProxyAccessor<S extends StandardSchemaV1> = {
  queue: (payload: StandardSchemaV1.InferInput<S>) => Promise<JobNode>
  queueBulk: (payloads: StandardSchemaV1.InferInput<S>[]) => Promise<JobNode[]>
}
export type QueueJobProxy<J extends JobDefinitionsObject> = {
  [K in keyof J]: J[K] extends Job<infer S, infer O>
    ? QueueJobProxyAccessor<S, O>
    : J[K] extends Flow<infer S, any>
      ? QueueFlowProxyAccessor<S>
      : J[K] extends JobDefinitionsObject
        ? QueueJobProxy<J[K]>
        : never
}

export type WorkerOptions<Input, Output> = Omit<Partial<BullWorkerOptions>, 'autorun' | 'name'> & {
  hooks?: Partial<BullWorkerListener<Input, Output, string>>
}
