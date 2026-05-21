import { Queue, Worker } from 'bullmq'
import type { QueueService, QueueJobCounts, FailedJob, JobOptions, QueueConfig } from '@supaproxy/core/ports/queue'
import pino from 'pino'

const log = pino({ name: 'bullmq-service' })

/**
 * Generic BullMQ adapter. Knows nothing about the product.
 * Queue names, handlers, concurrency, and job options are all
 * passed in via configuration at startup.
 */
export class BullMqService implements QueueService {
  private readonly queues: Map<string, Queue> = new Map()
  private readonly workers: Worker[] = []

  constructor(
    private readonly redisHost: string,
    private readonly redisPort: number,
    queueNames: string[] = [],
  ) {
    const connection = { host: this.redisHost, port: this.redisPort }
    for (const name of queueNames) {
      this.queues.set(name, new Queue(name, { connection }))
    }
  }

  private getQueue(name: string): Queue | undefined {
    return this.queues.get(name)
  }

  private ensureQueue(name: string): Queue {
    let queue = this.queues.get(name)
    if (!queue) {
      const connection = { host: this.redisHost, port: this.redisPort }
      queue = new Queue(name, { connection })
      this.queues.set(name, queue)
    }
    return queue
  }

  async addJob(queueName: string, jobName: string, data: Record<string, unknown>, options?: JobOptions): Promise<void> {
    const queue = this.ensureQueue(queueName)
    await queue.add(jobName, data, options)
  }

  async scheduleJob(queueName: string, schedulerId: string, cron: string, jobName: string, data: Record<string, unknown>): Promise<void> {
    const queue = this.ensureQueue(queueName)
    await queue.upsertJobScheduler(schedulerId, { pattern: cron }, { name: jobName, data })
    log.info({ queueName, schedulerId, cron }, 'Job scheduled')
  }

  async cancelSchedule(queueName: string, schedulerId: string): Promise<void> {
    const queue = this.getQueue(queueName)
    if (queue) {
      await queue.removeJobScheduler(schedulerId)
      log.info({ queueName, schedulerId }, 'Schedule cancelled')
    }
  }

  async getJobCounts(queueName: string): Promise<QueueJobCounts> {
    const queue = this.getQueue(queueName)
    if (!queue) return { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 }
    const counts = await queue.getJobCounts()
    return {
      waiting: counts.waiting ?? 0,
      active: counts.active ?? 0,
      completed: counts.completed ?? 0,
      failed: counts.failed ?? 0,
      delayed: counts.delayed ?? 0,
    }
  }

  async getFailedJobs(queueName: string, limit: number): Promise<FailedJob[]> {
    const queue = this.getQueue(queueName)
    if (!queue) return []
    const failed = await queue.getFailed(0, limit)
    return failed.map(j => ({
      id: j.id,
      data: j.data,
      failedReason: j.failedReason,
      timestamp: j.timestamp,
      attemptsMade: j.attemptsMade,
    }))
  }

  async retryAllFailed(queueName: string): Promise<number> {
    const queue = this.getQueue(queueName)
    if (!queue) return 0
    const failed = await queue.getFailed(0, 100)
    let retried = 0
    for (const job of failed) {
      await job.retry()
      retried++
    }
    return retried
  }

  async drainQueue(queueName: string): Promise<void> {
    const queue = this.getQueue(queueName)
    if (queue) await queue.drain()
  }

  listQueueNames(): string[] {
    return Array.from(this.queues.keys())
  }

  queueExists(name: string): boolean {
    return this.queues.has(name)
  }

  async startWorkers(configs: QueueConfig[]): Promise<void> {
    const connection = { host: this.redisHost, port: this.redisPort }

    for (const config of configs) {
      this.ensureQueue(config.name)

      if (config.handler) {
        const worker = new Worker(config.name, async (job) => {
          await config.handler!(job.data)
        }, { connection, concurrency: config.concurrency ?? 1 })
        worker.on('failed', (job, err) => log.error({ queue: config.name, job: job?.id, error: err.message }, 'Job failed'))
        this.workers.push(worker)
      }

      if (config.scheduler) {
        const queue = this.ensureQueue(config.name)
        if (config.scheduler.every) {
          await queue.upsertJobScheduler(config.name, { every: config.scheduler.every }, { name: config.scheduler.jobName })
        } else if (config.scheduler.pattern) {
          await queue.upsertJobScheduler(config.name, { pattern: config.scheduler.pattern }, { name: config.scheduler.jobName })
        }
      }
    }

    const workerCount = this.workers.length
    log.info({ workerCount, queues: configs.map(c => c.name) }, 'BullMQ workers started')
  }

  async stopWorkers(): Promise<void> {
    for (const worker of this.workers) {
      await worker.close()
    }
    this.workers.length = 0
  }
}
