import { Queue, Worker } from 'bullmq'
import type { QueueService, QueueJobCounts, FailedJob, QueueConfig, JobOptions } from '@supaproxy/core/ports/queue'
import pino from 'pino'

const log = pino({ name: 'bullmq-service' })

export class BullMqService implements QueueService {
  private readonly queues: Record<string, Queue> = {}
  private readonly workers: Worker[] = []
  private readonly redisHost: string
  private readonly redisPort: number

  constructor(redisHost: string, redisPort: number) {
    this.redisHost = redisHost
    this.redisPort = redisPort
  }

  async addJob(queueName: string, jobName: string, data: Record<string, unknown>, options?: JobOptions): Promise<void> {
    const queue = this.getOrCreateQueue(queueName)
    await queue.add(jobName, data, {
      removeOnComplete: options?.removeOnComplete ?? 100,
      removeOnFail: options?.removeOnFail ?? 500,
    })
  }

  async scheduleJob(queueName: string, schedulerId: string, cron: string, jobName: string, data: Record<string, unknown>): Promise<void> {
    const queue = this.getOrCreateQueue(queueName)
    await queue.upsertJobScheduler(schedulerId, { pattern: cron }, { name: jobName, data })
  }

  async cancelSchedule(queueName: string, schedulerId: string): Promise<void> {
    const queue = this.queues[queueName]
    if (queue) await queue.removeJobScheduler(schedulerId)
  }

  async getJobCounts(queueName: string): Promise<QueueJobCounts> {
    const queue = this.queues[queueName]
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
    const queue = this.queues[queueName]
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
    const queue = this.queues[queueName]
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
    const queue = this.queues[queueName]
    if (queue) await queue.drain()
  }

  listQueueNames(): string[] {
    return Object.keys(this.queues)
  }

  queueExists(name: string): boolean {
    return name in this.queues
  }

  async startWorkers(configs: QueueConfig[]): Promise<void> {
    const connection = { host: this.redisHost, port: this.redisPort }

    for (const config of configs) {
      this.getOrCreateQueue(config.name)

      if (config.handler) {
        const workerOpts = config.concurrency
          ? { connection, concurrency: config.concurrency }
          : { connection }
        const worker = new Worker(config.name, async (job) => {
          await config.handler!(job.data)
        }, workerOpts)

        worker.on('failed', (job, err) => {
          log.error({ queue: config.name, job: job?.id, error: err.message }, 'Job failed')
        })

        this.workers.push(worker)
      }

      if (config.scheduler) {
        const queue = this.queues[config.name]
        if (config.scheduler.every) {
          await queue.upsertJobScheduler('scan', { every: config.scheduler.every }, { name: config.scheduler.jobName })
        } else if (config.scheduler.pattern) {
          await queue.upsertJobScheduler('cron', { pattern: config.scheduler.pattern }, { name: config.scheduler.jobName })
        }
      }
    }

    const names = configs.map(c => c.name).join(', ')
    log.info({ workerCount: this.workers.length, queues: configs.map(c => c.name) }, 'BullMQ workers started')
  }

  async stopWorkers(): Promise<void> {
    for (const worker of this.workers) {
      await worker.close()
    }
    this.workers.length = 0
  }

  private getOrCreateQueue(name: string): Queue {
    if (!this.queues[name]) {
      this.queues[name] = new Queue(name, { connection: { host: this.redisHost, port: this.redisPort } })
    }
    return this.queues[name]
  }
}
