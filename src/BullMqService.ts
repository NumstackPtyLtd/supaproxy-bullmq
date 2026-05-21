import { Queue, Worker } from 'bullmq'
import type { QueueService, QueueJobCounts, FailedJob, LifecycleHandler } from '@supaproxy/core/ports/queue'
import { QUEUE_LIFECYCLE, QUEUE_COLD_MESSAGES, QUEUE_CONVERSATION_STATS, LIFECYCLE_SCAN_INTERVAL_MS, COLD_MESSAGE_CONCURRENCY, STATS_WORKER_CONCURRENCY } from '@supaproxy/core/defaults'
import pino from 'pino'

const log = pino({ name: 'bullmq-service' })

export class BullMqService implements QueueService {
  private readonly lifecycleQueue: Queue
  private readonly coldMessageQueue: Queue
  private readonly statsQueue: Queue
  private readonly queues: Record<string, Queue>
  private lifecycleWorker: Worker | null = null
  private coldMessageWorker: Worker | null = null
  private statsWorker: Worker | null = null

  constructor(
    private readonly redisHost: string,
    private readonly redisPort: number,
  ) {
    const connection = { host: this.redisHost, port: this.redisPort }
    this.lifecycleQueue = new Queue(QUEUE_LIFECYCLE, { connection })
    this.coldMessageQueue = new Queue(QUEUE_COLD_MESSAGES, { connection })
    this.statsQueue = new Queue(QUEUE_CONVERSATION_STATS, { connection })
    this.queues = {
      [QUEUE_LIFECYCLE]: this.lifecycleQueue,
      [QUEUE_COLD_MESSAGES]: this.coldMessageQueue,
      [QUEUE_CONVERSATION_STATS]: this.statsQueue,
    }
  }

  async addColdMessage(data: { conversationId: string; consumerType: string; channel: string; externalThreadId: string }): Promise<void> {
    await this.coldMessageQueue.add('send-cold-message', data)
  }

  async addStatsJob(conversationId: string): Promise<void> {
    await this.statsQueue.add('generate-stats', { conversationId })
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

  async startWorkers(handler: LifecycleHandler): Promise<void> {
    const connection = { host: this.redisHost, port: this.redisPort }

    this.lifecycleWorker = new Worker(QUEUE_LIFECYCLE, async () => {
      try {
        await handler.runLifecycleScan()
      } catch (err) {
        log.error({ error: (err as Error).message }, 'Lifecycle scan failed')
      }
    }, { connection })

    this.coldMessageWorker = new Worker(QUEUE_COLD_MESSAGES, async (job) => {
      await handler.sendColdMessage(job.data)
    }, { connection, concurrency: COLD_MESSAGE_CONCURRENCY })

    this.statsWorker = new Worker(QUEUE_CONVERSATION_STATS, async (job) => {
      await handler.generateStats(job.data.conversationId)
    }, { connection, concurrency: STATS_WORKER_CONCURRENCY })

    this.lifecycleWorker.on('failed', (job, err) => log.error({ job: job?.id, error: err.message }, 'Lifecycle job failed'))
    this.coldMessageWorker.on('failed', (job, err) => log.error({ job: job?.id, error: err.message }, 'Cold message job failed'))
    this.statsWorker.on('failed', (job, err) => log.error({ job: job?.id, error: err.message }, 'Stats job failed'))

    await this.lifecycleQueue.upsertJobScheduler('scan', { every: LIFECYCLE_SCAN_INTERVAL_MS }, { name: 'lifecycle-scan' })
    log.info(`BullMQ workers started (scan every ${LIFECYCLE_SCAN_INTERVAL_MS / 1000}s, ${COLD_MESSAGE_CONCURRENCY} cold message workers, ${STATS_WORKER_CONCURRENCY} stats workers)`)
  }

  async stopWorkers(): Promise<void> {
    await this.lifecycleWorker?.close()
    await this.coldMessageWorker?.close()
    await this.statsWorker?.close()
  }
}
