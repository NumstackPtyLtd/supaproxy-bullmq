import { describe, it, expect, vi, beforeEach } from 'vitest'

const mockAdd = vi.fn()
const mockGetJobCounts = vi.fn()
const mockGetFailed = vi.fn()
const mockDrain = vi.fn()
const mockUpsertJobScheduler = vi.fn()
const mockRemoveJobScheduler = vi.fn()
const mockWorkerClose = vi.fn()
const mockWorkerOn = vi.fn()

vi.mock('bullmq', () => ({
  Queue: vi.fn().mockImplementation(() => ({
    add: mockAdd,
    getJobCounts: mockGetJobCounts,
    getFailed: mockGetFailed,
    drain: mockDrain,
    upsertJobScheduler: mockUpsertJobScheduler,
    removeJobScheduler: mockRemoveJobScheduler,
  })),
  Worker: vi.fn().mockImplementation(() => ({
    close: mockWorkerClose,
    on: mockWorkerOn,
  })),
}))

vi.mock('pino', () => ({
  default: () => ({
    info: vi.fn(),
    error: vi.fn(),
  }),
}))

import { BullMqService } from './BullMqService.js'
import { Queue, Worker } from 'bullmq'

describe('BullMqService', () => {
  let service: BullMqService

  beforeEach(() => {
    vi.clearAllMocks()
    service = new BullMqService('localhost', 6379, ['queue-a', 'queue-b'])
  })

  describe('constructor', () => {
    it('creates queues from provided names', () => {
      expect(Queue).toHaveBeenCalledTimes(2)
      expect(Queue).toHaveBeenCalledWith('queue-a', { connection: { host: 'localhost', port: 6379 } })
      expect(Queue).toHaveBeenCalledWith('queue-b', { connection: { host: 'localhost', port: 6379 } })
    })
  })

  describe('addJob', () => {
    it('adds a job to the specified queue', async () => {
      await service.addJob('queue-a', 'process', { id: 'item-1' })
      expect(mockAdd).toHaveBeenCalledWith('process', { id: 'item-1' }, undefined)
    })

    it('passes job options when provided', async () => {
      const options = { removeOnComplete: 50, removeOnFail: 10 }
      await service.addJob('queue-a', 'process', { id: 'item-1' }, options)
      expect(mockAdd).toHaveBeenCalledWith('process', { id: 'item-1' }, options)
    })

    it('creates queue on demand if not pre-registered', async () => {
      await service.addJob('queue-c', 'process', { id: 'item-1' })
      expect(Queue).toHaveBeenCalledTimes(3)
      expect(mockAdd).toHaveBeenCalledWith('process', { id: 'item-1' }, undefined)
    })
  })

  describe('scheduleJob', () => {
    it('creates a repeatable job scheduler', async () => {
      await service.scheduleJob('queue-a', 'sched-1', '0 2 * * *', 'scheduled-task', { key: 'val' })
      expect(mockUpsertJobScheduler).toHaveBeenCalledWith(
        'sched-1',
        { pattern: '0 2 * * *' },
        { name: 'scheduled-task', data: { key: 'val' } },
      )
    })
  })

  describe('cancelSchedule', () => {
    it('removes the job scheduler', async () => {
      await service.cancelSchedule('queue-a', 'sched-1')
      expect(mockRemoveJobScheduler).toHaveBeenCalledWith('sched-1')
    })

    it('does nothing for unknown queue', async () => {
      await service.cancelSchedule('unknown', 'sched-1')
      expect(mockRemoveJobScheduler).not.toHaveBeenCalled()
    })
  })

  describe('getJobCounts', () => {
    it('returns counts from the queue', async () => {
      mockGetJobCounts.mockResolvedValue({ waiting: 5, active: 2, completed: 10, failed: 1, delayed: 3 })
      const counts = await service.getJobCounts('queue-a')
      expect(counts).toEqual({ waiting: 5, active: 2, completed: 10, failed: 1, delayed: 3 })
    })

    it('returns zeroes for unknown queue', async () => {
      const counts = await service.getJobCounts('unknown')
      expect(counts).toEqual({ waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 })
    })
  })

  describe('getFailedJobs', () => {
    it('returns mapped failed jobs', async () => {
      mockGetFailed.mockResolvedValue([
        { id: 'job-1', data: { key: 'a' }, failedReason: 'timeout', timestamp: 1000, attemptsMade: 3 },
        { id: 'job-2', data: { key: 'b' }, failedReason: 'error', timestamp: 2000, attemptsMade: 1 },
      ])
      const result = await service.getFailedJobs('queue-a', 10)
      expect(result).toHaveLength(2)
      expect(result[0]).toEqual({ id: 'job-1', data: { key: 'a' }, failedReason: 'timeout', timestamp: 1000, attemptsMade: 3 })
    })

    it('returns empty array for unknown queue', async () => {
      expect(await service.getFailedJobs('unknown', 10)).toEqual([])
    })
  })

  describe('retryAllFailed', () => {
    it('retries jobs and returns count', async () => {
      const mockRetry = vi.fn()
      mockGetFailed.mockResolvedValue([{ retry: mockRetry }, { retry: mockRetry }])
      const count = await service.retryAllFailed('queue-a')
      expect(count).toBe(2)
      expect(mockRetry).toHaveBeenCalledTimes(2)
    })

    it('returns 0 for unknown queue', async () => {
      expect(await service.retryAllFailed('unknown')).toBe(0)
    })
  })

  describe('drainQueue', () => {
    it('drains the queue', async () => {
      await service.drainQueue('queue-a')
      expect(mockDrain).toHaveBeenCalled()
    })

    it('does nothing for unknown queue', async () => {
      await service.drainQueue('unknown')
      expect(mockDrain).not.toHaveBeenCalled()
    })
  })

  describe('listQueueNames', () => {
    it('returns registered queue names', () => {
      expect(service.listQueueNames()).toEqual(['queue-a', 'queue-b'])
    })
  })

  describe('queueExists', () => {
    it('returns true for registered queue', () => {
      expect(service.queueExists('queue-a')).toBe(true)
    })

    it('returns false for unregistered queue', () => {
      expect(service.queueExists('unknown')).toBe(false)
    })
  })

  describe('startWorkers', () => {
    it('creates workers for configs with handlers', async () => {
      await service.startWorkers([
        { name: 'queue-a', handler: vi.fn(), concurrency: 2 },
        { name: 'queue-b', handler: vi.fn() },
      ])

      expect(Worker).toHaveBeenCalledTimes(2)
      expect(Worker).toHaveBeenCalledWith('queue-a', expect.any(Function), { connection: { host: 'localhost', port: 6379 }, concurrency: 2 })
      expect(Worker).toHaveBeenCalledWith('queue-b', expect.any(Function), { connection: { host: 'localhost', port: 6379 }, concurrency: 1 })
    })

    it('sets up schedulers for configs with scheduler property', async () => {
      await service.startWorkers([
        { name: 'queue-a', scheduler: { every: 30000, jobName: 'scan' } },
      ])
      expect(mockUpsertJobScheduler).toHaveBeenCalledWith('queue-a', { every: 30000 }, { name: 'scan' })
    })

    it('skips worker creation for configs without handler', async () => {
      await service.startWorkers([
        { name: 'queue-a' },
      ])
      expect(Worker).not.toHaveBeenCalled()
    })
  })

  describe('stopWorkers', () => {
    it('closes all started workers', async () => {
      await service.startWorkers([
        { name: 'queue-a', handler: vi.fn() },
        { name: 'queue-b', handler: vi.fn() },
      ])
      vi.clearAllMocks()

      await service.stopWorkers()
      expect(mockWorkerClose).toHaveBeenCalledTimes(2)
    })

    it('does not throw when no workers started', async () => {
      await expect(service.stopWorkers()).resolves.toBeUndefined()
    })
  })
})
