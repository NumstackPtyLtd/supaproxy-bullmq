import { describe, it, expect, vi, beforeEach } from 'vitest'

const mockAdd = vi.fn()
const mockGetJobCounts = vi.fn()
const mockGetFailed = vi.fn()
const mockDrain = vi.fn()
const mockUpsertJobScheduler = vi.fn()
const mockWorkerClose = vi.fn()
const mockWorkerOn = vi.fn()

vi.mock('bullmq', () => ({
  Queue: vi.fn().mockImplementation(() => ({
    add: mockAdd,
    getJobCounts: mockGetJobCounts,
    getFailed: mockGetFailed,
    drain: mockDrain,
    upsertJobScheduler: mockUpsertJobScheduler,
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
    service = new BullMqService('localhost', 6379)
  })

  describe('constructor', () => {
    it('creates 3 queues (lifecycle, cold-messages, conversation-stats)', () => {
      expect(Queue).toHaveBeenCalledTimes(3)
      expect(Queue).toHaveBeenCalledWith('lifecycle', { connection: { host: 'localhost', port: 6379 } })
      expect(Queue).toHaveBeenCalledWith('cold-messages', { connection: { host: 'localhost', port: 6379 } })
      expect(Queue).toHaveBeenCalledWith('conversation-stats', { connection: { host: 'localhost', port: 6379 } })
    })
  })

  describe('addColdMessage', () => {
    it('adds a job to the cold message queue', async () => {
      const data = {
        conversationId: 'conv-1',
        consumerType: 'slack',
        channel: 'C123',
        externalThreadId: 'T456',
      }
      await service.addColdMessage(data)
      expect(mockAdd).toHaveBeenCalledWith('send-cold-message', data)
    })
  })

  describe('addStatsJob', () => {
    it('adds a job to the stats queue', async () => {
      await service.addStatsJob('conv-42')
      expect(mockAdd).toHaveBeenCalledWith('generate-stats', { conversationId: 'conv-42' })
    })
  })

  describe('getJobCounts', () => {
    it('returns counts from the queue', async () => {
      mockGetJobCounts.mockResolvedValue({
        waiting: 5,
        active: 2,
        completed: 10,
        failed: 1,
        delayed: 3,
      })
      const counts = await service.getJobCounts('lifecycle')
      expect(counts).toEqual({ waiting: 5, active: 2, completed: 10, failed: 1, delayed: 3 })
    })

    it('returns zeroes for unknown queue', async () => {
      const counts = await service.getJobCounts('unknown-queue')
      expect(counts).toEqual({ waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0 })
    })
  })

  describe('getFailedJobs', () => {
    it('returns mapped failed jobs', async () => {
      mockGetFailed.mockResolvedValue([
        {
          id: 'job-1',
          data: { conversationId: 'c1' },
          failedReason: 'timeout',
          timestamp: 1000,
          attemptsMade: 3,
        },
        {
          id: 'job-2',
          data: { conversationId: 'c2' },
          failedReason: 'error',
          timestamp: 2000,
          attemptsMade: 1,
        },
      ])

      const result = await service.getFailedJobs('cold-messages', 10)
      expect(mockGetFailed).toHaveBeenCalledWith(0, 10)
      expect(result).toEqual([
        { id: 'job-1', data: { conversationId: 'c1' }, failedReason: 'timeout', timestamp: 1000, attemptsMade: 3 },
        { id: 'job-2', data: { conversationId: 'c2' }, failedReason: 'error', timestamp: 2000, attemptsMade: 1 },
      ])
    })

    it('returns empty array for unknown queue', async () => {
      const result = await service.getFailedJobs('nope', 10)
      expect(result).toEqual([])
    })
  })

  describe('retryAllFailed', () => {
    it('retries jobs and returns count', async () => {
      const mockRetry = vi.fn()
      mockGetFailed.mockResolvedValue([
        { retry: mockRetry },
        { retry: mockRetry },
        { retry: mockRetry },
      ])

      const count = await service.retryAllFailed('lifecycle')
      expect(count).toBe(3)
      expect(mockRetry).toHaveBeenCalledTimes(3)
    })

    it('returns 0 for unknown queue', async () => {
      const count = await service.retryAllFailed('nope')
      expect(count).toBe(0)
    })
  })

  describe('drainQueue', () => {
    it('drains the queue', async () => {
      await service.drainQueue('lifecycle')
      expect(mockDrain).toHaveBeenCalled()
    })

    it('does nothing for unknown queue', async () => {
      await service.drainQueue('nope')
      expect(mockDrain).not.toHaveBeenCalled()
    })
  })

  describe('listQueueNames', () => {
    it('returns all queue names', () => {
      const names = service.listQueueNames()
      expect(names).toEqual(['lifecycle', 'cold-messages', 'conversation-stats'])
    })
  })

  describe('queueExists', () => {
    it('returns true for existing queue', () => {
      expect(service.queueExists('lifecycle')).toBe(true)
      expect(service.queueExists('cold-messages')).toBe(true)
      expect(service.queueExists('conversation-stats')).toBe(true)
    })

    it('returns false for non-existing queue', () => {
      expect(service.queueExists('nope')).toBe(false)
    })
  })

  describe('startWorkers', () => {
    it('creates workers and scheduler', async () => {
      const handler = {
        runLifecycleScan: vi.fn(),
        sendColdMessage: vi.fn(),
        generateStats: vi.fn(),
      }

      await service.startWorkers(handler)

      expect(Worker).toHaveBeenCalledTimes(3)
      expect(Worker).toHaveBeenCalledWith('lifecycle', expect.any(Function), { connection: { host: 'localhost', port: 6379 } })
      expect(Worker).toHaveBeenCalledWith('cold-messages', expect.any(Function), { connection: { host: 'localhost', port: 6379 }, concurrency: 3 })
      expect(Worker).toHaveBeenCalledWith('conversation-stats', expect.any(Function), { connection: { host: 'localhost', port: 6379 }, concurrency: 2 })
      expect(mockWorkerOn).toHaveBeenCalledTimes(3)
      expect(mockUpsertJobScheduler).toHaveBeenCalledWith('scan', { every: 30_000 }, { name: 'lifecycle-scan' })
    })
  })

  describe('stopWorkers', () => {
    it('closes workers after they have been started', async () => {
      const handler = {
        runLifecycleScan: vi.fn(),
        sendColdMessage: vi.fn(),
        generateStats: vi.fn(),
      }

      await service.startWorkers(handler)
      vi.clearAllMocks()

      await service.stopWorkers()
      expect(mockWorkerClose).toHaveBeenCalledTimes(3)
    })

    it('does not throw when workers were never started', async () => {
      await expect(service.stopWorkers()).resolves.toBeUndefined()
    })
  })
})
