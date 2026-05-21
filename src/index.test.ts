import { describe, it, expect, vi } from 'vitest'

vi.mock('bullmq', () => ({
  Queue: vi.fn().mockImplementation(() => ({
    add: vi.fn(),
    getJobCounts: vi.fn(),
    getFailed: vi.fn(),
    drain: vi.fn(),
    upsertJobScheduler: vi.fn(),
    removeJobScheduler: vi.fn(),
  })),
  Worker: vi.fn().mockImplementation(() => ({
    close: vi.fn(),
    on: vi.fn(),
  })),
}))

vi.mock('pino', () => ({
  default: () => ({
    info: vi.fn(),
    error: vi.fn(),
  }),
}))

import { createBullMqQueue, BullMqService } from './index.js'

describe('createBullMqQueue', () => {
  it('returns a QueueService instance', () => {
    const service = createBullMqQueue('localhost', 6379)
    expect(service).toBeInstanceOf(BullMqService)
  })

  it('has all QueueService methods', () => {
    const service = createBullMqQueue('127.0.0.1', 6380)
    expect(typeof service.addJob).toBe('function')
    expect(typeof service.scheduleJob).toBe('function')
    expect(typeof service.cancelSchedule).toBe('function')
    expect(typeof service.getJobCounts).toBe('function')
    expect(typeof service.getFailedJobs).toBe('function')
    expect(typeof service.retryAllFailed).toBe('function')
    expect(typeof service.drainQueue).toBe('function')
    expect(typeof service.listQueueNames).toBe('function')
    expect(typeof service.queueExists).toBe('function')
    expect(typeof service.startWorkers).toBe('function')
    expect(typeof service.stopWorkers).toBe('function')
  })

  it('accepts initial queue names', () => {
    const service = createBullMqQueue('localhost', 6379, ['queue-x', 'queue-y'])
    expect(service.listQueueNames()).toEqual(['queue-x', 'queue-y'])
  })
})
