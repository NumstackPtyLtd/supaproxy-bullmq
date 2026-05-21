import type { QueueService } from '@supaproxy/core/ports/queue'
import { BullMqService } from './BullMqService.js'

export function createBullMqQueue(redisHost: string, redisPort: number, queueNames: string[] = []): QueueService {
  return new BullMqService(redisHost, redisPort, queueNames)
}

export { BullMqService } from './BullMqService.js'
