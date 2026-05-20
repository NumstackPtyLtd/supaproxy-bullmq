import type { QueueService } from '@supaproxy/core/ports/queue'
import { BullMqService } from './BullMqService.js'

export function createBullMqQueue(redisHost: string, redisPort: number): QueueService {
  return new BullMqService(redisHost, redisPort)
}

export { BullMqService } from './BullMqService.js'
