# @supaproxy/bullmq
BullMQ queue adapter for SupaProxy. Implements QueueService with Redis-backed job queues, lifecycle scheduling, and worker management.

## Install
```sh
pnpm add @supaproxy/bullmq
```

## Usage
```ts
import { createBullMqQueue } from '@supaproxy/bullmq'
const queueService = createBullMqQueue(QUEUE_HOST, QUEUE_PORT)
```
