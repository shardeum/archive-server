import { FastifyInstance, FastifyRequest } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { getTxDigestsForACycleRange } from './txDigestFunctions'

type GetTxDigestsRequest = FastifyRequest<{
  Querystring: {
    cycleStart: number
    cycleEnd: number
  }
}>

/* To-Do: Add LRU cache for the tx-digests */
export function registerRoutes(server: FastifyInstance<Server, IncomingMessage, ServerResponse>): void {
  server.get('/api/tx-digests', async (_request: GetTxDigestsRequest, reply) => {
    const cycleStart = Number(_request.query?.cycleStart)
    const cycleEnd = Number(_request.query?.cycleEnd)

    if (
      isNaN(cycleStart) ||
      isNaN(cycleEnd) ||
      cycleEnd <= cycleStart ||
      cycleStart < 0 ||
      cycleEnd < 0 ||
      cycleEnd - cycleStart > 10000
    ) {
      reply.status(400).send({
        error: 'Invalid query parameters. They must be positive numbers with cycleEnd > cycleStart',
      })
      return
    }

    console.log(`Fetching tx digests for cycles: ${cycleStart} to ${cycleEnd}`)
    const txDigests = await getTxDigestsForACycleRange(cycleStart, cycleEnd)
    console.log('Fetched Tx digests', txDigests)
    reply.send(txDigests)
  })
}
