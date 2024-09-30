import { FastifyInstance, FastifyRequest } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { getLatestTxDigests, getTxDigestsForACycleRange } from './txDigestFunctions'
import { config } from '../Config'
import * as Utils from '../Utils'

type GetTxDigestsRequest = FastifyRequest<{
  Querystring: {
    cycleStart: number
    cycleEnd: number
  }
}>
type GetLatestTxDigestsRequest = FastifyRequest<{
  Params: { count: string }
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
  
  server.get('/api/tx-digest-hash/:count', async (_request: GetLatestTxDigestsRequest, reply) => {
    const err = Utils.validateTypes(_request.params, { count: 's' })
    if (err) {
      reply.send({ success: false, error: err })
      return
    }
    let count: number = parseInt(_request.params.count)
    if (count <= 0 || Number.isNaN(count)) {
      reply.send({ success: false, error: `Invalid count` })
      return
    }
    if (count > config.REQUEST_LIMIT.MAX_DIGESTS_PER_REQUEST) {
      count = config.REQUEST_LIMIT.MAX_DIGESTS_PER_REQUEST
    }
    
    console.log(`Fetching latest ${count} tx digests`)
    const txDigests = await getLatestTxDigests(count)
    console.log('Fetched Tx digests', txDigests)
    reply.send(txDigests)
  })
}
