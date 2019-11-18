import { join } from 'path'
import * as fastify from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { overrideDefaultConfig, config } from './Config'
import { setCryptoHashKey, crypto } from './Crypto'
import { state, initStateFromConfig } from './State'
import * as NodeList from './NodeList'
import * as P2P from './P2P'
import * as Storage from './Storage'

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
const env = process.env
const args = process.argv
overrideDefaultConfig(file, env, args)

// Set crypto hash key from config
setCryptoHashKey(config.ARCHIVER_HASH_KEY)

// If no keypair provided, generate one
if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
  const keypair = crypto.generateKeypair()
  config.ARCHIVER_PUBLIC_KEY = keypair.publicKey
  config.ARCHIVER_SECRET_KEY = keypair.secretKey
}

// Initialize state from config
initStateFromConfig(config)

// Initialize storage
Storage.initStorage(state.dbFile)

if (state.isFirst === false) {
  // [TODO] If your not the first archiver node, get a nodelist from the others
  // [TODO] Send a join request a consensus node from the nodelist
  // [TODO] After you've joined, select a consensus node to be your cycleSender
  startServer()
} else {
  startServer()
}

function processNewCycle(cycle: Storage.Cycle) {
  // Update NodeList from cycle info
  // Get new cycleSender if current cycleSender leaves network
  Storage.storeCycle(cycle)
}

function startServer() {
  // Start REST server and register endpoints
  const server: fastify.FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
    logger: true,
  })

  server.get('/nodeinfo', (_request, reply) => {
    reply.send({
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      time: Date.now(),
    })
  })

  server.post('/nodelist', (request, reply) => {
    interface Response {
      nodeList: NodeList.ConsensusNodeInfo[]
      joinRequest?: P2P.ArchiverJoinRequest
    }

    const response: Response = {
      nodeList: NodeList.getList(),
    }

    // Network genesis
    if (state.isFirst && NodeList.isEmpty()) {
      const ip = request.req.socket.remoteAddress
      const port = request.body.nodeInfo.externalPort
      if (ip && port) {
        const firstNode = {
          ip,
          port,
        }
        // Add first node to NodeList
        NodeList.addNode(firstNode)
        // Set first node as cycleSender
        state.cycleSender = firstNode
        // Add joinRequest to response
        response.joinRequest = P2P.createJoinRequest()
      }
    }

    crypto.signObj(response, state.nodeInfo.secretKey, state.nodeInfo.publicKey)
    reply.send(response)
  })

  server.get('/nodelist', (_request, reply) => {
    const response = {
      nodeList: NodeList.getList(),
    }
    crypto.signObj(response, state.nodeInfo.secretKey, state.nodeInfo.publicKey)
    reply.send(response)
  })

  server.post('/newcycle', (request, reply) => {
    // [TODO] verify that it came from cycleSender
    const cycle = request.body
    processNewCycle(cycle)
    reply.send()
  })

  server.get('/exit', (_request, reply) => {
    reply.send('Shutting down...')
    process.exit()
  })

  server.listen(config.ARCHIVER_PORT, (err, address) => {
    if (err) {
      server.log.error(err)
      process.exit(1)
    }
  })
}
