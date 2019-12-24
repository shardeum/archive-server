import { join } from 'path'
import * as fastify from 'fastify'
import * as fastifyCors from 'fastify-cors'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { overrideDefaultConfig, config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as NodeList from './NodeList'
import * as P2P from './P2P'
import * as Storage from './Storage'
import * as Data from './Data/Data'

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
const env = process.env
const args = process.argv
overrideDefaultConfig(file, env, args)

// Set crypto hash key from config
Crypto.setCryptoHashKey(config.ARCHIVER_HASH_KEY)

// If no keypair provided, generate one
if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
  const keypair = Crypto.core.generateKeypair()
  config.ARCHIVER_PUBLIC_KEY = keypair.publicKey
  config.ARCHIVER_SECRET_KEY = keypair.secretKey
}

// Initialize state from config
State.initFromConfig(config)

// Initialize storage
Storage.initStorage(State.dbFile)

if (State.isFirst === false) {
  /**
   * ENTRY POINT: Existing Shardus network
   */
  // [TODO] If your not the first archiver node, get a nodelist from the others
  // [TODO] Send a join request to a consensus node from the nodelist
  // [TODO] After you've joined, select a consensus node to be your dataSender
  startServer()
} else {
  startServer()
}

function startServer() {
  // Start REST server and register endpoints
  const server: fastify.FastifyInstance<
    Server,
    IncomingMessage,
    ServerResponse
  > = fastify({
    logger: true,
  })

  server.register(fastifyCors)

  server.get('/nodeinfo', (_request, reply) => {
    reply.send({
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      time: Date.now(),
    })
  })

  /**
   * ENTRY POINT: New Shardus network
   *
   * Consensus node zero (CZ) posts IP and port to archiver node zero (AZ).
   *
   * AZ adds CZ to nodelist, sets CZ as dataSender, and responds with
   * nodelist + archiver join request
   *
   * CZ adds AZ's join reqeuest to cycle zero and sets AZ as cycleRecipient
   */
  server.post('/nodelist', (request, reply) => {
    interface Response {
      nodeList: NodeList.ConsensusNodeInfo[]
      joinRequest?: P2P.ArchiverJoinRequest
    }

    const response: Response = {
      nodeList: NodeList.getList(),
    }

    // Network genesis
    if (State.isFirst && NodeList.isEmpty()) {
      const ip = request.req.socket.remoteAddress
      const port = request.body.nodeInfo.externalPort
      const publicKey = request.body.nodeInfo.publicKey
      if (ip && port) {
        const firstNode = {
          ip,
          port,
          publicKey,
        }
        // Add first node to NodeList
        NodeList.addNodes(NodeList.Statuses.SYNCING, firstNode)
        // Set first node as dataSender
        Data.addDataSenders({
          nodeInfo: firstNode,
          type: Data.DataTypes.CYCLE,
        })
        // Add joinRequest to response
        response.joinRequest = P2P.createJoinRequest()
      }
    }

    Crypto.sign(response)
    reply.send(response)
  })

  server.get('/nodelist', (_request, reply) => {
    const response = {
      nodeList: NodeList.getList(),
    }
    Crypto.sign(response)
    reply.send(response)
  })

  server.get('/exit', (_request, reply) => {
    reply.send('Shutting down...')
    process.exit()
  })

  // POST /newdata
  server.route(Data.routePostNewdata)

  // Always bind to all interfaces on the desired port
  server.listen(config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
    if (err) {
      server.log.error(err)
      process.exit(1)
    }
  })
}
