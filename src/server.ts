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
import * as Cycles from './Data/Cycles'
import { StateHashes } from './Data/State'

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

// Define all endpoints, all requests, and start REST server
function startServer() {
  const server: fastify.FastifyInstance<
    Server,
    IncomingMessage,
    ServerResponse
  > = fastify({
    logger: false,
  })

  server.register(fastifyCors)

  // ========== ENDPOINTS ==========

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
    const signedFirstNodeInfo: P2P.FirstNodeInfo & Crypto.SignedMessage =
      request.body

    // [TODO] req type guard
    // [TODO] Verify req signature

    const ip = signedFirstNodeInfo.nodeInfo.externalIp
    const port = signedFirstNodeInfo.nodeInfo.externalPort
    const publicKey = signedFirstNodeInfo.nodeInfo.publicKey

    if (State.isFirst && NodeList.isEmpty()) {
      const firstNode: NodeList.ConsensusNodeInfo = {
        ip,
        port,
        publicKey,
      }
      // Add first node to NodeList
      // NodeList.addNodes(NodeList.Statuses.SYNCING, firstCycleMarker, firstNode)
      NodeList.addNodes(NodeList.Statuses.SYNCING, 'bogus', firstNode)
      // Set first node as dataSender
      Data.addDataSenders({
        nodeInfo: firstNode,
        type: [Data.TypeNames.CYCLE, Data.TypeNames.STATE],
      })

      const dataRequestCycle: Data.DataRequest<Cycles.Cycle> = {
        type: Data.TypeNames.CYCLE,
        lastData: Cycles.currentCycleCounter,
      } as Data.DataRequest<Cycles.Cycle>
    
      const dataRequestSTATE: Data.DataRequest<StateHashes> = {
        type: Data.TypeNames.STATE,
        lastData: Cycles.currentCycleCounter,
      } as Data.DataRequest<StateHashes>

      const res = Crypto.sign<P2P.FirstNodeResponse>({
        nodeList: NodeList.getList(),
        joinRequest: P2P.createArchiverJoinRequest(),
        dataRequestCycle: Data.createDataRequest<Cycles.Cycle>(
          Data.TypeNames.CYCLE,
          Cycles.currentCycleCounter,
          publicKey
        ),
        dataRequestState: Data.createDataRequest<StateHashes>(
          Data.TypeNames.STATE,
          Cycles.currentCycleCounter,
          publicKey
        ),
      })

      reply.send(res)
    } else {
      let nodeList = NodeList.getActiveList()
      // If we dont have any active nodes, send back the first node in our list
      if (nodeList.length < 1) {
        nodeList = NodeList.getList().slice(0, 1)
      }
      const res = Crypto.sign({
        nodeList,
      })
      reply.send(res)
    }
  })

  server.get('/nodelist', (_request, reply) => {
    let nodeList = NodeList.getActiveList()
    if (nodeList.length < 1) {
      nodeList = NodeList.getList().slice(0, 1)
    }
    const res = Crypto.sign({
      nodeList,
    })
    reply.send(res)
  })

  server.get('/full-nodelist', (_request, reply) => {
    const activeNodeList = NodeList.getActiveList()
    const syncingNodeList = NodeList.getSyncingList()
    const fullNodeList = activeNodeList.concat(syncingNodeList)
    const res = Crypto.sign({
      nodeList: fullNodeList,
    })
    reply.send(res)
  })

  server.get('/debug', (_request, reply) => {
    let nodeList = NodeList.getActiveList()
    if (nodeList.length < 1) {
      nodeList = NodeList.getList().slice(0, 1)
    }
    const nodes = nodeList.map((node) => node.port)
    const senders = [...Data.dataSenders.values()].map(
      (sender) => sender.nodeInfo.port
    )
    const lastData = Cycles.currentCycleCounter

    reply.send({
      lastData,
      senders,
      nodes,
    })
  })

  server.get('/nodeinfo', (_request, reply) => {
    reply.send({
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      time: Date.now(),
    })
  })

  server.get('/cycleinfo', async (_request, reply) => {
    let cycleInfo = await Storage.queryAllCycles()
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  server.get('/cycleinfo/:count', async (_request, reply) => {
    let count = _request.params.count
    console.log(_request.params)
    let cycleInfo = await Storage.queryLatestCycle(count)
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  // [TODO] Remove this before production
  server.get('/exit', (_request, reply) => {
    reply.send('Shutting down...')
    process.exit()
  })

  // [TODO] Remove this before production
  server.get('/nodeids', (_request, reply) => {
    reply.send(NodeList.byId)
  })

  // POST /newdata
  server.route(Data.routePostNewdata)

  // ========== REQUESTS ==========

  Data.emitter.on(
    'selectNewDataSender',
    (
      newSenderInfo: NodeList.ConsensusNodeInfo,
      dataRequest: Data.DataRequest<Cycles.Cycle> & Crypto.TaggedMessage
    ) => {
      // Omar added this logging
      console.log('Sending data request to: ', newSenderInfo.port)
      console.log(
        `http://${newSenderInfo.ip}:${newSenderInfo.port}/requestdata`,
        JSON.stringify(dataRequest, null, 2)
      )
      P2P.postJson(
        `http://${newSenderInfo.ip}:${newSenderInfo.port}/requestdata`,
        dataRequest
      )
    }
  )

  // Start server and bind to port on all interfaces
  server.listen(config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
    if (err) {
      server.log.error(err)
      process.exit(1)
    }
    console.log('Archive-server has started.')
  })
}
