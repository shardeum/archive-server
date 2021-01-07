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
import * as Utils from './Utils'
import { sendGossip, addHashesGossip } from './Data/Gossip'

// Socket modules
let io: SocketIO.Server

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
const env = process.env
const args = process.argv

async function start () {
  overrideDefaultConfig(file, env, args)

  // Set crypto hash key from config
  Crypto.setCryptoHashKey(config.ARCHIVER_HASH_KEY)

  // If no keypair provided, generate one
  if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
    const keypair = Crypto.core.generateKeypair()
    config.ARCHIVER_PUBLIC_KEY = keypair.publicKey
    config.ARCHIVER_SECRET_KEY = keypair.secretKey
  }

  // Initialize storage
  await Storage.initStorage(config)

  // Initialize state from config
  await State.initFromConfig(config)

  if (State.isFirst === false) {
    console.log('We are not first archiver. Syncing and starting archive-server')
    syncAndStartServer()
  } else {
    console.log('We are first archiver. Starting archive-server')
    io = startServer()
  }
}

async function syncAndStartServer() {
  // If your not the first archiver node, get a nodelist from the others
  const nodeList: any = await NodeList.getActiveListFromArchivers(State.activeArchivers)

  // If there are active consensors in the network, sync cycle chain and state metadata from other archivers
  if (nodeList && nodeList.length > 0) {
    const randomIndex = Math.floor(Math.random() * nodeList.length)
    const randomConsensor: NodeList.ConsensusNodeInfo = nodeList[randomIndex]

    // Set randomly selected consensors as dataSender
    Data.addDataSenders({
      nodeInfo: randomConsensor,
      types: [Data.TypeNames.CYCLE, Data.TypeNames.STATE_METADATA],
    })

    // Send a join request to a consensus node from the nodelist
    Data.sendJoinRequest(randomConsensor)
    const cycleDuration = await Data.getCycleDuration()

    if (!cycleDuration) return
    await Data.checkJoinStatus(cycleDuration)
    
    console.log('We have successfully joined the network')

    await Data.syncCyclesAndNodeList(State.activeArchivers)

    // TODO: Sync all cycles until no older cycle is fetched from other archivers

    await Data.syncStateMetaData(State.activeArchivers)

    // wait for one cycle before sending data request
    Utils.sleep(cycleDuration * 1000)

    // After we've joined, select a consensus node to be your dataSender
    const dataRequest = Crypto.sign({
      dataRequestCycle: Data.createDataRequest<Cycles.Cycle>(
        Data.TypeNames.CYCLE,
        Cycles.getCurrentCycleCounter(),
        randomConsensor.publicKey
      ),
      dataRequestStateMetaData: Data.createDataRequest<Data.StateMetaData>(
        Data.TypeNames.STATE_METADATA,
        Cycles.lastProcessedMetaData,
        randomConsensor.publicKey
      ),
    })
    const newSender: Data.DataSender = {
      nodeInfo: randomConsensor,
      types: [Data.TypeNames.CYCLE, Data.TypeNames.STATE_METADATA],
      contactTimeout: Data.createContactTimeout(randomConsensor.publicKey),
      replaceTimeout: Data.createReplaceTimeout(randomConsensor.publicKey),
    }
    Data.sendDataRequest(newSender, dataRequest)
    Data.initSocketClient(randomConsensor)
  }
  io = startServer()
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


  // Socket server instance
  io = (require('socket.io'))(server.server)
  Data.initSocketServer(io)

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

      Data.initSocketClient(firstNode)

      // Add first node to NodeList
      // NodeList.addNodes(NodeList.Statuses.SYNCING, firstCycleMarker, firstNode)
      NodeList.addNodes(NodeList.Statuses.SYNCING, 'bogus', [firstNode])
      // Set first node as dataSender
      Data.addDataSenders({
        nodeInfo: firstNode,
        types: [Data.TypeNames.CYCLE, Data.TypeNames.STATE_METADATA],
        replaceTimeout: Data.createReplaceTimeout(firstNode.publicKey)
      })

      const res = Crypto.sign<P2P.FirstNodeResponse>({
        nodeList: NodeList.getList(),
        joinRequest: P2P.createArchiverJoinRequest(),
        dataRequestCycle: Data.createDataRequest<Cycles.Cycle>(
          Data.TypeNames.CYCLE,
          Cycles.currentCycleCounter,
          publicKey
        ),
        dataRequestStateMetaData: Data.createDataRequest<Data.StateMetaData>(
          Data.TypeNames.STATE_METADATA,
          Cycles.lastProcessedMetaData,
          publicKey
        )
      })

      reply.send(res)
    } else {
      let nodeList = NodeList.getActiveList()
      // If we dont have any active nodes, send back the first node in our list
      if (nodeList.length < 1) {
        nodeList = NodeList.getList().slice(0, 1)
      }
      const res = Crypto.sign({
        nodeList: nodeList.sort((a: any, b: any) => a.id - b.id),
      })
      reply.send(res)
    }
  })

  server.get('/nodelist', (_request, reply) => {
    let nodeList = NodeList.getActiveList()
    if (nodeList.length < 1) {
      nodeList = NodeList.getList().slice(0, 1)
    }
    let sortedNodeList = [...nodeList].sort((a: any, b: any) => a.id - b.id)
    console.log('nodeList', nodeList)
    console.log('sortedNodeList', sortedNodeList)

    const res = Crypto.sign({
      nodeList: sortedNodeList,
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

  server.get('/full-archive', async (_request, reply) => {
    const archivedCycles = await Storage.queryAllArchivedCycles()
    const res = Crypto.sign({
      archivedCycles,
    })
    reply.send(res)
  })

  server.get('/full-archive/:count', async (_request, reply) => {
    let count = _request.params.count || 10
    const archivedCycles = await Storage.queryAllArchivedCycles(count)
    const res = Crypto.sign({
      archivedCycles,
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
    let cycleInfo = []
    if (_request.query.start && _request.query.end) {
      let { start, end } = _request.query
      cycleInfo = await Storage.queryCycleRecordsBetween(start, end)
    } else {
      cycleInfo = await Storage.queryAllCycleRecords()
    }
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  server.get('/cycleinfo/:count', async (_request, reply) => {
    let count = _request.params.count
    let cycleInfo = await Storage.queryLatestCycleRecords(count)
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  server.post('/gossip-hashes', async (_request, reply) => {
    let gossipMessage = _request.body
    console.log('Gossip received', gossipMessage)
    addHashesGossip(gossipMessage.data.counter, gossipMessage.sender, gossipMessage.data)
    const res = Crypto.sign({
      success: true,
    })
    reply.send(res)
  })

  // server.get('/statehashes', async (_request, reply) => {
  //   let stateHashes = await Storage.queryAllStateHashes()
  //   const res = Crypto.sign({
  //     stateHashes,
  //   })
  //   reply.send(res)
  // })

  // server.get('/statehashes/:count', async (_request, reply) => {
  //   let count = _request.params.count
  //   console.log(_request.params)
  //   let stateHashes = await Storage.queryLatestStateHash(count)
  //   const res = Crypto.sign({
  //     stateHashes,
  //   })
  //   reply.send(res)
  // })

  // server.get('/download/receipt', async (_request, reply) => {
  //   let receiptMap = await Storage.queryAllReceipts()
  //   const res = Crypto.sign({
  //     receiptMap
  //   })
  //   reply.send(res)
  // })

  // [TODO] Remove this before production
  server.get('/exit', (_request, reply) => {
    reply.send('Shutting down...')
    process.exit()
  })

  // [TODO] Remove this before production
  server.get('/nodeids', (_request, reply) => {
    reply.send(NodeList.byId)
  })

  // Start server and bind to port on all interfaces
  server.listen(config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
    console.log('Listening3')
    if (err) {
      server.log.error(err)
      process.exit(1)
    }
    console.log('Archive-server has started.')
  })
  return io
}

start()