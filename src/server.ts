import { join } from 'path'
import fastify, { FastifyInstance, FastifyRequest } from 'fastify'
import fastifyCors from '@fastify/cors'
import fastifyRateLimit from '@fastify/rate-limit'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { overrideDefaultConfig, config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as NodeList from './NodeList'
import * as P2P from './P2P'
import * as Storage from './archivedCycle/Storage'
import * as Data from './Data/Data'
import * as Cycles from './Data/Cycles'
import * as Utils from './Utils'
import { addHashesGossip } from './archivedCycle/Gossip'
import { syncStateMetaData } from './archivedCycle/StateMetaData'
import * as Logger from './Logger'
import { P2P as P2PTypes } from '@shardus/types'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { Readable } from 'stream'
import MemoryReporting, { memoryReportingInstance } from './profiler/memoryReporting'
import NestedCounters, { nestedCountersInstance } from './profiler/nestedCounters'
import Profiler, { profilerInstance } from './profiler/profiler'
import Statistics from './statistics'
import * as dbstore from './dbstore'
import * as CycleDB from './dbstore/cycles'
import * as AccountDB from './dbstore/accounts'
import * as TransactionDB from './dbstore/transactions'
import * as ReceiptDB from './dbstore/receipts'
import * as OriginalTxDB from './dbstore/originalTxsData'
import { startSaving } from './saveConsoleOutput'
import { setupArchiverDiscovery } from '@shardus/archiver-discovery'
import * as Collector from './Data/Collector'
import * as GossipTxData from './Data/GossipTxData'
const { version } = require('../package.json')

// Socket modules
let io: SocketIO.Server

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
const env = process.env
const args = process.argv
let logDir: string

const TXID_LENGTH = 64

async function start() {
  overrideDefaultConfig(file, env, args)

  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  try {
    await setupArchiverDiscovery({
      hashKey,
      customConfigPath: file.toString(),
    })
  } catch (e) {
    console.log('Error setting up archiver discovery: ', e)
  }

  // If no keypair provided, generate one
  if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
    const keypair = Crypto.core.generateKeypair()
    config.ARCHIVER_PUBLIC_KEY = keypair.publicKey
    config.ARCHIVER_SECRET_KEY = keypair.secretKey
  }
  let logsConfig
  try {
    logsConfig = JSON.parse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
  } catch (err) {
    console.log('Failed to parse archiver log file:', err)
  }
  logDir = `${config.ARCHIVER_LOGS}/${config.ARCHIVER_IP}_${config.ARCHIVER_PORT}`
  const baseDir = '.'
  logsConfig.dir = logDir
  Logger.initLogger(baseDir, logsConfig)
  if (logsConfig.saveConsoleOutput) {
    startSaving(join(baseDir, logsConfig.dir))
  }
  // Initialize storage
  if (config.experimentalSnapshot) {
    await dbstore.initializeDB(config)
  } else {
    await Storage.initStorage(config)
  }

  // Initialize state from config
  await State.initFromConfig(config)

  if (State.isFirst) {
    Logger.mainLogger.debug('We are first archiver. Starting archive-server')
    let lastStoredCycle = await CycleDB.queryLatestCycleRecords(1)
    if (lastStoredCycle && lastStoredCycle.length > 0) {
      await Data.buildNodeListFromStoredCycle(lastStoredCycle[0])
    }

    if (lastStoredCycle && lastStoredCycle.length > 0) {
      // Seems you got restarted, and there are no other archivers to check; sends join request to the nodes first
      let isJoined = false
      let firstTime = true
      let cycleDuration = Cycles.currentCycleDuration
      let checkFromConsensor = true
      do {
        try {
          // Get active nodes from Archiver
          const nodeList = NodeList.getActiveList()

          // try to join the network
          isJoined = await Data.joinNetwork(nodeList, firstTime, checkFromConsensor)
        } catch (err: any) {
          Logger.mainLogger.error('Error while joining network:')
          Logger.mainLogger.error(err)
          Logger.mainLogger.error(err.stack)
          Logger.mainLogger.debug(`Trying to join again in ${cycleDuration} seconds...`)
          await Utils.sleep(cycleDuration)
        }
        firstTime = false
      } while (!isJoined)

      /**
       * [NOTE] [AS] There's a possibility that we could get stuck in this loop
       * if the joinRequest was sent in the wrong cycle quarter (Q2, Q3, or Q4).
       *
       * Since we've dealt with this problem in shardus-global-server, it might be
       * good to refactor this code to do what shardus-global-server does to join
       * the network.
       */

      Logger.mainLogger.debug('We have successfully joined the network')
      io = await startServer()
      await Data.subscribeNodeForDataTransfer()
    } else {
      io = await startServer()
    }
  } else {
    Logger.mainLogger.debug('We are not first archiver. Syncing and starting archive-server')
    syncAndStartServer()
  }
}

function initProfiler(server: FastifyInstance) {
  let memoryReporter = new MemoryReporting(server)
  let nestedCounter = new NestedCounters(server)
  let profiler = new Profiler(server)
  let statistics = new Statistics(
    logDir,
    config.STATISTICS,
    {
      counters: [],
      watchers: {},
      timers: [],
      manualStats: ['cpuPercent'],
    },
    {}
  )
  statistics.startSnapshots()
  statistics.on('snapshot', memoryReportingInstance.updateCpuPercent)

  // ========== ENDPOINTS ==========
  memoryReporter.registerEndpoints()
  nestedCounter.registerEndpoints()
  profiler.registerEndpoints()
}

/** Asynchronous function to synchronize and start the server. */
async function syncAndStartServer() {
  // Validate data if there is any in db
  // Retrieve the count of receipts currently stored in the database
  let lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()

  // Retrieve the count of cycles currently stored in the database
  let lastStoredCycleCount = await CycleDB.queryCyleCount()
  let lastStoredOriginalTxCount = await OriginalTxDB.queryOriginalTxDataCount()
  // Query the latest cycle record from the database
  let lastStoredCycleInfo = await CycleDB.queryLatestCycleRecords(1)

  // Select a random active archiver node from the state
  const randomArchiver = Data.getRandomArchiver()
  // Initialize last stored receipt cycle as 0
  let lastStoredReceiptCycle = 0
  let lastStoredOriginalTxCycle = 0

  // Request total data from the random archiver
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`, 10)

  // Check if the response is valid and all data fields are non-negative
  if (
    !response ||
    response.totalCycles < 0 ||
    response.totalAccounts < 0 ||
    response.totalTransactions < 0 ||
    response.totalReceipts < 0
  ) {
    throw Error(`Can't fetch data from the archiver ${randomArchiver.ip}:${randomArchiver.port}`)
  }
  // Destructure the response to get total counts for cycles, accounts, transactions and receipts
  const { totalCycles, totalReceipts } = response

  // Check if local database has more data than the network, if so, clear the database
  if (lastStoredReceiptCount > totalReceipts || lastStoredCycleCount > totalCycles) {
    throw Error(
      'The existing db has more data than the network data! Clear the DB and start the server again!'
    )
  }

  // If there are stored cycles, validate the old cycle data
  if (lastStoredCycleCount > 0) {
    Logger.mainLogger.debug('Validating old cycles data!')

    // Compare old cycle data with the archiver data
    const cycleResult = await Data.compareWithOldCyclesData(randomArchiver, lastStoredCycleCount)

    // If the cycle data does not match, clear the DB and start again
    if (!cycleResult.success) {
      throw Error(
        'The last saved 10 cycles data does not match with the archiver data! Clear the DB and start the server again!'
      )
    }

    // Update the last stored cycle count
    lastStoredCycleCount = cycleResult.cycle
  }

  // If there are stored receipts, validate the old receipt data
  if (lastStoredReceiptCount > 0) {
    Logger.mainLogger.debug('Validating old receipts data!')
    // Query latest receipts from the DB
    let lastStoredReceiptInfo = await ReceiptDB.queryLatestReceipts(1)

    // If there's any stored receipt, update lastStoredReceiptCycle
    if (lastStoredReceiptInfo && lastStoredReceiptInfo.length > 0)
      lastStoredReceiptCycle = lastStoredReceiptInfo[0].cycle

    // Compare old receipts data with the archiver data
    const receiptResult = await Data.compareWithOldReceiptsData(randomArchiver, lastStoredReceiptCycle)

    // If the receipt data does not match, clear the DB and start again
    if (!receiptResult.success) {
      throw Error(
        'The last saved receipts of last 10 cycles data do not match with the archiver data! Clear the DB and start the server again!'
      )
    }

    // Update the last stored receipt cycle
    lastStoredReceiptCycle = receiptResult.matchedCycle
  }

  if (lastStoredOriginalTxCount > 0) {
    Logger.mainLogger.debug('Validating old Original Txs data!')
    let lastStoredOriginalTxInfo = await OriginalTxDB.queryLatestOriginalTxs(1)
    if (lastStoredOriginalTxInfo && lastStoredOriginalTxInfo.length > 0)
      lastStoredOriginalTxCycle = lastStoredOriginalTxInfo[0].cycle
    const txResult = await Data.compareWithOldOriginalTxsData(randomArchiver, lastStoredOriginalTxCycle)
    if (!txResult.success) {
      throw Error(
        'The saved Original-Txs of last 10 cycles data do not match with the archiver data! Clear the DB and start the server again!'
      )
    }
    lastStoredOriginalTxCycle = txResult.matchedCycle
  }

  // Log the last stored cycle and receipt counts
  Logger.mainLogger.debug(
    'lastStoredCycleCount',
    lastStoredCycleCount,
    'lastStoredReceiptCount',
    lastStoredReceiptCount,
    'lastStoredOriginalTxCount',
    lastStoredOriginalTxCount
  )

  // If your not the first archiver node, get a nodelist from the others

  // Initialize variables for joining the network
  let isJoined = false
  let firstTime = true

  // Get the cycle duration
  let cycleDuration = await Data.getCycleDuration()

  // Attempt to join the network until successful
  do {
    try {
      const randomArchiver = Data.getRandomArchiver()
      // Get active nodes from Archiver
      const nodeList: any = await NodeList.getActiveNodeListFromArchiver(randomArchiver)

      // If no nodes are active, retry the loop
      if (nodeList.length === 0) continue

      // Attempt to join the network
      isJoined = await Data.joinNetwork(nodeList, firstTime)
    } catch (err: any) {
      // Log the error if the joining process fails
      Logger.mainLogger.error('Error while joining network:')
      Logger.mainLogger.error(err)
      Logger.mainLogger.error(err.stack)

      // Sleep for a cycle duration and then retry
      Logger.mainLogger.debug(`Trying to join again in ${cycleDuration} seconds...`)
      await Utils.sleep(cycleDuration * 1000)
    }

    // After the first attempt, set firstTime to false
    firstTime = false
  } while (!isJoined)

  /**
   * [NOTE] [AS] There's a possibility that we could get stuck in this loop
   * if the joinRequest was sent in the wrong cycle quarter (Q2, Q3, or Q4).
   *
   * Since we've dealt with this problem in shardus-global-server, it might be
   * good to refactor this code to do what shardus-global-server does to join
   * the network.
   */

  Logger.mainLogger.debug('We have successfully joined the network')

  // Synchronize Genesis accounts and transactions from the network archivers
  await Data.syncGenesisAccountsFromArchiver() // Sync Genesis Accounts that the network start with.
  await Data.syncGenesisTransactionsFromArchiver()

  // Sync cycle and node list information
  if (config.useSyncV2 === true) {
    await Data.syncCyclesAndNodeListV2(State.activeArchivers, lastStoredCycleCount)
  } else {
    await Data.syncCyclesAndNodeList(lastStoredCycleCount)
  }

  // If experimentalSnapshot is enabled, perform receipt synchronization
  if (config.experimentalSnapshot) {
    // If no receipts stored, synchronize all receipts, otherwise synchronize by cycle
    if (lastStoredReceiptCount === 0) await Data.syncReceipts(lastStoredReceiptCount)
    else {
      Logger.mainLogger.debug('lastStoredReceiptCycle', lastStoredReceiptCycle)
      await Data.syncReceiptsByCycle(lastStoredReceiptCycle)
    }

    if (lastStoredOriginalTxCount === 0) await Data.syncOriginalTxs(lastStoredOriginalTxCount)
    else {
      Logger.mainLogger.debug('lastStoredOriginalTxCycle', lastStoredOriginalTxCycle)
      await Data.syncOriginalTxsByCycle(lastStoredOriginalTxCycle)
    }
    // After receipt data syncing completes, check cycle and receipt again to be sure it's not missing any data

    // Query for the cycle and receipt counts
    lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()
    lastStoredOriginalTxCount = await OriginalTxDB.queryOriginalTxDataCount()
    lastStoredCycleCount = await CycleDB.queryCyleCount()
    lastStoredCycleInfo = await CycleDB.queryLatestCycleRecords(1)

    // Check for any missing data and perform syncing if necessary
    if (lastStoredCycleCount && lastStoredCycleInfo && lastStoredCycleInfo.length > 0) {
      if (lastStoredCycleCount - 1 !== lastStoredCycleInfo[0].counter) {
        throw Error(
          `The archiver has ${lastStoredCycleCount} and the latest stored cycle is ${lastStoredCycleInfo[0].counter}`
        )
      }
      // The following function also syncs Original-tx data
      await Data.syncCyclesAndReceiptsData(
        lastStoredCycleCount,
        lastStoredReceiptCount,
        lastStoredOriginalTxCount
      )
    }
  } else {
    // Sync all state metadata until no older data is fetched from other archivers
    await syncStateMetaData(State.activeArchivers)
  }

  // Wait for one cycle before sending data request if experimentalSnapshot is not enabled
  if (!config.experimentalSnapshot) await Utils.sleep(cycleDuration * 1000)

  // Start the server
  io = await startServer()
  let beforeCycle = Cycles.currentCycleCounter
  // Sending active message to the network
  let isActive = false
  while (!isActive) {
    await Data.sendActiveRequest()
    isActive = await Data.checkActiveStatus()
    // Set as true for now, This needs to be removed after the active record for the archiver is added on the validator side
    isActive = true
  }
  Data.subscribeNodeForDataTransfer()

  // Sync the missing data during the cycle of sending active request
  const randomArchivers = Utils.getRandomItemFromArr(State.activeArchivers, 0, 5)
  const latestCycle = await Cycles.getNewestCycleFromArchivers(randomArchivers)
  await Data.syncCyclesAndTxsDataBetweenCycles(beforeCycle - 1, latestCycle.counter + 1)
}

export function isDebugMode(): boolean {
  return !!(config && config.MODE && config.MODE === 'debug')
}

export function getHashedDevKey(): string {
  console.log(config)
  if (config && config.DEBUG && config.DEBUG.hashedDevAuth) {
    return config.DEBUG.hashedDevAuth
  }
  return ''
}
export function getDevPublicKey(): string {
  if (config && config.DEBUG && config.DEBUG.devPublicKey) {
    return config.DEBUG.devPublicKey
  }
  return ''
}

let lastCounter = 0

export const isDebugMiddleware = (_req, res) => {
  const isDebug = isDebugMode()
  if (!isDebug) {
    try {
      //auth with by checking a password against a hash
      if (_req.query.auth != null) {
        const hashedAuth = Crypto.hashObj({ key: _req.query.auth })
        const hashedDevKey = getHashedDevKey()
        // can get a hash back if no key is set
        if (hashedDevKey === '' || hashedDevKey !== hashedAuth) {
          throw new Error('FORBIDDEN. HashedDevKey authentication is failed.')
        }
        return
      }
      //auth my by checking a signature
      if (_req.query.sig != null && _req.query.sig_counter != null) {
        const ownerPk = getDevPublicKey()
        let requestSig = _req.query.sig
        //check if counter is valid
        let sigObj = {
          route: _req.route,
          count: _req.query.sig_counter,
          sign: { owner: ownerPk, sig: requestSig },
        }

        //reguire a larger counter than before.
        if (sigObj.count < lastCounter) {
          let verified = Crypto.verify(sigObj)
          if (!verified) {
            throw new Error('FORBIDDEN. signature authentication is failed.')
          }
        } else {
          throw new Error('FORBIDDEN. signature counter is failed.')
        }
        lastCounter = sigObj.count //update counter so we can't use it again
        return
      }
      throw new Error('FORBIDDEN. Endpoint is only available in debug mode.')
    } catch (error) {
      // console.log(error)
      // throw new Error('FORBIDDEN. Endpoint is only available in debug mode.')
      res.code(401).send(error)
    }
  }
}

// Define all endpoints, all requests, and start REST server
async function startServer() {
  const server: FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
    logger: false,
  })

  await server.register(fastifyCors)
  await server.register(fastifyRateLimit, {
    global: true,
    max: config.RATE_LIMIT,
    timeWindow: 10,
    allowList: ['127.0.0.1', '0.0.0.0'], // Excludes local IPs from rate limits
  })

  // Socket server instance
  io = require('socket.io')(server.server)
  Data.initSocketServer(io)

  initProfiler(server)

  /**
   * Check the cache for the node list, if it's hot, return it. Otherwise,
   * rebuild the cache and return the node list.
   */
  const getCachedNodeList = (): NodeList.SignedNodeList => {
    const cacheUpdatedTime = NodeList.cacheUpdatedTimes.get('/nodelist')
    const realUpdatedTime = NodeList.realUpdatedTimes.get('/nodelist')

    const byAscendingNodeId = (a: NodeList.ConsensusNodeInfo, b: NodeList.ConsensusNodeInfo) =>
      a.id > b.id ? 1 : -1
    const bucketCacheKey = (index: number) => `/nodelist/${index}`

    if (cacheUpdatedTime && realUpdatedTime && cacheUpdatedTime > realUpdatedTime) {
      // cache is hot, send cache

      const randomIndex = Math.floor(Math.random() * config.N_RANDOM_NODELIST_BUCKETS)
      const cachedNodeList = NodeList.cache.get(bucketCacheKey(randomIndex))
      return cachedNodeList
    }

    // cache is cold, remake cache
    const nodeCount = Math.min(config.N_NODELIST, NodeList.getActiveList().length)

    for (let index = 0; index < config.N_RANDOM_NODELIST_BUCKETS; index++) {
      // If we dont have any active nodes, send back the first node in our list
      const nodeList =
        nodeCount < 1 ? NodeList.getList().slice(0, 1) : NodeList.getRandomActiveNodes(nodeCount)
      const sortedNodeList = [...nodeList].sort(byAscendingNodeId)
      const signedSortedNodeList = Crypto.sign({
        nodeList: sortedNodeList,
      })

      // Update cache
      NodeList.cache.set(bucketCacheKey(index), signedSortedNodeList)
    }

    // Update cache timestamps
    if (NodeList.realUpdatedTimes.get('/nodelist') === undefined) {
      // This gets set when the list of nodes changes. For the first time, set to a large value
      NodeList.realUpdatedTimes.set('/nodelist', Infinity)
    }
    NodeList.cacheUpdatedTimes.set('/nodelist', Date.now())

    const nodeList = NodeList.cache.get(bucketCacheKey(0))
    return nodeList
  }

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
  type NodeListRequest = FastifyRequest<{
    Body: P2P.FirstNodeInfo & Crypto.SignedMessage
  }>

  server.get('/myip', function (request, reply) {
    const ip = request.raw.socket.remoteAddress
    reply.send({ ip })
  })

  server.post('/nodelist', (request: NodeListRequest, reply) => {
    profilerInstance.profileSectionStart('POST_nodelist')
    nestedCountersInstance.countEvent('consensor', 'POST_nodelist', 1)
    const signedFirstNodeInfo = request.body

    if (State.isFirst && NodeList.isEmpty()) {
      try {
        const isSignatureValid = Crypto.verify(signedFirstNodeInfo)
        if (!isSignatureValid) {
          Logger.mainLogger.error('Invalid signature', signedFirstNodeInfo)
          return
        }
      } catch (e) {
        Logger.mainLogger.error(e)
      }
      const ip = signedFirstNodeInfo.nodeInfo.externalIp
      const port = signedFirstNodeInfo.nodeInfo.externalPort
      const publicKey = signedFirstNodeInfo.nodeInfo.publicKey

      const firstNode: NodeList.ConsensusNodeInfo = {
        ip,
        port,
        publicKey,
      }

      Data.initSocketClient(firstNode)

      // Add first node to NodeList
      NodeList.addNodes(NodeList.Statuses.SYNCING, 'bogus', [firstNode])

      // Set first node as dataSender
      const firstDataSender: Data.DataSender = {
        nodeInfo: firstNode,
        types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
        contactTimeout: Data.createContactTimeout(
          firstNode.publicKey,
          'This timeout is created for the first node'
        ),
      }
      Data.addDataSender(firstDataSender)
      let res: P2P.FirstNodeResponse

      if (config.experimentalSnapshot) {
        res = Crypto.sign<P2P.FirstNodeResponse>({
          nodeList: NodeList.getList(),
          joinRequest: P2P.createArchiverJoinRequest(),
          dataRequestCycle: Cycles.currentCycleCounter,
        })
      } else {
        res = Crypto.sign<P2P.FirstNodeResponse>({
          nodeList: NodeList.getList(),
          joinRequest: P2P.createArchiverJoinRequest(),
          dataRequestCycle: Data.createDataRequest<Cycles.Cycle>(
            P2PTypes.SnapshotTypes.TypeNames.CYCLE,
            Cycles.currentCycleCounter,
            publicKey
          ),
          dataRequestStateMetaData: Data.createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
            P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
            Cycles.lastProcessedMetaData,
            publicKey
          ),
        })
      }

      reply.send(res)
    } else {
      // Note, this is doing the same thing as GET /nodelist. However, it has been kept for backwards
      // compatibility.
      const res = getCachedNodeList()
      reply.send(res)
    }
    profilerInstance.profileSectionEnd('POST_nodelist')
  })

  server.get('/nodelist', (_request, reply) => {
    profilerInstance.profileSectionStart('GET_nodelist')
    nestedCountersInstance.countEvent('consensor', 'GET_nodelist')

    const nodeList = getCachedNodeList()
    profilerInstance.profileSectionEnd('GET_nodelist')

    reply.send(nodeList)
  })

  type FullNodeListRequest = FastifyRequest<{
    Querystring: { activeOnly: 'true' | 'false'; syncingOnly: 'true' | 'false' }
  }>

  server.get(
    '/full-nodelist',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request: FullNodeListRequest, reply) => {
      profilerInstance.profileSectionStart('FULL_nodelist')
      nestedCountersInstance.countEvent('consensor', 'FULL_nodelist')
      const { activeOnly, syncingOnly } = _request.query
      const activeNodeList = NodeList.getActiveList()
      const syncingNodeList = NodeList.getSyncingList()
      if (activeOnly === 'true') reply.send(Crypto.sign({ nodeList: activeNodeList }))
      else if (syncingOnly === 'true') reply.send(Crypto.sign({ nodeList: syncingNodeList }))
      else {
        const fullNodeList = activeNodeList.concat(syncingNodeList)
        reply.send(Crypto.sign({ nodeList: fullNodeList }))
      }
      profilerInstance.profileSectionEnd('FULL_nodelist')
    }
  )

  server.get(
    '/removed',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request: FullNodeListRequest, reply) => {
      profilerInstance.profileSectionStart('removed')
      nestedCountersInstance.countEvent('consensor', 'removed')
      reply.send(Crypto.sign({ removedNodes: Cycles.removedNodes }))
      profilerInstance.profileSectionEnd('removed')
    }
  )

  type LostRequest = FastifyRequest<{
    Querystring: { start: any; end: any }
  }>

  server.get('/lost', async (_request: LostRequest, reply) => {
    let { start, end } = _request.query
    if (!start) start = 0
    if (!end) end = Cycles.currentCycleCounter

    let from = parseInt(start)
    let to = parseInt(end)
    if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
      reply.send(Crypto.sign({ success: false, error: `Invalid start and end counters` }))
      return
    }
    let lostNodes = []
    lostNodes = Cycles.getLostNodes(from, to)
    const res = Crypto.sign({
      lostNodes,
    })
    reply.send(res)
  })

  server.get('/archivers', (_request, reply) => {
    profilerInstance.profileSectionStart('GET_archivers')
    nestedCountersInstance.countEvent('consensor', 'GET_archivers')
    const activeArchivers = State.activeArchivers
      .filter(
        (archiver) =>
          State.archiversReputation.has(archiver.publicKey) &&
          State.archiversReputation.get(archiver.publicKey) === 'up'
      )
      .map(({ publicKey, ip, port }) => ({ publicKey, ip, port }))
    profilerInstance.profileSectionEnd('GET_archivers')
    const res = Crypto.sign({
      activeArchivers,
    })
    reply.send(res)
  })

  server.get('/nodeInfo', (_request, reply) => {
    reply.send({
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      version,
      time: Date.now(),
    })
  })

  type CycleInfoRequest = FastifyRequest<{
    Querystring: { start: any; end: any; download: 'true' | 'false' }
  }>

  server.get('/cycleinfo', async (_request: CycleInfoRequest, reply) => {
    let { start, end, download } = _request.query
    if (!start) start = 0
    if (!end) end = Cycles.currentCycleCounter
    let from = parseInt(start)
    let to = parseInt(end)
    let isDownload: boolean = download === 'true'

    if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
      Logger.mainLogger.error(`Invalid start and end counters`)
      reply.send(Crypto.sign({ success: false, error: `Invalid start and end counters` }))
      return
    }
    let cycleInfo = []
    if (config.experimentalSnapshot) cycleInfo = await CycleDB.queryCycleRecordsBetween(from, to)
    else cycleInfo = await Storage.queryCycleRecordsBetween(from, to)
    if (isDownload) {
      let dataInBuffer = Buffer.from(JSON.stringify(cycleInfo), 'utf-8')
      // @ts-ignore
      let dataInStream = Readable.from(dataInBuffer)
      let filename = `cycle_records_from_${from}_to_${to}`

      reply.headers({
        'content-disposition': `attachment; filename="${filename}"`,
        'content-type': 'application/octet-stream',
      })
      reply.send(dataInStream)
    } else {
      const res = Crypto.sign({
        cycleInfo,
      })
      reply.send(res)
    }
  })

  type CycleInfoCountRequest = FastifyRequest<{
    Params: { count: string }
  }>

  server.get('/cycleinfo/:count', async (_request: CycleInfoCountRequest, reply) => {
    let err = Utils.validateTypes(_request.params, { count: 's' })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let count: number = parseInt(_request.params.count)
    if (count <= 0 || Number.isNaN(count)) {
      reply.send(Crypto.sign({ success: false, error: `Invalid count` }))
      return
    }
    if (count > 100) count = 100 // return max 100 cycles
    let cycleInfo: any[]
    if (config.experimentalSnapshot) cycleInfo = await CycleDB.queryLatestCycleRecords(count)
    else cycleInfo = await Storage.queryLatestCycleRecords(count)
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  type ReceiptRequest = FastifyRequest<{
    Querystring: {
      start: string
      end: string
      startCycle: string
      endCycle: string
      type: string
      page: string
      txId: string
      txIdList: string
    }
  }>

  server.get('/originalTx', async (_request: ReceiptRequest, reply) => {
    let err = Utils.validateTypes(_request.query, {
      start: 's?',
      end: 's?',
      startCycle: 's?',
      endCycle: 's?',
      type: 's?',
      page: 's?',
      txId: 's?',
      txIdList: 's?',
    })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let { start, end, startCycle, endCycle, type, page, txId, txIdList } = _request.query
    let originalTxs: any = []
    if (txId) {
      if (txId.length !== TXID_LENGTH) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid txId ${txId}`,
          })
        )
        return
      }
      originalTxs = await OriginalTxDB.queryOriginalTxDataByTxId(txId)
    } else if (txIdList) {
      let txIdListArr = []
      try {
        txIdListArr = JSON.parse(txIdList)
      } catch (e) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid txIdList ${txIdList}`,
          })
        )
        return
      }
      for (const txId of txIdListArr) {
        if (typeof txId !== 'string' || txId.length !== TXID_LENGTH) {
          reply.send(
            Crypto.sign({
              success: false,
              error: `Invalid txId ${txId} in the List`,
            })
          )
          return
        }
        const originalTx = await OriginalTxDB.queryOriginalTxDataByTxId(txId)
        if (originalTx) originalTxs.push(originalTx)
      }
    } else if (start && end) {
      let from = parseInt(start)
      let to = parseInt(end)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 10000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 10000 original transactions`,
          })
        )
        return
      }
      originalTxs = await OriginalTxDB.queryOriginalTxsData(from, count)
    } else if (startCycle && endCycle) {
      let from = parseInt(startCycle)
      let to = parseInt(endCycle)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid startCycle and endCycle counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 1000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 1000 cycles`,
          })
        )
        return
      }
      if (type === 'tally') {
        originalTxs = await OriginalTxDB.queryOriginalTxDataCountByCycles(from, to)
      } else if (type === 'count') {
        originalTxs = await OriginalTxDB.queryOriginalTxDataCount(from, to)
      } else {
        let skip = 0
        let limit = 100
        if (page) {
          skip = parseInt(page) - 1
          if (skip > 0) skip = skip * limit
        }
        originalTxs = await OriginalTxDB.queryOriginalTxsData(skip, limit, from, to)
      }
    }
    const res = Crypto.sign({
      originalTxs,
    })
    reply.send(res)
  })

  server.get('/receipt', async (_request: ReceiptRequest, reply) => {
    let err = Utils.validateTypes(_request.query, {
      start: 's?',
      end: 's?',
      startCycle: 's?',
      endCycle: 's?',
      type: 's?',
      page: 's?',
      txId: 's?',
      txIdList: 's?',
    })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let { start, end, startCycle, endCycle, type, page, txId, txIdList } = _request.query
    let receipts = []
    if (txId) {
      if (txId.length !== TXID_LENGTH) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid txId ${txId}`,
          })
        )
        return
      }
      receipts = await ReceiptDB.queryReceiptByReceiptId(txId)
    } else if (txIdList) {
      let txIdListArr = []
      try {
        txIdListArr = JSON.parse(txIdList)
      } catch (e) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid txIdList ${txIdList}`,
          })
        )
        return
      }
      for (const txId of txIdListArr) {
        if (typeof txId !== 'string' || txId.length !== TXID_LENGTH) {
          reply.send(
            Crypto.sign({
              success: false,
              error: `Invalid txId ${txId} in the List`,
            })
          )
          return
        }
        const receipt = await ReceiptDB.queryReceiptByReceiptId(txId)
        if (receipt) receipts.push(receipt)
      }
    } else if (start && end) {
      let from = parseInt(start)
      let to = parseInt(end)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 10000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 10000 receipts`,
          })
        )
        return
      }
      receipts = await ReceiptDB.queryReceipts(from, count)
    } else if (startCycle && endCycle) {
      let from = parseInt(startCycle)
      let to = parseInt(endCycle)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid startCycle and endCycle counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 1000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 1000 cycles`,
          })
        )
        return
      }
      if (type === 'tally') {
        receipts = await ReceiptDB.queryReceiptCountByCycles(from, to)
      } else if (type === 'count') {
        receipts = await ReceiptDB.queryReceiptCountBetweenCycles(from, to)
      } else {
        let skip = 0
        let limit = 100
        if (page) {
          skip = parseInt(page) - 1
          if (skip > 0) skip = skip * limit
        }
        receipts = await ReceiptDB.queryReceiptsBetweenCycles(skip, limit, from, to)
      }
    }
    const res = Crypto.sign({
      receipts,
    })
    reply.send(res)
  })

  type ReceiptCountRequest = FastifyRequest<{
    Params: {
      count: string
    }
  }>

  server.get('/receipt/:count', async (_request: ReceiptCountRequest, reply) => {
    let err = Utils.validateTypes(_request.params, { count: 's' })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }

    let count: number = parseInt(_request.params.count)
    if (count <= 0 || Number.isNaN(count)) {
      reply.send(Crypto.sign({ success: false, error: `Invalid count` }))
      return
    }
    if (count > 100) {
      reply.send(Crypto.sign({ success: false, error: `Max count is 100` }))
      return
    }
    const receipts = await ReceiptDB.queryLatestReceipts(count)
    const res = Crypto.sign({
      receipts,
    })
    reply.send(res)
  })

  type AccountRequest = FastifyRequest<{
    Querystring: {
      start: string
      end: string
      startCycle: string
      endCycle: string
      type: string
      page: string
      accountId: string
    }
  }>

  server.get('/account', async (_request: AccountRequest, reply) => {
    let err = Utils.validateTypes(_request.query, {
      start: 's?',
      end: 's?',
      startCycle: 's?',
      endCycle: 's?',
      page: 's?',
      address: 's?',
      accountId: 's?',
    })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let accounts = []
    let totalAccounts = 0
    let res
    let { start, end, startCycle, endCycle, page, accountId } = _request.query
    if (start && end) {
      let from = parseInt(start)
      let to = parseInt(end)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 10000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 10000 accounts`,
          })
        )
        return
      }
      accounts = await AccountDB.queryAccounts(from, count)
      res = Crypto.sign({
        accounts,
      })
    } else if (startCycle && endCycle) {
      let from = parseInt(startCycle)
      let to = parseInt(endCycle)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 100) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 100 cycles to query accounts Count`,
          })
        )
        return
      }
      totalAccounts = await AccountDB.queryAccountCountBetweenCycles(from, to)
      if (page) {
        let offset = parseInt(page)
        if (offset < 0) {
          reply.send(Crypto.sign({ success: false, error: `Invalid page number` }))
          return
        }
        let skip = 0
        let limit = 10000 // query 10000 accounts
        if (offset > 0) {
          skip = offset * 10000
        }
        accounts = await AccountDB.queryAccountsBetweenCycles(skip, limit, from, to)
      }
      res = Crypto.sign({
        accounts,
        totalAccounts,
      })
    } else if (accountId) {
      accounts = await AccountDB.queryAccountByAccountId(accountId)
      res = Crypto.sign({
        accounts,
      })
    } else {
      reply.send({
        success: false,
        error: 'not specified which account to show',
      })
      return
    }
    reply.send(res)
  })

  type AccountCountRequest = FastifyRequest<{
    Params: {
      count: string
    }
  }>

  server.get('/account/:count', async (_request: AccountCountRequest, reply) => {
    let err = Utils.validateTypes(_request.params, { count: 's' })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }

    let count: number = parseInt(_request.params.count)
    if (count <= 0 || Number.isNaN(count)) {
      reply.send(Crypto.sign({ success: false, error: `Invalid count` }))
      return
    }
    if (count > 100) {
      reply.send(Crypto.sign({ success: false, error: `Max count is 100` }))
      return
    }
    const accounts = await AccountDB.queryLatestAccounts(count)
    const res = Crypto.sign({
      accounts,
    })
    reply.send(res)
  })

  type TransactionRequest = FastifyRequest<{
    Querystring: {
      start: string
      end: string
      startCycle: string
      endCycle: string
      txId: string
      page: string
      accountId: string
    }
  }>

  server.get('/transaction', async (_request: TransactionRequest, reply) => {
    let err = Utils.validateTypes(_request.query, {
      start: 's?',
      end: 's?',
      txId: 's?',
      accountId: 's?',
      startCycle: 's?',
      endCycle: 's?',
      page: 's?',
    })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let { start, end, txId, accountId, startCycle, endCycle, page } = _request.query
    let transactions = []
    let totalTransactions = 0
    let res
    if (start && end) {
      let from = parseInt(start)
      let to = parseInt(end)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 10000) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 10000 transactions`,
          })
        )
        return
      }
      transactions = await TransactionDB.queryTransactions(from, count)
      res = Crypto.sign({
        transactions,
      })
    } else if (startCycle && endCycle) {
      let from = parseInt(startCycle)
      let to = parseInt(endCycle)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Invalid start and end counters`,
          })
        )
        return
      }
      let count = to - from
      if (count > 100) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 100 cycles to query transactions Count`,
          })
        )
        return
      }
      totalTransactions = await TransactionDB.queryTransactionCountBetweenCycles(from, to)
      if (page) {
        let offset = parseInt(page)
        if (offset < 0) {
          reply.send(Crypto.sign({ success: false, error: `Invalid page number` }))
          return
        }
        let skip = 0
        let limit = 10000 // query 10000 transactions
        if (offset > 0) {
          skip = offset * 10000
        }
        transactions = await TransactionDB.queryTransactionsBetweenCycles(skip, limit, from, to)
      }
      res = Crypto.sign({
        transactions,
        totalTransactions,
      })
    } else if (txId) {
      transactions = await TransactionDB.queryTransactionByTxId(txId)
      res = Crypto.sign({
        transactions,
      })
    } else if (accountId) {
      transactions = await TransactionDB.queryTransactionByAccountId(accountId)
      res = Crypto.sign({
        transactions,
      })
    } else {
      res = {
        success: false,
        error: 'not specified which account to show',
      }
    }
    reply.send(res)
  })

  type TransactionCountRequest = FastifyRequest<{
    Params: {
      count: string
    }
  }>

  server.get('/transaction/:count', async (_request: TransactionCountRequest, reply) => {
    let err = Utils.validateTypes(_request.params, { count: 's' })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }

    let count: number = parseInt(_request.params.count)
    if (count <= 0 || Number.isNaN(count)) {
      reply.send(Crypto.sign({ success: false, error: `Invalid count` }))
      return
    }
    if (count > 100) {
      reply.send(Crypto.sign({ success: false, error: `Max count is 100` }))
      return
    }
    const transactions = await TransactionDB.queryLatestTransactions(count)
    const res = Crypto.sign({
      transactions,
    })
    reply.send(res)
  })

  server.get('/totalData', async (_request, reply) => {
    const totalCycles = await CycleDB.queryCyleCount()
    const totalAccounts = await AccountDB.queryAccountCount()
    const totalTransactions = await TransactionDB.queryTransactionCount()
    const totalReceipts = await ReceiptDB.queryReceiptCount()
    const totalOriginalTxs = await OriginalTxDB.queryOriginalTxDataCount()
    reply.send({
      totalCycles,
      totalAccounts,
      totalTransactions,
      totalReceipts,
      totalOriginalTxs,
    })
  })

  type GossipTxDataRequest = FastifyRequest<{
    Body: GossipTxData.GossipTxData
  }>

  server.post('/gossip-tx-data', async (_request: GossipTxDataRequest, reply) => {
    let gossipPayload = _request.body
    Logger.mainLogger.debug('Gossip Data received', JSON.stringify(gossipPayload))
    const result = Collector.validateGossipTxData(gossipPayload)
    if (!result.success) {
      reply.send(Crypto.sign({ success: false, error: result.error }))
      return
    }
    const res = Crypto.sign({
      success: true,
    })
    reply.send(res)
    Collector.processGossipTxData(gossipPayload)
  })

  // [TODO] Remove this before production
  // server.get('/exit', (_request, reply) => {
  //   reply.send('Shutting down...')
  //   process.exit()
  // })

  // [TODO] Remove this before production
  server.get(
    '/nodeids',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request, reply) => {
      reply.send(NodeList.byId)
    }
  )

  // Config Endpoint
  server.get(
    '/config',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request, reply) => {
      const res = Crypto.sign(config)
      reply.send(res)
    }
  )

  // dataSenders Endpoint
  server.get(
    '/dataSenders',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request, reply) => {
      let data = {
        dataSendersSize: Data.dataSenders.size,
        socketClientsSize: Data.socketClients.size,
      }
      if (_request.query && _request.query['dataSendersList'] === 'true')
        data['dataSendersList'] = Array.from(Data.dataSenders.values()).map(
          (item) => item.nodeInfo.ip + ':' + item.nodeInfo.port
        )
      const res = Crypto.sign(data)
      reply.send(res)
    }
  )

  // Old snapshot ArchivedCycle endpoint;
  if (!config.experimentalSnapshot) {
    type FullArchiveRequest = FastifyRequest<{
      Querystring: {
        start: string
        end: string
      }
    }>

    server.get('/full-archive', async (_request: FullArchiveRequest, reply) => {
      let err = Utils.validateTypes(_request.query, { start: 's', end: 's' })
      if (err) {
        reply.send(Crypto.sign({ success: false, error: err }))
        return
      }
      let { start, end } = _request.query
      let from = parseInt(start)
      let to = parseInt(end)
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send(Crypto.sign({ success: false, error: `Invalid start and end counters` }))
        return
      }
      let count = to - from
      if (count > 100) {
        reply.send(
          Crypto.sign({
            success: false,
            error: `Exceed maximum limit of 100 cycles`,
          })
        )
        return
      }
      let archivedCycles = []
      archivedCycles = await Storage.queryAllArchivedCyclesBetween(from, to)
      const res = Crypto.sign({
        archivedCycles,
      })
      reply.send(res)
    })

    type FullArchiveCountRequest = FastifyRequest<{
      Params: {
        count: string
      }
    }>

    server.get('/full-archive/:count', async (_request: FullArchiveCountRequest, reply) => {
      let err = Utils.validateTypes(_request.params, { count: 's' })
      if (err) {
        reply.send(Crypto.sign({ success: false, error: err }))
        return
      }

      let count: number = parseInt(_request.params.count)
      if (count <= 0 || Number.isNaN(count)) {
        reply.send(Crypto.sign({ success: false, error: `Invalid count` }))
        return
      }
      if (count > 100) {
        reply.send(Crypto.sign({ success: false, error: `Max count is 100` }))
        return
      }
      const archivedCycles = await Storage.queryAllArchivedCycles(count)
      const res = Crypto.sign({
        archivedCycles,
      })
      reply.send(res)
    })

    type GossipHashesRequest = FastifyRequest<{
      Body: {
        sender: string
        data: any
      }
    }>

    server.post('/gossip-hashes', async (_request: GossipHashesRequest, reply) => {
      let gossipMessage = _request.body
      Logger.mainLogger.debug('Gossip received', JSON.stringify(gossipMessage))
      addHashesGossip(gossipMessage.sender, gossipMessage.data)
      const res = Crypto.sign({
        success: true,
      })
      reply.send(res)
    })
  }

  // Start server and bind to port on all interfaces
  server.listen(
    {
      port: config.ARCHIVER_PORT,
      host: '0.0.0.0',
    },
    (err, _address) => {
      Logger.mainLogger.debug('Listening', config.ARCHIVER_PORT)
      if (err) {
        server.log.error(err)
        process.exit(1)
      }
      Logger.mainLogger.debug('Archive-server has started.')
      State.addSigListeners()
      Collector.scheduleCacheCleanup()
      Collector.scheduleMissingTxsDataQuery()
    }
  )
  return io
}

start()
