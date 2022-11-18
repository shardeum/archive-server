import { join } from 'path'
import * as fastify from 'fastify'
import * as fastifyCors from 'fastify-cors'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { overrideDefaultConfig, config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as NodeList from './NodeList'
import * as P2P from './P2P'
import * as Storage from './Snapshot/Storage'
import * as Data from './Data/Data'
import * as Cycles from './Data/Cycles'
import * as Utils from './Utils'
import { sendGossip, addHashesGossip } from './Snapshot/Gossip'
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
import { startSaving } from './saveConsoleOutput'

// Socket modules
let io: SocketIO.Server

// Override default config params from config file, env vars, and cli args
const file = join(process.cwd(), 'archiver-config.json')
const env = process.env
const args = process.argv
let logDir: string

async function start() {
  overrideDefaultConfig(file, env, args)

  // Set crypto hash key from config
  Crypto.setCryptoHashKey(config.ARCHIVER_HASH_KEY)

  // If no keypair provided, generate one
  if (config.ARCHIVER_SECRET_KEY === '' || config.ARCHIVER_PUBLIC_KEY === '') {
    const keypair = Crypto.core.generateKeypair()
    config.ARCHIVER_PUBLIC_KEY = keypair.publicKey
    config.ARCHIVER_SECRET_KEY = keypair.secretKey
  }

  const logsConfig = JSON.parse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
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
          await Utils.sleep(cycleDuration * 1000)
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
      io = startServer()
      await Data.subscribeNodeForDataTransfer()
    } else {
      io = startServer()
    }
  } else {
    Logger.mainLogger.debug('We are not first archiver. Syncing and starting archive-server')
    syncAndStartServer()
  }
}

function initProfiler(server: fastify.FastifyInstance) {
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

async function syncAndStartServer() {
  // Validate data if there is any in db
  let lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()
  let lastStoredCycleCount = await CycleDB.queryCyleCount()
  let lastStoredCycleInfo = await CycleDB.queryLatestCycleRecords(1)
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)[0]
  let lastStoredReceiptCycle = 0
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
  if (
    !response ||
    response.totalCycles < 0 ||
    response.totalAccounts < 0 ||
    response.totalTransactions < 0 ||
    response.totalReceipts < 0
  ) {
    throw Error(`Can't fetch data from the archiver ${randomArchiver.ip}:${randomArchiver.port}`)
  }
  const { totalCycles, totalAccounts, totalTransactions, totalReceipts } = response
  if (lastStoredReceiptCount > totalReceipts || lastStoredCycleCount > totalCycles) {
    throw Error(
      'The existing db has more data than the network data! Clear the DB and start the server again!'
    )
  }
  if (lastStoredCycleCount > 0) {
    Logger.mainLogger.debug('Validating old cycles data!')
    const cycleResult = await Data.compareWithOldCyclesData(randomArchiver, lastStoredCycleCount)
    if (!cycleResult.success) {
      throw Error(
        'The last saved 10 cycles data does not match with the archiver data! Clear the DB and start the server again!'
      )
    }
    lastStoredCycleCount = cycleResult.cycle
  }
  if (lastStoredReceiptCount > 0) {
    Logger.mainLogger.debug('Validating old receipts data!')
    // const receiptResult = await Data.compareWithOldReceiptsData(
    //   randomArchiver,
    //   lastStoredReceiptCount
    // )
    // if (!receiptResult.success) {
    //   throw Error(
    //     'The last saved 10 receipts data does not match with the archiver data! Clear the DB and start the server again!'
    //   )
    // }
    // lastStoredReceiptCount =
    //   lastStoredReceiptCount - receiptResult.receiptsToMatchCount

    let lastStoredReceiptInfo = await ReceiptDB.queryLatestReceipts(1)
    if (lastStoredReceiptInfo && lastStoredReceiptInfo.length > 0)
      lastStoredReceiptCycle = lastStoredReceiptInfo[0].cycle
    const receiptResult = await Data.compareWithOldReceiptsData(randomArchiver, lastStoredReceiptCycle)
    if (!receiptResult.success) {
      throw Error(
        'The last saved receipts of last 10 cycles data do not match with the archiver data! Clear the DB and start the server again!'
      )
    }
    lastStoredReceiptCycle = receiptResult.matchedCycle
  }

  Logger.mainLogger.debug(
    'lastStoredCycleCount',
    lastStoredCycleCount,
    'lastStoredReceiptCount',
    lastStoredReceiptCount
  )
  // If your not the first archiver node, get a nodelist from the others
  let isJoined = false
  let firstTime = true
  let cycleDuration = await Data.getCycleDuration()
  do {
    try {
      // Get active nodes from Archiver
      const nodeList: any = await NodeList.getActiveListFromArchivers(State.activeArchivers)

      // try to join the network
      isJoined = await Data.joinNetwork(nodeList, firstTime)
    } catch (err: any) {
      Logger.mainLogger.error('Error while joining network:')
      Logger.mainLogger.error(err)
      Logger.mainLogger.error(err.stack)
      Logger.mainLogger.debug(`Trying to join again in ${cycleDuration} seconds...`)
      await Utils.sleep(cycleDuration * 1000)
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

  // TODO - update to sync geneis transaction receipts data also from the archiver
  await Data.syncGenesisAccountsFromArchiver(State.activeArchivers) // Sync Genesis Accounts that the network start with.
  await Data.syncGenesisTransactionsFromArchiver(State.activeArchivers)

  await Data.syncCyclesAndNodeList(State.activeArchivers, lastStoredCycleCount)

  if (config.experimentalSnapshot) {
    if (lastStoredReceiptCount === 0) await Data.syncReceipts(State.activeArchivers, lastStoredReceiptCount)
    else {
      Logger.mainLogger.debug('lastStoredReceiptCycle', lastStoredReceiptCycle)
      await Data.syncReceiptsByCycle(State.activeArchivers, lastStoredReceiptCycle)
    }
    // After receipt data syncing completes, check cycle and receipt again to be sure it's not missing any data
    lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()
    lastStoredCycleCount = await CycleDB.queryCyleCount()
    lastStoredCycleInfo = await CycleDB.queryLatestCycleRecords(1)
    if (lastStoredCycleCount && lastStoredCycleInfo && lastStoredCycleInfo.length > 0) {
      if (lastStoredCycleCount - 1 !== lastStoredCycleInfo[0].counter) {
        throw Error(
          `The archiver has ${lastStoredCycleCount} and the latest stored cycle is ${lastStoredCycleInfo[0].counter}`
        )
      }
      await Data.syncCyclesAndReceiptsData(
        State.activeArchivers,
        lastStoredCycleCount,
        lastStoredReceiptCount
      )
    }
  } else {
    // Sync all state metadata until no older data is fetched from other archivers
    await Data.syncStateMetaData(State.activeArchivers)
  }

  if (!config.experimentalSnapshot)
    // wait for one cycle before sending data request
    await Utils.sleep(cycleDuration * 1000)

  // start fastify server
  io = startServer()
  await Data.subscribeNodeForDataTransfer()
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
function startServer() {
  const server: fastify.FastifyInstance<Server, IncomingMessage, ServerResponse> = fastify({
    logger: false,
  })

  server.register(fastifyCors)
  // server.register(require('fastify-rate-limit'), {
  //   max: config.RATE_LIMIT,
  //   timeWindow: 1000,
  // })

  // Socket server instance
  io = require('socket.io')(server.server)
  Data.initSocketServer(io)

  initProfiler(server)

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
    profilerInstance.profileSectionStart('POST_nodelist')
    nestedCountersInstance.countEvent('consensor', 'POST_nodelist', 1)
    const signedFirstNodeInfo: P2P.FirstNodeInfo & Crypto.SignedMessage = request.body

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
      Data.addDataSenders({
        nodeInfo: firstNode,
        types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
        replaceTimeout: Data.createReplaceTimeout(firstNode.publicKey),
      })

      const res = Crypto.sign<P2P.FirstNodeResponse>({
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

      reply.send(res)
    } else {
      const cacheUpdatedTime = NodeList.cacheUpdatedTimes.get('/nodelist')
      const realUpdatedTime = NodeList.realUpdatedTimes.get('/nodelist')
      const cached = NodeList.cache.get('/nodelist')
      if (cached && cacheUpdatedTime && realUpdatedTime && cacheUpdatedTime > realUpdatedTime) {
        // cache is hot, send cache

        reply.send(cached)
      } else {
        // cache is cold, remake cache

        const NODE_COUNT = Math.min(config.N_NODELIST, NodeList.getActiveList().length)
        // If we dont have any active nodes, send back the first node in our list
        let nodeList: NodeList.ConsensusNodeInfo[] =
          NODE_COUNT < 1 ? NodeList.getList().slice(0, 1) : NodeList.getRandomActiveNode(NODE_COUNT)
        let sortedNodeList = [...nodeList].sort((a: any, b: any) => (a.id > b.id ? 1 : -1))
        const res = Crypto.sign({
          nodeList: sortedNodeList,
        })

        // Update cache
        if (NodeList.realUpdatedTimes.get('/nodelist') === undefined) {
          NodeList.realUpdatedTimes.set('/nodelist', 0)
        }
        NodeList.cache.set('/nodelist', res)
        NodeList.cacheUpdatedTimes.set('/nodelist', Date.now())

        reply.send(res)
      }
    }
    profilerInstance.profileSectionEnd('POST_nodelist')
  })

  server.get('/nodelist', (_request, reply) => {
    profilerInstance.profileSectionStart('GET_nodelist')
    nestedCountersInstance.countEvent('consensor', 'GET_nodelist')

    const NODE_COUNT = Math.min(config.N_NODELIST, NodeList.getActiveList().length)
    let nodeList: NodeList.ConsensusNodeInfo[] =
      NODE_COUNT < 1 ? NodeList.getList().slice(0, 1) : NodeList.getRandomActiveNode(NODE_COUNT)

    let sortedNodeList = [...nodeList].sort((a: any, b: any) => (a.id > b.id ? 1 : -1))
    const res = Crypto.sign({
      nodeList: sortedNodeList,
    })
    profilerInstance.profileSectionEnd('GET_nodelist')
    reply.send(res)
  })

  server.get(
    '/full-nodelist',
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request, reply) => {
      const activeNodeList = NodeList.getActiveList()
      const syncingNodeList = NodeList.getSyncingList()
      const fullNodeList = activeNodeList.concat(syncingNodeList)
      const res = Crypto.sign({
        nodeList: fullNodeList,
      })
      reply.send(res)
    }
  )

  server.get('/lost', async (_request, reply) => {
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

  server.get('/nodeinfo', (_request, reply) => {
    reply.send({
      publicKey: config.ARCHIVER_PUBLIC_KEY,
      ip: config.ARCHIVER_IP,
      port: config.ARCHIVER_PORT,
      time: Date.now(),
    })
  })

  server.get('/cycleinfo', async (_request, reply) => {
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

  server.get('/cycleinfo/:count', async (_request, reply) => {
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

  server.get('/receipt', async (_request, reply) => {
    let err = Utils.validateTypes(_request.query, {
      start: 's?',
      end: 's?',
      startCycle: 's?',
      endCycle: 's?',
      type: 's?',
      page: 's?',
    })
    if (err) {
      reply.send(Crypto.sign({ success: false, error: err }))
      return
    }
    let { start, end, startCycle, endCycle, type, page } = _request.query
    let receipts = []
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

  server.get('/receipt/:count', async (_request, reply) => {
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

  server.get('/account', async (_request, reply) => {
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

  server.get('/account/:count', async (_request, reply) => {
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

  server.get('/transaction', async (_request, reply) => {
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

  server.get('/transaction/:count', async (_request, reply) => {
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
    reply.send({
      totalCycles,
      totalAccounts,
      totalTransactions,
      totalReceipts,
    })
  })

  server.post('/gossip-hashes', async (_request, reply) => {
    let gossipMessage = _request.body
    Logger.mainLogger.debug('Gossip received', JSON.stringify(gossipMessage))
    addHashesGossip(gossipMessage.sender, gossipMessage.data)
    const res = Crypto.sign({
      success: true,
    })
    reply.send(res)
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

  // Old snapshot ArchivedCycle endpoint;
  if (!config.experimentalSnapshot) {
    server.get('/full-archive', async (_request, reply) => {
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

    server.get('/full-archive/:count', async (_request, reply) => {
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
  }

  // Start server and bind to port on all interfaces
  server.listen(config.ARCHIVER_PORT, '0.0.0.0', (err, _address) => {
    Logger.mainLogger.debug('Listening3', config.ARCHIVER_PORT)
    if (err) {
      server.log.error(err)
      process.exit(1)
    }
    Logger.mainLogger.debug('Archive-server has started.')
    State.addSigListeners()
  })
  return io
}

start()
