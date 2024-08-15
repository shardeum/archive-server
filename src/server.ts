const _startingMessage = `@shardus/archiver starting at
  locale:  ${new Date().toLocaleString()}
  ISO/UTC: ${new Date().toISOString()}`
console.log(_startingMessage)
console.error(_startingMessage)

import { join } from 'path'
import fastify, { FastifyInstance } from 'fastify'
import fastifyCors from '@fastify/cors'
import fastifyRateLimit from '@fastify/rate-limit'
import * as clusterModule from 'cluster'
import type { Worker } from 'cluster'
import { cpus } from 'os'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { overrideDefaultConfig, config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as NodeList from './NodeList'
import * as Storage from './archivedCycle/Storage'
import * as Data from './Data/Data'
import * as Cycles from './Data/Cycles'
import { initDataLogWriter } from './Data/DataLogWriter'
import * as Utils from './Utils'
import { syncStateMetaData } from './archivedCycle/StateMetaData'
import * as Logger from './Logger'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import MemoryReporting, {
  memoryReportingInstance,
  setMemoryReportingInstance,
} from './profiler/memoryReporting'
import NestedCounters, { setNestedCountersInstance } from './profiler/nestedCounters'
import Profiler, { setProfilerInstance } from './profiler/profiler'
import Statistics from './statistics'
import * as dbstore from './dbstore'
import * as CycleDB from './dbstore/cycles'
import * as ReceiptDB from './dbstore/receipts'
import * as OriginalTxDB from './dbstore/originalTxsData'
import { startSaving } from './saveConsoleOutput'
import { setupArchiverDiscovery } from '@shardus/archiver-discovery'
import * as Collector from './Data/Collector'
import { loadGlobalAccounts, syncGlobalAccount } from './GlobalAccount'
import { setShutdownCycleRecord, cycleRecordWithShutDownMode } from './Data/Cycles'
import { registerRoutes } from './API'
import { Utils as StringUtils } from '@shardus/types'
import { healthCheckRouter } from './routes/healthCheck'

const configFile = join(process.cwd(), 'archiver-config.json')
let logDir: string
const cluster = clusterModule as unknown as clusterModule.Cluster
const numCPUs = cpus().length

type workerProcessId = number
export const workerProcessMap = new Map<workerProcessId, Worker>()

async function start(): Promise<void> {
  overrideDefaultConfig(configFile)
  // Set crypto hash keys from config
  const hashKey = config.ARCHIVER_HASH_KEY
  Crypto.setCryptoHashKey(hashKey)
  try {
    await setupArchiverDiscovery({
      hashKey,
      customConfigPath: configFile.toString(),
      archiverTimeoutInMilliSeconds: 2000, // 2 seconds
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
    logsConfig = StringUtils.safeJsonParse(readFileSync(resolve(__dirname, '../archiver-log.json'), 'utf8'))
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
  State.addSigListeners()

  const lastStoredCycle = await CycleDB.queryLatestCycleRecords(1)
  if (lastStoredCycle && lastStoredCycle.length > 0) {
    const lastStoredCycleMode = lastStoredCycle[0].mode
    if (lastStoredCycleMode === 'shutdown') {
      setShutdownCycleRecord(lastStoredCycle[0])
      Logger.mainLogger.debug('Found shutdown cycleRecord', cycleRecordWithShutDownMode)
      // Initialize state from config
      await State.initFromConfig(config, true)
      const result = await State.compareCycleRecordWithOtherArchivers(
        cycleRecordWithShutDownMode.archiversAtShutdown,
        cycleRecordWithShutDownMode
      )
      if (result) {
        State.resetActiveArchivers(cycleRecordWithShutDownMode.archiversAtShutdown)
        // Load global account from db
        await loadGlobalAccounts()
        await startServer()
        return
      }
    }
  }
  // Initialize state from config
  await State.initFromConfig(config)

  if (State.isFirst) {
    Logger.mainLogger.debug('We are first archiver. Starting archive-server')
    const lastStoredCycle = await CycleDB.queryLatestCycleRecords(1)
    if (lastStoredCycle && lastStoredCycle.length > 0) {
      // Load global account from db
      await loadGlobalAccounts()
      // Seems you got restarted, and there are no other archivers to check; build nodelists and send join request to the nodes first
      await Data.buildNodeListFromStoredCycle(lastStoredCycle[0])

      let isJoined = false
      let firstTime = true
      const cycleDuration = Cycles.currentCycleDuration
      do {
        try {
          // Get active nodes from Archiver
          const nodeList = NodeList.getActiveList()

          // try to join the network
          isJoined = await Data.joinNetwork(nodeList, firstTime)
        } catch (err: unknown) {
          Logger.mainLogger.error('Error while joining network:')
          Logger.mainLogger.error(err as Error)
          Logger.mainLogger.error((err as Error).stack)
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
      await startServer()
      await Data.subscribeNodeForDataTransfer()
    } else {
      await startServer()
    }
  } else {
    Logger.mainLogger.debug('We are not first archiver. Syncing and starting archive-server')
    syncAndStartServer()
  }
}

function initProfiler(server: FastifyInstance): void {
  const memoryReporter = new MemoryReporting(server)
  setMemoryReportingInstance(memoryReporter)
  const nestedCounter = new NestedCounters(server)
  setNestedCountersInstance(nestedCounter)
  const profiler = new Profiler(server)
  setProfilerInstance(profiler)
  const statistics = new Statistics(
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
async function syncAndStartServer(): Promise<void> {
  // Validate data if there is any in db
  // Retrieve the count of receipts currently stored in the database
  let lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()

  // Retrieve the count of cycles currently stored in the database
  let lastStoredCycleCount = await CycleDB.queryCyleCount()
  let lastStoredOriginalTxCount = await OriginalTxDB.queryOriginalTxDataCount()
  // Query the latest cycle record from the database
  let lastStoredCycleInfo = (await CycleDB.queryLatestCycleRecords(1))[0]

  // Initialize last stored receipt cycle as 0
  let lastStoredReceiptCycle = 0
  let lastStoredOriginalTxCycle = 0

  interface TotalDataResponse {
    totalCycles: number
    totalAccounts: number
    totalTransactions: number
    totalReceipts: number
    totalOriginalTxs: number
  }

  // Request total data from the random archiver
  const response: TotalDataResponse = await Data.getTotalDataFromArchivers()

  // Check if the response is valid and all data fields are non-negative
  if (
    !response ||
    response.totalCycles < 0 ||
    response.totalAccounts < 0 ||
    response.totalTransactions < 0 ||
    response.totalReceipts < 0
  ) {
    throw Error(`Can't fetch total data from archivers`)
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
    const cycleResult = await Data.compareWithOldCyclesData(lastStoredCycleInfo.counter)

    // If the cycle data does not match, clear the DB and start again
    if (!cycleResult.success) {
      throw Error(
        'The last saved 10 cycles data does not match with the archiver data! Clear the DB and start the server again!'
      )
    }

    // Update the last stored cycle count
    lastStoredCycleCount = cycleResult.matchedCycle
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
  const cycleDuration = await Data.getCycleDuration()

  // Attempt to join the network until successful
  do {
    try {
      const randomArchiver = State.getRandomArchiver()
      // Get active nodes from Archiver
      const nodeList: NodeList.ConsensusNodeInfo[] =
        await NodeList.getActiveNodeListFromArchiver(randomArchiver)

      // If no nodes are active, retry the loop
      if (nodeList.length === 0) continue

      // Attempt to join the network
      isJoined = await Data.joinNetwork(nodeList, firstTime)
    } catch (err) {
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

  // Once the archiver is joined, check if the existing data (receipt/originalTxData) is valid
  // If there are stored receipts, validate the old receipt data
  if (lastStoredReceiptCount > 0) {
    Logger.mainLogger.debug('Validating old receipts data!')
    // Query latest receipts from the DB
    const lastStoredReceiptInfo = await ReceiptDB.queryLatestReceipts(1)

    // If there's any stored receipt, update lastStoredReceiptCycle
    if (lastStoredReceiptInfo && lastStoredReceiptInfo.length > 0)
      lastStoredReceiptCycle = lastStoredReceiptInfo[0].cycle

    // Compare old receipts data with the archiver data
    const receiptResult = await Data.compareWithOldReceiptsData(lastStoredReceiptCycle)

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
    const lastStoredOriginalTxInfo = await OriginalTxDB.queryLatestOriginalTxs(1)
    if (lastStoredOriginalTxInfo && lastStoredOriginalTxInfo.length > 0)
      lastStoredOriginalTxCycle = lastStoredOriginalTxInfo[0].cycle
    const txResult = await Data.compareWithOldOriginalTxsData(lastStoredOriginalTxCycle)
    if (!txResult.success) {
      throw Error(
        'The saved Original-Txs of last 10 cycles data do not match with the archiver data! Clear the DB and start the server again!'
      )
    }
    lastStoredOriginalTxCycle = txResult.matchedCycle
  }

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
    // Sync GlobalAccountsList and cache the Global Network Account
    await syncGlobalAccount()
    // If no receipts stored, synchronize all receipts, otherwise synchronize by cycle
    if (lastStoredReceiptCount === 0) await Data.syncReceipts()
    else {
      Logger.mainLogger.debug('lastStoredReceiptCycle', lastStoredReceiptCycle)
      await Data.syncReceiptsByCycle(lastStoredReceiptCycle)
    }

    if (lastStoredOriginalTxCount === 0) await Data.syncOriginalTxs()
    else {
      Logger.mainLogger.debug('lastStoredOriginalTxCycle', lastStoredOriginalTxCycle)
      await Data.syncOriginalTxsByCycle(lastStoredOriginalTxCycle)
    }
    // After receipt data syncing completes, check cycle and receipt again to be sure it's not missing any data

    // Query for the cycle and receipt counts
    lastStoredReceiptCount = await ReceiptDB.queryReceiptCount()
    lastStoredOriginalTxCount = await OriginalTxDB.queryOriginalTxDataCount()
    lastStoredCycleCount = await CycleDB.queryCyleCount()
    lastStoredCycleInfo = (await CycleDB.queryLatestCycleRecords(1))[0]

    // Check for any missing data and perform syncing if necessary
    if (lastStoredCycleCount - 1 !== lastStoredCycleInfo.counter) {
      throw Error(
        `The archiver has ${lastStoredCycleCount} and the latest stored cycle is ${lastStoredCycleInfo.counter}`
      )
    }
    await Data.syncCyclesAndTxsData(lastStoredCycleCount, lastStoredReceiptCount, lastStoredOriginalTxCount)
  } else {
    // Sync all state metadata until no older data is fetched from other archivers
    await syncStateMetaData(State.activeArchivers)
  }

  // Wait for one cycle before sending data request if experimentalSnapshot is not enabled
  if (!config.experimentalSnapshot) await Utils.sleep(cycleDuration * 1000)

  // Start the server
  await startServer()

  if (!config.sendActiveMessage) {
    await Data.subscribeNodeForDataTransfer()
    return
  }
  const beforeCycle = Cycles.getCurrentCycleCounter()
  // Sending active message to the network
  let isActive = false
  while (!isActive) {
    await Data.sendActiveRequest()

    // TODO not used for now
    // isActive = await Data.checkActiveStatus()

    // Set as true for now, This needs to be removed after the active record for the archiver is added on the validator side
    isActive = true
  }
  Data.subscribeNodeForDataTransfer()

  // Sync the missing data during the cycle of sending active request
  const latestCycle = await Cycles.getNewestCycleFromArchivers()
  await Data.syncCyclesAndTxsDataBetweenCycles(beforeCycle - 1, latestCycle.counter + 1)
}

// Define all endpoints, all requests, and start REST server
async function startServer(): Promise<void> {
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
  await server.register(healthCheckRouter)

  server.addContentTypeParser('application/json', { parseAs: 'string' }, (req, body, done) => {
    try {
      const jsonString = typeof body === 'string' ? body : body.toString('utf8')
      done(null, StringUtils.safeJsonParse(jsonString))
    } catch (err) {
      err.statusCode = 400
      done(err, undefined)
    }
  })

  server.setReplySerializer((payload) => {
    return StringUtils.safeStringify(payload)
  })

  initProfiler(server)

  // Initialize the data log writer
  if (config.dataLogWrite) await initDataLogWriter()

  // Register API routes
  registerRoutes(server as FastifyInstance<Server, IncomingMessage, ServerResponse>)

  // Start server and bind to port on all interfaces
  server.listen(
    {
      port: config.ARCHIVER_PORT,
      host: '0.0.0.0',
    },
    (err) => {
      Logger.mainLogger.debug('Listening', config.ARCHIVER_PORT)
      if (err) {
        server.log.error(err)
        process.exit(1)
      }
      Logger.mainLogger.debug('Archive-server has started.')
      State.setActive()
      Collector.scheduleMissingTxsDataQuery()
    }
  )
}

const spawnWorker = (): Worker => {
  const worker: Worker = cluster.fork()
  workerProcessMap.set(worker.process.pid, worker)
  registerWorkerMessageListener(worker)
  return worker
}

export const registerWorkerMessageListener = (worker: Worker): void => {
  worker.on('message', (message: any) => {
    console.log(`Master received message from worker ${worker.process.pid}: ${message}`)
  })
}

// Creating a worker pool
export const workers: Worker[] = []

if (cluster.isPrimary) {
  console.log(`Master ${process.pid} is running`)

  for (let i = 0; i < numCPUs; i++) {
    const worker = spawnWorker()
    console.log(`⛏️ Worker ${worker.process.pid} started`)
  }

  // Optional: Handle worker exit
  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`)
    workerProcessMap.delete(worker.process.pid)
    // Respawn the worker
    const newWorker = spawnWorker()
    console.log(`⛏️ New Worker ${newWorker.process.pid} started`)
  })

  start()
} else {
  console.log(`Worker ${process.pid} started`)
  // Worker processes
  process.on('message', (receipt: any) => {
    console.log(`Worker ${process.pid} validating receipt`)
    // Add receipt validation logic here
    const verified = true
    process.send({ workerId: process.pid, success: verified })
  })
}
