import { Signature } from '@shardus/crypto-utils'
import { FastifyInstance, FastifyRequest } from 'fastify'
import { Server, IncomingMessage, ServerResponse } from 'http'
import { config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as NodeList from './NodeList'
import * as P2P from './P2P'
import * as Storage from './archivedCycle/Storage'
import * as Data from './Data/Data'
import * as Cycles from './Data/Cycles'
import * as Utils from './Utils'
import { addHashesGossip } from './archivedCycle/Gossip'
import * as Logger from './Logger'
import { P2P as P2PTypes } from '@shardus/types'
import { Readable } from 'stream'
import { nestedCountersInstance } from './profiler/nestedCounters'
import { profilerInstance } from './profiler/profiler'
import * as CycleDB from './dbstore/cycles'
import * as AccountDB from './dbstore/accounts'
import * as TransactionDB from './dbstore/transactions'
import * as ReceiptDB from './dbstore/receipts'
import * as OriginalTxDB from './dbstore/originalTxsData'
import * as Collector from './Data/Collector'
import * as GossipData from './Data/GossipData'
import * as AccountDataProvider from './Data/AccountDataProvider'
import { getGlobalNetworkAccount } from './GlobalAccount'
import { cycleRecordWithShutDownMode } from './Data/Cycles'
import { isDebugMiddleware } from './DebugMode'
const { version } = require('../package.json') // eslint-disable-line @typescript-eslint/no-var-requires

const TXID_LENGTH = 64
const {
  MAX_CYCLES_PER_REQUEST,
  MAX_ORIGINAL_TXS_PER_REQUEST,
  MAX_RECEIPTS_PER_REQUEST,
  MAX_ACCOUNTS_PER_REQUEST,
  MAX_BETWEEN_CYCLES_PER_REQUEST,
} = config.REQUEST_LIMIT

let reachabilityAllowed = true

export function registerRoutes(server: FastifyInstance<Server, IncomingMessage, ServerResponse>): void {
  type Request = FastifyRequest<{
    Body: {
      sender: string
      sign: Signature
    }
  }>

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

    if (State.isFirst && NodeList.isEmpty() && !NodeList.foundFirstNode) {
      try {
        const isSignatureValid = Crypto.verify(signedFirstNodeInfo)
        if (!isSignatureValid) {
          Logger.mainLogger.error('Invalid signature', signedFirstNodeInfo)
          return
        }
      } catch (e) {
        Logger.mainLogger.error(e)
      }
      NodeList.toggleFirstNode()
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
      NodeList.addNodes(NodeList.NodeStatus.SYNCING, [firstNode])

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
        const data = {
          nodeList: NodeList.getList(),
        }
        if (cycleRecordWithShutDownMode) {
          // For restore network to start the network from the 'restart' mode
          data['restartCycleRecord'] = cycleRecordWithShutDownMode
          data['dataRequestCycle'] = cycleRecordWithShutDownMode.counter
        } else {
          // For new network to start the network from the 'forming' mode
          data['joinRequest'] = P2P.createArchiverJoinRequest()
          data['dataRequestCycle'] = Cycles.getCurrentCycleCounter()
        }

        res = Crypto.sign<P2P.FirstNodeResponse>(data)
      } else {
        res = Crypto.sign<P2P.FirstNodeResponse>({
          nodeList: NodeList.getList(),
          joinRequest: P2P.createArchiverJoinRequest(),
          dataRequestCycle: Data.createDataRequest<P2PTypes.CycleCreatorTypes.CycleRecord>(
            P2PTypes.SnapshotTypes.TypeNames.CYCLE,
            Cycles.getCurrentCycleCounter(),
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
      const res = NodeList.getCachedNodeList()
      reply.send(res)
    }
    profilerInstance.profileSectionEnd('POST_nodelist')
  })

  server.get('/nodelist', (_request, reply) => {
    profilerInstance.profileSectionStart('GET_nodelist')
    nestedCountersInstance.countEvent('consensor', 'GET_nodelist')

    const res = NodeList.getCachedNodeList()
    reply.send(res)
    profilerInstance.profileSectionEnd('GET_nodelist')
  })

  type FullNodeListRequest = FastifyRequest<{
    Querystring: {
      activeOnly: 'true' | 'false'
      syncingOnly: 'true' | 'false'
      standbyOnly: 'true' | 'false'
    }
  }>

  server.get('/full-nodelist', (_request: FullNodeListRequest, reply) => {
    profilerInstance.profileSectionStart('FULL_nodelist')
    nestedCountersInstance.countEvent('consensor', 'FULL_nodelist')
    const query = _request.query
    let activeOnly = false
    let syncingOnly = false
    let standbyOnly = false
    if (query.activeOnly === 'true') activeOnly = true
    if (query.syncingOnly === 'true') syncingOnly = true
    if (query.standbyOnly === 'true') standbyOnly = true
    const res = NodeList.getCachedFullNodeList(activeOnly, syncingOnly, standbyOnly)
    reply.send(res)
    profilerInstance.profileSectionEnd('FULL_nodelist')
  })

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
      reply.send(Crypto.sign({ removedAndApopedNodes: Cycles.removedAndApopedNodes }))
      profilerInstance.profileSectionEnd('removed')
    }
  )

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

  server.get('/nodeInfo', (request, reply) => {
    if (reachabilityAllowed) {
      reply.send({
        publicKey: config.ARCHIVER_PUBLIC_KEY,
        ip: config.ARCHIVER_IP,
        port: config.ARCHIVER_PORT,
        version,
        time: Date.now(),
      })
    } else {
      request.raw.socket.destroy()
    }
  })

  type CycleInfoRequest = FastifyRequest<{
    Body: {
      start: number
      end: number
      count: number
      download: boolean
    }
  }>

  server.post('/cycleinfo', async (_request: CycleInfoRequest & Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(
      requestData,
      {
        start: 'n?',
        end: 'n?',
        count: 'n?',
        download: 'b?',
        sender: 's',
        sign: 'o',
      },
      true
    )
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const { start, end, count, download } = _request.body
    if (download !== undefined && typeof download !== 'boolean') {
      reply.send({ success: false, error: `Invalid download flag` })
      return
    }
    const isDownload: boolean = download === true
    let cycleInfo = []
    if (count) {
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_CYCLES_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_CYCLES_PER_REQUEST}.` })
        return
      }
      cycleInfo = await CycleDB.queryLatestCycleRecords(count)
    } else if (start || end) {
      const from = start ? start : 0
      const to = end ? end : from + 100
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        Logger.mainLogger.error(`Invalid start and end counters`)
        reply.send({ success: false, error: `Invalid start and end counters` })
        return
      }
      // Limit the number of cycles to 100
      const cycleCount = to - from
      if (cycleCount > MAX_CYCLES_PER_REQUEST) {
        Logger.mainLogger.error(`Exceed maximum limit of ${MAX_CYCLES_PER_REQUEST} cycles`)
        reply.send({ success: false, error: `Exceed maximum limit of ${MAX_CYCLES_PER_REQUEST} cycles` })
        return
      }
      cycleInfo = await CycleDB.queryCycleRecordsBetween(from, to)
      if (isDownload) {
        const dataInBuffer = Buffer.from(JSON.stringify(cycleInfo), 'utf-8')
        const dataInStream = Readable.from(dataInBuffer)
        const filename = `cycle_records_from_${from}_to_${to}`

        reply.headers({
          'content-disposition': `attachment; filename="${filename}"`,
          'content-type': 'application/octet-stream',
        })
        reply.send(dataInStream)
        return
      }
    } else {
      reply.send({
        success: false,
        error: 'not specified which cycle to show',
      })
      return
    }
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  type CycleInfoCountRequest = FastifyRequest<{
    Params: { count: string }
  }>

  server.get('/cycleinfo/:count', async (_request: CycleInfoCountRequest, reply) => {
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
    if (count > MAX_CYCLES_PER_REQUEST) count = MAX_CYCLES_PER_REQUEST
    const cycleInfo = await CycleDB.queryLatestCycleRecords(count)
    const res = Crypto.sign({
      cycleInfo,
    })
    reply.send(res)
  })

  type ReceiptRequest = FastifyRequest<{
    Body: {
      count: number
      start: number
      end: number
      startCycle: number
      endCycle: number
      type: string
      page: number
      txId: string
      txIdList: string[]
    }
  }>

  server.post('/originalTx', async (_request: ReceiptRequest & Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(requestData, {
      count: 'n?',
      start: 'n?',
      end: 'n?',
      startCycle: 'n?',
      endCycle: 'n?',
      type: 's?',
      page: 'n?',
      txId: 's?',
      txIdList: 'a?',
      sender: 's',
      sign: 'o',
    })
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const { count, start, end, startCycle, endCycle, type, page, txId, txIdList } = _request.body
    let originalTxs: (OriginalTxDB.OriginalTxData | OriginalTxDB.OriginalTxDataCount)[] | number = []
    if (count) {
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_ORIGINAL_TXS_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_ORIGINAL_TXS_PER_REQUEST}` })
        return
      }
      originalTxs = await OriginalTxDB.queryLatestOriginalTxs(count)
    } else if (txId) {
      if (txId.length !== TXID_LENGTH) {
        reply.send({
          success: false,
          error: `Invalid txId ${txId}`,
        })
        return
      }
      const originalTx = await OriginalTxDB.queryOriginalTxDataByTxId(txId)
      if (originalTx) originalTxs.push(originalTx)
    } else if (txIdList) {
      for (const [txId, txTimestamp] of txIdList) {
        if (typeof txId !== 'string' || txId.length !== TXID_LENGTH || typeof txTimestamp !== 'number') {
          reply.send({
            success: false,
            error: `Invalid txId ${txId} in the List`,
          })
          return
        }

        const originalTx = await OriginalTxDB.queryOriginalTxDataByTxId(txId, txTimestamp)
        if (originalTx) originalTxs.push(originalTx)
      }
    } else if (start || end) {
      const from = start ? start : 0
      const to = end ? end : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid start and end counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_ORIGINAL_TXS_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_ORIGINAL_TXS_PER_REQUEST} original transactions`,
        })
        return
      }
      originalTxs = await OriginalTxDB.queryOriginalTxsData(from, count + 1)
    } else if (startCycle || endCycle) {
      const from = startCycle ? startCycle : 0
      const to = endCycle ? endCycle : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid startCycle and endCycle counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_BETWEEN_CYCLES_PER_REQUEST} cycles`,
        })
        return
      }
      if (type === 'tally') {
        originalTxs = await OriginalTxDB.queryOriginalTxDataCountByCycles(from, to)
      } else if (type === 'count') {
        originalTxs = await OriginalTxDB.queryOriginalTxDataCount(from, to)
      } else {
        let skip = 0
        const limit = MAX_ORIGINAL_TXS_PER_REQUEST
        if (page) {
          if (page < 1 || Number.isNaN(page)) {
            reply.send({ success: false, error: `Invalid page number` })
            return
          }
          skip = page - 1
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

  server.post('/receipt', async (_request: ReceiptRequest & Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(requestData, {
      count: 'n?',
      start: 'n?',
      end: 'n?',
      startCycle: 'n?',
      endCycle: 'n?',
      type: 's?',
      page: 'n?',
      txId: 's?',
      txIdList: 'a?',
      sender: 's',
      sign: 'o',
    })
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const { count, start, end, startCycle, endCycle, type, page, txId, txIdList } = _request.body
    let receipts: (ReceiptDB.Receipt | ReceiptDB.ReceiptCount)[] | number = []
    if (count) {
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_RECEIPTS_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_RECEIPTS_PER_REQUEST}` })
        return
      }
      receipts = await ReceiptDB.queryLatestReceipts(count)
    } else if (txId) {
      if (txId.length !== TXID_LENGTH) {
        reply.send({
          success: false,
          error: `Invalid txId ${txId}`,
        })
        return
      }
      const receipt = await ReceiptDB.queryReceiptByReceiptId(txId)
      if (receipt) receipts.push(receipt)
    } else if (txIdList) {
      for (const [txId, txTimestamp] of txIdList) {
        if (typeof txId !== 'string' || txId.length !== TXID_LENGTH || typeof txTimestamp !== 'number') {
          reply.send({
            success: false,
            error: `Invalid txId ${txId} in the List`,
          })
          return
        }
        const receipt = await ReceiptDB.queryReceiptByReceiptId(txId, txTimestamp)
        if (receipt) receipts.push(receipt)
      }
    } else if (start || end) {
      const from = start ? start : 0
      const to = end ? end : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid start and end counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_RECEIPTS_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_RECEIPTS_PER_REQUEST} receipts`,
        })
        return
      }
      receipts = await ReceiptDB.queryReceipts(from, count + 1)
    } else if (startCycle || endCycle) {
      const from = startCycle ? startCycle : 0
      const to = endCycle ? endCycle : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid startCycle and endCycle counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_BETWEEN_CYCLES_PER_REQUEST} cycles`,
        })
        return
      }
      if (type === 'tally') {
        receipts = await ReceiptDB.queryReceiptCountByCycles(from, to)
      } else if (type === 'count') {
        receipts = await ReceiptDB.queryReceiptCountBetweenCycles(from, to)
      } else {
        let skip = 0
        const limit = MAX_RECEIPTS_PER_REQUEST
        if (page) {
          if (page < 1 || Number.isNaN(page)) {
            reply.send({ success: false, error: `Invalid page number` })
            return
          }
          skip = page - 1
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

  type AccountRequest = FastifyRequest<{
    Body: {
      count: number
      start: number
      end: number
      startCycle: number
      endCycle: number
      page: number
      accountId: string
    }
  }>

  server.post('/account', async (_request: AccountRequest & Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(requestData, {
      count: 'n?',
      start: 'n?',
      end: 'n?',
      startCycle: 'n?',
      endCycle: 'n?',
      page: 'n?',
      accountId: 's?',
      sender: 's',
      sign: 'o',
    })
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    let accounts: AccountDB.AccountCopy | AccountDB.AccountCopy[] | number = []
    let totalAccounts = 0
    let res
    const { count, start, end, startCycle, endCycle, page, accountId } = _request.body
    if (count) {
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_ACCOUNTS_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_ACCOUNTS_PER_REQUEST}` })
        return
      }
      accounts = await AccountDB.queryLatestAccounts(count)
      res = { accounts }
    } else if (start || end) {
      const from = start ? start : 0
      const to = end ? end : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid start and end counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_ACCOUNTS_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_ACCOUNTS_PER_REQUEST} accounts`,
        })
        return
      }
      accounts = await AccountDB.queryAccounts(from, count + 1)
      res = { accounts }
    } else if (startCycle || endCycle) {
      const from = startCycle ? startCycle : 0
      const to = endCycle ? endCycle : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid startCycle and endCycle counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_BETWEEN_CYCLES_PER_REQUEST} cycles to query accounts Count`,
        })
        return
      }
      totalAccounts = await AccountDB.queryAccountCountBetweenCycles(from, to)
      if (page) {
        if (page < 1 || Number.isNaN(page)) {
          reply.send({ success: false, error: `Invalid page number` })
          return
        }
        let skip = page - 1
        const limit = MAX_ACCOUNTS_PER_REQUEST
        if (skip > 0) skip = skip * limit
        accounts = await AccountDB.queryAccountsBetweenCycles(skip, limit, from, to)
        res = { accounts, totalAccounts }
      } else {
        res = { totalAccounts }
      }
    } else if (accountId) {
      accounts = await AccountDB.queryAccountByAccountId(accountId)
      res = { accounts }
    } else {
      reply.send({
        success: false,
        error: 'not specified which account to show',
      })
      return
    }
    reply.send(Crypto.sign(res))
  })

  type TransactionRequest = FastifyRequest<{
    Body: {
      count: number
      start: number
      end: number
      startCycle: number
      endCycle: number
      txId: string
      page: number
      appReceiptId: string
    }
  }>

  server.post('/transaction', async (_request: TransactionRequest & Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(requestData, {
      count: 'n?',
      start: 'n?',
      end: 'n?',
      txId: 's?',
      appReceiptId: 's?',
      startCycle: 'n?',
      endCycle: 'n?',
      page: 'n?',
      sender: 's',
      sign: 'o',
    })
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const { count, start, end, txId, appReceiptId, startCycle, endCycle, page } = _request.body
    let transactions: TransactionDB.Transaction | TransactionDB.Transaction[] = []
    let totalTransactions = 0
    let res
    if (count) {
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_ACCOUNTS_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_ACCOUNTS_PER_REQUEST}` })
        return
      }
      transactions = await TransactionDB.queryLatestTransactions(count)
      res = { transactions }
    } else if (start || end) {
      const from = start ? start : 0
      const to = end ? end : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid start and end counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_ACCOUNTS_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_ACCOUNTS_PER_REQUEST} transactions`,
        })
        return
      }
      transactions = await TransactionDB.queryTransactions(from, count + 1)
      res = { transactions }
    } else if (startCycle || endCycle) {
      const from = startCycle ? startCycle : 0
      const to = endCycle ? endCycle : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({
          success: false,
          error: `Invalid startCycle and endCycle counters`,
        })
        return
      }
      const count = to - from
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_BETWEEN_CYCLES_PER_REQUEST} cycles to query transactions Count`,
        })
        return
      }
      totalTransactions = await TransactionDB.queryTransactionCountBetweenCycles(from, to)
      if (page) {
        if (page < 1 || Number.isNaN(page)) {
          reply.send({ success: false, error: `Invalid page number` })
          return
        }
        let skip = page - 1
        const limit = MAX_ACCOUNTS_PER_REQUEST
        if (skip > 0) skip = skip * limit
        transactions = await TransactionDB.queryTransactionsBetweenCycles(skip, limit, from, to)
        res = { transactions, totalTransactions }
      } else {
        res = { totalTransactions }
      }
    } else if (txId) {
      transactions = await TransactionDB.queryTransactionByTxId(txId)
      res = { transactions }
    } else if (appReceiptId) {
      transactions = await TransactionDB.queryTransactionByAccountId(appReceiptId)
      res = { transactions }
    } else {
      res = {
        success: false,
        error: 'not specified which transaction to show',
      }
      reply.send(res)
      return
    }
    reply.send(Crypto.sign(res))
  })

  server.post('/totalData', async (_request: Request, reply) => {
    const requestData = _request.body
    const result = validateRequestData(requestData, {
      sender: 's',
      sign: 'o',
    })
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const totalCycles = await CycleDB.queryCyleCount()
    const totalAccounts = await AccountDB.queryAccountCount()
    const totalTransactions = await TransactionDB.queryTransactionCount()
    const totalReceipts = await ReceiptDB.queryReceiptCount()
    const totalOriginalTxs = await OriginalTxDB.queryOriginalTxDataCount()
    reply.send(
      Crypto.sign({ totalCycles, totalAccounts, totalTransactions, totalReceipts, totalOriginalTxs })
    )
  })

  type GossipDataRequest = FastifyRequest<{
    Body: GossipData.GossipData
  }>

  server.post('/gossip-data', async (_request: GossipDataRequest, reply) => {
    const gossipPayload = _request.body
    if (config.VERBOSE) Logger.mainLogger.debug('Gossip Data received', JSON.stringify(gossipPayload))
    const result = Collector.validateGossipData(gossipPayload)
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const res = Crypto.sign({
      success: true,
    })
    reply.send(res)
    Collector.processGossipData(gossipPayload)
  })

  type AccountDataRequest = FastifyRequest<{
    Body: AccountDataProvider.AccountDataRequestSchema | AccountDataProvider.AccountDataByListRequestSchema
  }>

  server.post('/get_account_data_archiver', async (_request: AccountDataRequest, reply) => {
    const payload = _request.body as AccountDataProvider.AccountDataRequestSchema
    if (config.VERBOSE) Logger.mainLogger.debug('Account Data received', JSON.stringify(payload))
    const result = AccountDataProvider.validateAccountDataRequest(payload)
    // Logger.mainLogger.debug('Account Data validation result', result)
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const data = await AccountDataProvider.provideAccountDataRequest(payload)
    // Logger.mainLogger.debug('Account Data result', data)
    const res = Crypto.sign({
      success: true,
      data,
    })
    reply.send(res)
  })

  server.post('/get_account_data_by_list_archiver', async (_request: AccountDataRequest, reply) => {
    const payload = _request.body as AccountDataProvider.AccountDataByListRequestSchema
    if (config.VERBOSE) Logger.mainLogger.debug('Account Data By List received', JSON.stringify(payload))
    const result = AccountDataProvider.validateAccountDataByListRequest(payload)
    // Logger.mainLogger.debug('Account Data By List validation result', result)
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const accountData = await AccountDataProvider.provideAccountDataByListRequest(payload)
    // Logger.mainLogger.debug('Account Data By List result', accountData)
    const res = Crypto.sign({
      success: true,
      accountData,
    })
    reply.send(res)
  })

  server.post('/get_globalaccountreport_archiver', async (_request: AccountDataRequest, reply) => {
    const payload = _request.body as AccountDataProvider.GlobalAccountReportRequestSchema
    if (config.VERBOSE) Logger.mainLogger.debug('Global Account Report received', JSON.stringify(payload))
    const result = AccountDataProvider.validateGlobalAccountReportRequest(payload)
    // Logger.mainLogger.debug('Global Account Report validation result', result)
    if (!result.success) {
      reply.send({ success: false, error: result.error })
      return
    }
    const report = await AccountDataProvider.provideGlobalAccountReportRequest()
    // Logger.mainLogger.debug('Global Account Report result', report)
    const res = Crypto.sign(report)
    reply.send(res)
  })

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
      const data = {
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

  const enableLoseYourself = false // set this to `true` during testing, but never commit as `true`

  server.get(
    '/lose-yourself',
    // this debug endpoint is in support of development and testing of
    // lost archiver detection
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (_request, reply) => {
      if (enableLoseYourself) {
        Logger.mainLogger.debug('/lose-yourself: exit(1)')

        reply.send(Crypto.sign({ status: 'success', message: 'will exit' }))

        // We don't call exitArchiver() here because that awaits Data.sendLeaveRequest(...),
        // but we're simulating a lost node.
        process.exit(1)
      } else {
        Logger.mainLogger.debug('/lose-yourself: not enabled. no action taken.')
        reply.send({ status: 'failure', message: 'not enabled' })
        // set enableLoseYourself to true--but never commit!
      }
    }
  )

  // ping the archiver to see if it's alive
  server.get(
    '/ping',
    // this debug endpoint is in support of development and testing of
    // lost archiver detection
    {
      preHandler: async (_request, reply) => {
        isDebugMiddleware(_request, reply)
      },
    },
    (request, reply) => {
      if (reachabilityAllowed) {
        reply.status(200).send('pong!')
      } else {
        request.raw.socket.destroy()
      }
    }
  )

  server.get(
    '/set-reachability',
    {
      schema: {
        querystring: {
          properties: {
            value: {
              type: 'boolean',
            },
          },
        },
      },
      preHandler: async (request, reply) => {
        isDebugMiddleware(request, reply)
      },
    },
    async (request, reply) => {
      const msg = `/set-reachability`
      console.log(msg)
      Logger.mainLogger.info(msg)
      const value = (request.query as { value: boolean }).value
      if (typeof value !== 'boolean') {
        Logger.mainLogger.info('/set-reachability: value must be a boolean')
        reply.status(400).send('value must be a boolean')
      } else {
        const msg = `/set-reachability: ${value}`
        console.log(msg)
        Logger.mainLogger.info(msg)
        reachabilityAllowed = value
      }
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
      const err = Utils.validateTypes(_request.query, { start: 's', end: 's' })
      if (err) {
        reply.send({ success: false, error: err })
        return
      }
      const { start, end } = _request.query
      const from = start ? parseInt(start) : 0
      const to = end ? parseInt(end) : from
      if (!(from >= 0 && to >= from) || Number.isNaN(from) || Number.isNaN(to)) {
        reply.send({ success: false, error: `Invalid start and end counters` })
        return
      }
      const count = to - from
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({
          success: false,
          error: `Exceed maximum limit of ${MAX_BETWEEN_CYCLES_PER_REQUEST} cycles`,
        })
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
      const err = Utils.validateTypes(_request.params, { count: 's' })
      if (err) {
        reply.send({ success: false, error: err })
        return
      }

      const count: number = parseInt(_request.params.count)
      if (count <= 0 || Number.isNaN(count)) {
        reply.send({ success: false, error: `Invalid count` })
        return
      }
      if (count > MAX_BETWEEN_CYCLES_PER_REQUEST) {
        reply.send({ success: false, error: `Max count is ${MAX_BETWEEN_CYCLES_PER_REQUEST}` })
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
        data: any // eslint-disable-line @typescript-eslint/no-explicit-any
      }
    }>

    server.post('/gossip-hashes', async (_request: GossipHashesRequest, reply) => {
      const gossipMessage = _request.body
      Logger.mainLogger.debug('Gossip received', JSON.stringify(gossipMessage))
      addHashesGossip(gossipMessage.sender, gossipMessage.data)
      const res = Crypto.sign({
        success: true,
      })
      reply.send(res)
    })
  }

  type GetNetworkAccountRequest = FastifyRequest<{
    Querystring: { hash: 'true' | 'false' }
  }>

  server.get('/get-network-account', (_request: GetNetworkAccountRequest, reply) => {
    const useHash = _request.query?.hash !== 'false'

    const response = useHash
      ? { networkAccountHash: getGlobalNetworkAccount(useHash) }
      : { networkAccount: getGlobalNetworkAccount(useHash) }

    // We might want to sign this response
    reply.send(Crypto.sign(response))
  })
}

export const validateRequestData = (
  data: unknown & { sender: string; sign: Signature },
  expectedDataType: Record<string, unknown>,
  skipArchiverCheck = false
): { success: boolean; error?: string } => {
  try {
    let err = Utils.validateTypes(data, expectedDataType)
    if (err) {
      Logger.mainLogger.error('Invalid request data ', err)
      return { success: false, error: 'Invalid request data ' + err }
    }
    err = Utils.validateTypes(data.sign, { owner: 's', sig: 's' })
    if (err) {
      Logger.mainLogger.error('Invalid request data signature ', err)
      return { success: false, error: 'Invalid request data signature ' + err }
    }
    if (data.sign.owner !== data.sender) {
      Logger.mainLogger.error('Data sender publicKey and sign owner key does not match')
      return { success: false, error: 'Data sender publicKey and sign owner key does not match' }
    }
    if (!skipArchiverCheck && config.limitToArchiversOnly) {
      // Check if the sender is in the archiver list or is the devPublicKey
      const approvedSender =
        State.activeArchivers.some((archiver) => archiver.publicKey === data.sender) ||
        config.DevPublicKey === data.sender
      if (!approvedSender) {
        return { success: false, error: 'Data request sender is not an archiver' }
      }
    }
    if (!Crypto.verify(data)) {
      Logger.mainLogger.error('Invalid signature', data)
      return { success: false, error: 'Invalid signature' }
    }
    return { success: true }
  } catch (e) {
    Logger.mainLogger.error('Error validating request data', e)
    return { success: false, error: 'Error validating request data' }
  }
}

export enum RequestDataType {
  CYCLE = 'cycleinfo',
  RECEIPT = 'receipt',
  ORIGINALTX = 'originalTx',
  ACCOUNT = 'account',
  TRANSACTION = 'transaction',
  TOTALDATA = 'totalData',
}

export const queryFromArchivers = async (
  type: RequestDataType,
  queryParameters: object,
  timeoutInSecond?: number
): Promise<unknown | null> => {
  const data = {
    ...queryParameters,
    sender: config.ARCHIVER_PUBLIC_KEY,
  }
  const signedDataToSend = Crypto.sign(data)
  let url
  switch (type) {
    case RequestDataType.CYCLE:
      url = `/cycleinfo`
      break
    case RequestDataType.RECEIPT:
      url = `/receipt`
      break
    case RequestDataType.ORIGINALTX:
      url = `/originalTx`
      break
    case RequestDataType.ACCOUNT:
      url = `/account`
      break
    case RequestDataType.TRANSACTION:
      url = `/transaction`
      break
    case RequestDataType.TOTALDATA:
      url = `/totalData`
      break
  }
  const filteredArchivers = State.activeArchivers.filter(
    (archiver) => archiver.publicKey !== config.ARCHIVER_PUBLIC_KEY
  )
  const maxNumberofArchiversToRetry = 3
  const randomArchivers = Utils.getRandomItemFromArr(filteredArchivers, 0, maxNumberofArchiversToRetry)
  let retry = 0
  while (retry < maxNumberofArchiversToRetry) {
    let randomArchiver = randomArchivers[retry]
    if (!randomArchiver) randomArchiver = randomArchivers[0]
    try {
      const response = await P2P.postJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}${url}`,
        signedDataToSend,
        timeoutInSecond
      )
      if (response) return response
    } catch (e) {
      Logger.mainLogger.error(
        `Error while querying ${randomArchiver.ip}:${randomArchiver.port}${url} for data ${queryParameters}`,
        e
      )
    }
    retry++
  }
  return null
}
