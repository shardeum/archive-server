import { Server, IncomingMessage, ServerResponse } from 'http'
import { EventEmitter } from 'events'
import fastify = require('fastify')
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import {
  Cycle,
  currentCycleCounter,
  currentCycleDuration,
  processCycles,
} from './Cycles'
import { Transaction } from './Transactions'
import { StateHashes, processStateHashes } from './State'
import { ReceiptHashes, processReceiptHashes } from './Receipt'
import { SummaryHashes, processSummaryHashes } from './Summary'

// Socket modules
let socketServer: SocketIO.Server
let ioclient: SocketIOClientStatic = require('socket.io-client')
let socketClient: SocketIOClientStatic["Socket"]

export interface StateMetaData {
  counter: Cycle['counter']
  stateHashes: StateHashes[],
  receiptHashes: ReceiptHashes[],
  summaryHashes: SummaryHashes[]
}
// Data types

export type ValidTypes = Cycle | StateMetaData

export enum TypeNames {
  CYCLE = 'CYCLE',
  STATE_METADATA = 'STATE_METADATA'
}

interface NamesToTypes {
  CYCLE: Cycle
  STATE_METADATA: StateMetaData
}

export type TypeName<T extends ValidTypes> = T extends Cycle
  ? TypeNames.CYCLE
  : TypeNames.STATE_METADATA

export type TypeIndex<T extends ValidTypes> = T extends Cycle
  ? Cycle['counter']
  : StateMetaData['counter']

// Data network messages

export interface DataRequest<T extends ValidTypes> {
  type: TypeName<T>
  lastData: TypeIndex<T>
}

interface DataResponse<T extends ValidTypes> {
  type: TypeName<T>
  data: T[]
}

interface DataKeepAlive {
  keepAlive: boolean
}

export function initSocketServer(io: SocketIO.Server) {
  socketServer = io
  socketServer.on('connection', (socket: SocketIO.Socket) => {
    console.log('Explorer has connected')
  })
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo) {
  console.log(node)
  socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)

  socketClient.on('connect', () => {
    console.log('Connection to consensus node was made')
  })

  socketClient.on('DATA', (newData: any) => {
    console.log('DATA from consensor', newData)
    socketServer.emit('DATA', newData)

    const sender = dataSenders.get(newData.publicKey)

    // If publicKey is not in dataSenders, dont keepAlive, END
    if (!sender) {
      console.log('NO SENDER')
      return
    }

    // If unexpected data type from sender, dont keepAlive, END
    const newDataTypes = Object.keys(newData.responses)
    for (const type of newDataTypes as (keyof typeof TypeNames)[]) {
      if (sender.types.includes(type) === false) {
        console.log(
          `NEW DATA type ${type} not included in sender's types: ${JSON.stringify(
            sender.types
          )}`
        )
        return
      }
    }

    // If tag is invalid, dont keepAlive, END
    if (Crypto.authenticate(newData) === false) {
      console.log('Invalid tag')
      return
    }

    setImmediate(processData, newData)
  })
}

export function createDataRequest<T extends ValidTypes>(
  type: TypeName<T>,
  lastData: TypeIndex<T>,
  recipientPk: Crypto.types.publicKey
) {
  return Crypto.tag<DataRequest<T>>(
    {
      type,
      lastData,
    },
    recipientPk
  )
}
// Vars to track Data senders

interface DataSender {
  nodeInfo: NodeList.ConsensusNodeInfo
  types: (keyof typeof TypeNames)[]
  contactTimeout?: NodeJS.Timeout
}

export const dataSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  DataSender
> = new Map()

const timeoutPadding = 1000

export const emitter = new EventEmitter()

function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  console.log(`replaceDataSender: replacing ${publicKey}`)

  // Remove old dataSender
  const removedSenders = removeDataSenders(publicKey)
  if (removedSenders.length < 1) {
    throw new Error('replaceDataSender failed: old sender not removed')
  }
  // console.log(
  //   `replaceDataSender: removed old sender ${JSON.stringify(
  //     removedSenders,
  //     null,
  //     2
  //   )}`
  // )

  // Pick a new dataSender
  const newSenderInfo = selectNewDataSender()
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [TypeNames.CYCLE, TypeNames.STATE_METADATA],
    contactTimeout: createContactTimeout(newSenderInfo.publicKey),
  }
  // console.log(
  //   `replaceDataSender: selected new sender ${JSON.stringify(
  //     newSender.nodeInfo,
  //     null,
  //     2
  //   )}`
  // )

  // Add new dataSender to dataSenders
  addDataSenders(newSender)
  console.log(
    `replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`
  )

  // Send dataRequest to new dataSender
  const dataRequest = {
    dataRequestCycle: createDataRequest<Cycle>(
      TypeNames.CYCLE,
      currentCycleCounter,
      publicKey
    ),
    dataRequestState: createDataRequest<StateMetaData>(
      TypeNames.STATE_METADATA,
      currentCycleCounter,
      publicKey
    )
  }

  sendDataRequest(newSender, dataRequest)

  // console.log(
  //   `replaceDataSender: sent dataRequest to new sender: ${JSON.stringify(
  //     dataRequest,
  //     null,
  //     2
  //   )}`
  // )
}

/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
function createContactTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  const ms = currentCycleDuration + timeoutPadding
  const contactTimeout = setTimeout(replaceDataSender, ms, publicKey)
  // console.log(
  //   `createContactTimeout: created timeout for ${publicKey} in ${ms} ms...`
  // )
  return contactTimeout
}

export function addDataSenders(...senders: DataSender[]) {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
  }
}

function removeDataSenders(
  ...publicKeys: Array<NodeList.ConsensusNodeInfo['publicKey']>
) {
  // console.log(`Removing data sender ${JSON.stringify([...publicKeys])}`)
  const removedSenders = []
  for (const key of publicKeys) {
    const sender = dataSenders.get(key)
    if (sender) {
      // Clear contactTimeout associated with this sender
      if (sender.contactTimeout) {
        clearTimeout(sender.contactTimeout)
      }

      // Record which sender was removed
      removedSenders.push(sender)

      // Delete sender from dataSenders
      dataSenders.delete(key)
    }
  }
  return removedSenders
}

function selectNewDataSender() {
  // Randomly pick an active node
  const activeList = NodeList.getActiveList()
  const newSender = activeList[Math.floor(Math.random() * activeList.length)]
  initSocketClient(newSender)
  return newSender
}

function sendDataRequest(
  sender: DataSender,
  dataRequest: any
) {
  // TODO: crypto.tag cannot handle array type. To change something else
  const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey)
  emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest)
}

function processData(newData: DataResponse<ValidTypes> & Crypto.TaggedMessage) {
  console.log('processing data')
  // Get sender entry
  const sender = dataSenders.get(newData.publicKey)

  // If no sender entry, remove publicKey from senders, END
  if (!sender) {
    console.log('No sender found')
    removeDataSenders(newData.publicKey)
    return
  }

  // Clear senders contactTimeout, if it has one
  if (sender.contactTimeout) {
    clearTimeout(sender.contactTimeout)
  }

  const newDataTypes = Object.keys(newData.responses)
  for (const type of newDataTypes as (keyof typeof TypeNames)[]) {

    // Process data depending on type
    switch (type) {
      case TypeNames.CYCLE: {
        // Process cycles
        processCycles(newData.responses.CYCLE as Cycle[])
        break
      }
      case TypeNames.STATE_METADATA: {
        let stateMetaData: StateMetaData = newData.responses.STATE_METADATA[0]
        console.log('Received MetaData', stateMetaData)
        // [TODO] validate the state data by robust querying other nodes
        processStateHashes(stateMetaData.stateHashes as StateHashes[])
        processSummaryHashes(stateMetaData.summaryHashes as SummaryHashes[])
        processReceiptHashes(stateMetaData.receiptHashes as ReceiptHashes[])
        break
      }
      default: {
        // If data type not recognized, remove sender from dataSenders
        console.log('Unknow data type detected', type)
        removeDataSenders(newData.publicKey)
      }
    }
  }
  // Set new contactTimeout for sender
  if (currentCycleDuration > 0) {
    sender.contactTimeout = createContactTimeout(sender.nodeInfo.publicKey)
  }
}

// Data endpoints

export const routePostNewdata: fastify.RouteOptions<
  Server,
  IncomingMessage,
  ServerResponse,
  fastify.DefaultQuery,
  fastify.DefaultParams,
  fastify.DefaultHeaders,
  DataResponse<ValidTypes> & Crypto.TaggedMessage
> = {
  method: 'POST',
  url: '/newdata',
  // [TODO] Compile json-schemas from types and add for validation + performance
  handler: (request, reply) => {
    const newData = request.body

    // console.log('GOT NEWDATA', JSON.stringify(newData))

    const resp = { keepAlive: true } as DataKeepAlive

    const sender = dataSenders.get(newData.publicKey)

    // If publicKey is not in dataSenders, dont keepAlive, END
    if (!sender) {
      resp.keepAlive = false
      Crypto.tag(resp, newData.publicKey)
      reply.send(resp)
      console.log('NO SENDER')
      return
    }

    // If unexpected data type from sender, dont keepAlive, END
    const newDataTypes = Object.keys(newData.responses)
    for (const type of newDataTypes as (keyof typeof TypeNames)[]) {
      if (sender.types.includes(type) === false) {
        resp.keepAlive = false
        Crypto.tag(resp, newData.publicKey)
        reply.send(resp)
        console.log(
          `NEW DATA type ${type} not included in sender's types: ${JSON.stringify(
            sender.types
          )}`
        )
        return
      }
    }

    // If tag is invalid, dont keepAlive, END
    if (Crypto.authenticate(newData) === false) {
      resp.keepAlive = false
      Crypto.tag(resp, newData.publicKey)
      reply.send(resp)
      return
    }

    // Reply with keepAlive and schedule data processing after I/O
    Crypto.tag(resp, newData.publicKey)
    reply.send(resp)
    setImmediate(processData, newData)
  },
}
