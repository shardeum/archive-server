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
import { StateHashes } from './State'

// Data types

export type ValidTypes = Cycle | Transaction | StateHashes

export enum TypeNames {
  CYCLE = 'CYCLE',
  TRANSACTION = 'TRANSACTION',
  STATE = 'STATE',
}

interface NamesToTypes {
  CYCLE: Cycle
  TRANSACTION: Transaction
  STATE: StateHashes
}

export type TypeName<T extends ValidTypes> = T extends Cycle
  ? TypeNames.CYCLE
  : T extends Transaction
  ? TypeNames.TRANSACTION
  : TypeNames.STATE

export type TypeIndex<T extends ValidTypes> = T extends Cycle
  ? Cycle['counter']
  : T extends Transaction
  ? Transaction['id']
  : StateHashes['counter']

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

export function createCombinedDataRequest<T extends ValidTypes>(
  DataRequest: (DataRequest<Cycle> | DataRequest<StateHashes>)[],
  recipientPk: Crypto.types.publicKey
) {
  return Crypto.tag(
    {data: DataRequest},
    recipientPk
  )
}

// Vars to track Data senders

interface DataSender<T extends ValidTypes> {
  nodeInfo: NodeList.ConsensusNodeInfo
  type: (TypeNames.CYCLE | TypeNames.STATE)[]
  contactTimeout?: NodeJS.Timeout
}

export const dataSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  DataSender<ValidTypes>
> = new Map()

const timeoutPadding = 1000

export const emitter = new EventEmitter()

function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  // console.log(`replaceDataSender: replacing ${publicKey}`)

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
  const newSender: DataSender<Cycle> = {
    nodeInfo: newSenderInfo,
    type: [TypeNames.CYCLE, TypeNames.STATE],
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
  // console.log(
  //   `replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`
  // )

  // Send dataRequest to new dataSender
  const dataRequestCycle: DataRequest<Cycle> = {
    type: TypeNames.CYCLE,
    lastData: currentCycleCounter,
  } as DataRequest<Cycle>

  const dataRequestSTATE: DataRequest<StateHashes> = {
    type: TypeNames.STATE,
    lastData: currentCycleCounter,
  } as DataRequest<StateHashes>

  const dataRequest = [dataRequestCycle, dataRequestSTATE]
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

export function addDataSenders(...senders: Array<DataSender<ValidTypes>>) {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
  }
}

function removeDataSenders(
  ...publicKeys: Array<NodeList.ConsensusNodeInfo['publicKey']>
) {
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
  return newSender
}

function sendDataRequest(
  sender: DataSender<Cycle>,
  dataRequest: (DataRequest<Cycle> | DataRequest<StateHashes>)[]
) {
  // TODO: crypto.tag cannot handle array type. To change something else
  const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey)
  emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest)
}

function processData(newData: DataResponse<ValidTypes> & Crypto.TaggedMessage) {
  // Get sender entry
  const sender = dataSenders.get(newData.publicKey)

  // If no sender entry, remove publicKey from senders, END
  if (!sender) {
    removeDataSenders(newData.publicKey)
    return
  }

  // Clear senders contactTimeout, if it has one
  if (sender.contactTimeout) {
    clearTimeout(sender.contactTimeout)
  }

  // Process data depending on type
  switch (newData.type) {
    case TypeNames.CYCLE: {
      // Process cycles
      processCycles(newData.responses.CYCLE as Cycle[])
      break
    }
    case TypeNames.TRANSACTION: {
      // [TODO] process transactions
      break
    }
    case TypeNames.STATE: {
      // [TODO] process state data
      break
    }
    default: {
      // If data type not recognized, remove sender from dataSenders
      removeDataSenders(newData.publicKey)
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

    console.log('GOT NEWDATA')
    console.log(newData)

    const resp = { keepAlive: true } as DataKeepAlive

    // If publicKey is not in dataSenders, dont keepAlive, END
    const sender = dataSenders.get(newData.publicKey)
    if (!sender) {
      resp.keepAlive = false
      Crypto.tag(resp, newData.publicKey)
      reply.send(resp)
      console.log('NO SENDER')
      return
    }

    // If unexpected data type from sender, dont keepAlive, END
    if (newData.type.indexOf(newData.type) === -1) {
      resp.keepAlive = false
      Crypto.tag(resp, newData.publicKey)
      reply.send(resp)
      console.log(`NEW DATA type ${newData.type} not included in sender's type: ${JSON.stringify(sender.type)}`)
      return
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
