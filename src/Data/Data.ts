import { Server, IncomingMessage, ServerResponse } from 'http'
import fastify = require('fastify')
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import { postJson } from '../P2P'
import * as Cycles from './Cycles'
import { Transaction } from './Transactions'
import { Partition } from './Partitions'
import * as NewDataJson from './schemas/NewData.json'
import * as DataResponseJson from './schemas/DataResponse.json'

export enum DataTypes {
  CYCLE = 'cycle',
  TRANSACTION = 'transaction',
  PARTITION = 'partition',
  ALL = 'all',
}

// Data network messages

interface NewData extends Crypto.TaggedMessage {
  type: Exclude<DataTypes, DataTypes.ALL>
  data: Cycles.Cycle[] | Transaction[] | Partition[]
}

interface DataRequest extends Crypto.TaggedMessage {
  type: DataTypes
  lastData: Cycles.Cycle['counter'] | Transaction['id'] | Partition['hash']
}

interface DataResponse extends Crypto.TaggedMessage {
  keepAlive: boolean
}

// Constructs to track Data senders

interface DataSender {
  nodeInfo: NodeList.ConsensusNodeInfo
  type: DataTypes
  contactTimeout?: NodeJS.Timeout
}

const dataSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  DataSender
> = new Map()
const timeoutPadding = 5000

/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
function removeOnTimeout(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  return setTimeout(() => {
    removeDataSenders(publicKey)
    selectNewDataSender()
  }, Cycles.currentCycleDuration + timeoutPadding)
}

export function addDataSenders(...senders: DataSender[]) {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
  }
}

function removeDataSenders(
  ...publicKeys: Array<NodeList.ConsensusNodeInfo['publicKey']>
) {
  for (const key of publicKeys) {
    dataSenders.delete(key)
  }
}

function selectNewDataSender() {
  // Randomly pick an active node
  const activeList = NodeList.getActiveList()
  const newSender = activeList[Math.floor(Math.random() * activeList.length)]
  // Add it to dataSenders
  addDataSenders({
    nodeInfo: newSender,
    type: DataTypes.CYCLE,
    contactTimeout: removeOnTimeout(newSender.publicKey),
  })
  //  Send it a DataRequest
  const request: DataRequest = {
    type: DataTypes.CYCLE,
    lastData: Cycles.currentCycleCounter,
  } as DataRequest
  Crypto.tag(request, newSender.publicKey)
  postJson(`http://${newSender.ip}:${newSender.port}/requestdata`, request)
}

// Data endpoints

function processData(newData: NewData) {
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
    case DataTypes.CYCLE: {
      // Process cycles
      Cycles.processCycles(newData.data as Cycles.Cycle[])
      break
    }
    case DataTypes.TRANSACTION: {
      // [TODO] process transactions
      break
    }
    case DataTypes.PARTITION: {
      // [TODO] process partitions
      break
    }
    default: {
      // If data type not recognized, remove sender from dataSenders
      removeDataSenders(newData.publicKey)
    }
  }

  // Set new contactTimeout for sender
  sender.contactTimeout = removeOnTimeout(sender.nodeInfo.publicKey)
}

export const routePostNewdata: fastify.RouteOptions<
  Server,
  IncomingMessage,
  ServerResponse,
  fastify.DefaultQuery,
  fastify.DefaultParams,
  fastify.DefaultHeaders,
  NewData
> = {
  method: 'POST',
  url: '/newdata',
  // Compile json-schemas from types and add for validation + performance
  schema: {
    body: NewDataJson,
    response: DataResponseJson,
  },
  handler: (request, reply) => {
    const newData = request.body
    const resp = { keepAlive: true } as DataResponse

    // If publicKey is not in dataSenders, dont keepAlive, END
    const sender = dataSenders.get(newData.publicKey)
    if (!sender) {
      resp.keepAlive = false
      Crypto.tag(resp, newData.publicKey)
      reply.send(resp)
      return
    }

    // If unexpected data type from sender, dont keepAlive, END
    if (sender.type !== DataTypes.ALL) {
      if (sender.type !== newData.type) {
        resp.keepAlive = false
        Crypto.tag(resp, newData.publicKey)
        reply.send(resp)
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
