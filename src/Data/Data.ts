import { Server, IncomingMessage, ServerResponse } from 'http'
import { EventEmitter } from 'events'
import fastify = require('fastify')
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Storage from '../Storage'
import * as State from '../State'
import * as P2P from '../P2P'
import {
  Cycle,
  currentCycleCounter,
  currentCycleDuration,
  processCycles,
} from './Cycles'
import { Transaction } from './Transactions'
import { StateHashes, processStateHashes } from './State'
import { ReceiptHashes, processReceiptHashes, getReceiptMapHash } from './Receipt'
import { SummaryHashes, processSummaryHashes, getSummaryHash } from './Summary'

// Socket modules
export let socketServer: SocketIO.Server
let ioclient: SocketIOClientStatic = require('socket.io-client')
let socketClient: SocketIOClientStatic["Socket"]

let verifiedReceiptMapResults: {
  [key: number]: { [key: number]: ReceiptMapResult }
} = {}

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

export type ReceiptMap = {[txId:string] : string[]  }

export type ReceiptMapResult = {
  cycle:number;
  partition:number;
  receiptMap:ReceiptMap;
  txCount:number
}

type OpaqueBlob = any

export type SummaryBlob = {
  latestCycle: number; //The highest cycle that was used in this summary.  
  counter:number; 
  errorNull:number; 
  partition:number; 
  opaqueBlob:OpaqueBlob;
}

//A collection of blobs that share the same cycle.  For TX summaries
type SummaryBlobCollection = {
  cycle:number; 
  blobsByPartition:Map<number, SummaryBlob>;
}

// Stats collected for a cycle
export type StatsClump = {
  error:boolean; 
  cycle:number; 
  dataStats:SummaryBlob[]; 
  txStats:SummaryBlob[]; 
  covered:number[];
  coveredParititionCount:number;
  skippedParitionCount:number; 
}

export interface ReceiptMapQueryResponse {
  success: boolean
  data: { [key: number]: ReceiptMapResult[]}
}
export interface SummaryBlobQueryResponse {
  success: boolean
  data: { [key: number]: SummaryBlob[]}
}
export interface DataQueryResponse {
  success: boolean
  data: any
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

export function createQueryRequest<T extends ValidTypes>(
  type: string,
  lastData: number,
  recipientPk: Crypto.types.publicKey
) {
  return Crypto.tag(
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

function sendDataQuery(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: any
) {
  // TODO: crypto.tag cannot handle array type. To change something else
  const taggedDataQuery = Crypto.tag(dataQuery, consensorNode.publicKey)
  queryDataFromNode(consensorNode, taggedDataQuery)
}

async function processData(newData: DataResponse<ValidTypes> & Crypto.TaggedMessage) {
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

        if (stateMetaData.receiptHashes.length > 0) {
          let activeNodes = NodeList.getActiveList()
          for (let node of activeNodes) {
            const queryRequest = createQueryRequest('RECEIPT_MAP', stateMetaData.receiptHashes[0].counter, node.publicKey)
            sendDataQuery(node, queryRequest)
          }
        }

        // Query receipt maps from other nodes and store it
        if (stateMetaData.summaryHashes.length > 0) {
          let activeNodes = NodeList.getActiveList()
          for (let node of activeNodes) {
            const queryRequest = createQueryRequest('SUMMARY_BLOB', stateMetaData.summaryHashes[0].counter, node.publicKey)
            sendDataQuery(node, queryRequest)
          }
        }
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

async function queryDataFromNode (
  newSenderInfo: NodeList.ConsensusNodeInfo,
  dataQuery: any
) {
  let request = {
    ...dataQuery,
    nodeInfo: State.getNodeInfo(),
  }
  let response = await P2P.postJson(
    `http://${newSenderInfo.ip}:${newSenderInfo.port}/querydata`,
    request
  )
  if (response && request.type === 'RECEIPT_MAP') {
    for (let counter in response.data) {
      validateAndStoreReceiptMaps(response.data)
    }
  } else if (response && request.type === 'SUMMARY_BLOB') {
    for (let counter in response.data) {
      validateAndStoreSummaryBlobs(Object.values(response.data))
    }
  }
}

async function validateAndStoreReceiptMaps (receiptMapResultsForCycles: {
  [key: number]: ReceiptMapResult[]
}) {
  for (let counter in receiptMapResultsForCycles) {
    let receiptMapResults: ReceiptMapResult[] =
      receiptMapResultsForCycles[counter]
    for (let partitionBlock of receiptMapResults) {
      let { partition } = partitionBlock
      let reciptMapHash = await getReceiptMapHash(parseInt(counter), partition)
      let calculatedReceiptMapHash = Crypto.hashObj(partitionBlock)
      if (calculatedReceiptMapHash === reciptMapHash) {
        await Storage.storeReceiptMap(partitionBlock)
      }
    }
  }
}

async function validateAndStoreSummaryBlobs (
  statsClumpForCycles: StatsClump[]
) {
  for (let statsClump of statsClumpForCycles) {

    let { cycle, dataStats, txStats, covered } = statsClump

    console.log('cycle', cycle)
    console.log('length of dataStats', dataStats.length)
    console.log('length of txStats', txStats.length)

    for (let partition of covered) {
      let summaryBlob
      let dataBlob = dataStats.find(d => d.partition === partition)
      let txBlob = txStats.find(t => t.partition === partition)
      let summaryHash = await getSummaryHash(cycle, partition)
      let calculatedSummaryHash = Crypto.hashObj({
        dataStat: dataBlob,
        txStats: txBlob,
      })
      if (summaryHash !== calculatedSummaryHash) return
      if (dataBlob) {
        summaryBlob = {
          ...dataBlob,
        }
      }
      console.log('txBlob', txBlob)
      if (txBlob) {
        if (!summaryBlob) {
          summaryBlob = {
            ...txBlob
          }
        } else if (summaryBlob && txBlob.latestCycle > summaryBlob.latestCycle) {
          summaryBlob.latestCycle = txBlob.latestCycle
          summaryBlob.opaqueBlob = {
            ...summaryBlob.opaqueBlob,
            ...txBlob.opaqueBlob,
          }
        }
      }
      if (summaryBlob) {
        console.log(`Summary blob for partition: ${partition}`, summaryBlob)
        try {
          await Storage.storeSummaryBlob(summaryBlob, cycle)
        } catch (e) {
          console.log('Unable to store summary blob', e)
        }
      }
    }
  }
}
