import { Server, IncomingMessage, ServerResponse } from 'http'
import { EventEmitter } from 'events'
import * as deepmerge from 'deepmerge'
import fastify = require('fastify')
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Storage from '../Storage'
import * as Cycles from './Cycles'
import * as State from '../State'
import * as P2P from '../P2P'
import * as Utils from '../Utils'
import * as Gossip from './Gossip'
import { isDeepStrictEqual } from 'util'
import { config, Config } from '../Config'

import {
  Cycle,
  currentCycleCounter,
  currentCycleDuration,
  processCycles,
  lastProcessedMetaData,
  validateCycle
} from './Cycles'
import { StateHashes } from './State'
import { ReceiptHashes } from './Receipt'
import { SummaryHashes } from './Summary'
import { BaseModel } from 'tydb'

// Socket modules
export let socketServer: SocketIO.Server
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
type CycleMarker = string

type StateData = {
  parentCycle?: CycleMarker
  networkHash?: string
  partitionHashes?: string[]
}

type Receipt = {
  parentCycle?: CycleMarker
  networkHash?: string
  partitionHashes?: string[]
  partitionMaps?: { [partition: number]: ReceiptMapResult }
  partitionTxs?: { [partition: number]: any }
}

type Summary = {
  parentCycle?: CycleMarker
  networkHash?: string
  partitionHashes?: string[]
  partitionBlobs?: { [partition: number]: SummaryBlob }
}

export class ArchivedCycle extends BaseModel {
  cycleRecord!: Cycle
  cycleMarker!: CycleMarker
  data!: StateData
  receipt!: Receipt
  summary!: Summary
}

export let StateMetaDataMap = new Map()
export let currentDataSender: string = ''

export function initSocketServer(io: SocketIO.Server) {
  socketServer = io
  socketServer.on('connection', (socket: SocketIO.Socket) => {
    console.log('Explorer has connected')
  })
}

export function unsubscribeDataSender() {
  console.log('Disconnecting previous connection')
  socketClient.emit('UNSUBSCRIBE', config.ARCHIVER_PUBLIC_KEY);
  socketClient.disconnect()
  dataSenders.delete(currentDataSender)
  currentDataSender = ''
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo) {
  console.log(node)
  socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)

  socketClient.on('connect', () => {
    console.log('Connection to consensus node was made')
    // Send ehlo event right after connect:
    socketClient.emit('ARCHIVER_PUBLIC_KEY', config.ARCHIVER_PUBLIC_KEY);
  })


  socketClient.on('DATA', (newData: any) => {
    if (!newData || !newData.responses) return
    if(newData.responses.STATE_METADATA.length > 0) console.log('New STATEMETADATA', newData.responses.STATE_METADATA[0].summaryHashes)
    // If tag is invalid, dont keepAlive, END
    if (Crypto.authenticate(newData) === false) {
      console.log('Invalid tag. Data received from archiver cannot be authenticated', newData)
      unsubscribeDataSender()
      return
    }
    currentDataSender = newData.publicKey
    if (newData.responses && newData.responses.STATE_METADATA) {
      // console.log('New DATA from consensor STATE_METADATA', newData.publicKey, newData.responses.STATE_METADATA)
      let hashArray: any = Gossip.convertStateMetadataToHashArray(newData.responses.STATE_METADATA[0])
      for (let stateMetadataHash of hashArray) {
        StateMetaDataMap.set(stateMetadataHash.counter, stateMetadataHash)
        Gossip.sendGossip('hashes', stateMetadataHash)
      }
    }
    
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

export interface DataSender {
  nodeInfo: NodeList.ConsensusNodeInfo
  types: (keyof typeof TypeNames)[]
  contactTimeout?: NodeJS.Timeout | null
  replaceTimeout?: NodeJS.Timeout | null
}

export const dataSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  DataSender
> = new Map()

const timeoutPadding = 1000

export const emitter = new EventEmitter()

export function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  if (NodeList.getActiveList().length < 2) {
    console.log('There is only one active node in the network. Unable to replace data sender')
    let sender = dataSenders.get(publicKey)
    if (sender && sender.replaceTimeout) {
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
      sender.replaceTimeout = createReplaceTimeout(publicKey)
    }
    return
  }
  console.log(`replaceDataSender: replacing ${publicKey}`)

  // Remove old dataSender
  const removedSenders = removeDataSenders(publicKey)
  if (removedSenders.length < 1) {
    // throw new Error('replaceDataSender failed: old sender not removed')
    console.log('replaceDataSender failed: old sender not removed')
    return
  }

  // Pick a new dataSender
  const newSenderInfo = selectNewDataSender()
  if (!newSenderInfo) {
    console.log('Unable to select a new data sender.')
    return
  }
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [TypeNames.CYCLE, TypeNames.STATE_METADATA],
    contactTimeout: createContactTimeout(newSenderInfo.publicKey, "This timeout is created during newSender selection", 2 * currentCycleDuration),
    replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
  }

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
    dataRequestStateMetaData: createDataRequest<StateMetaData>(
      TypeNames.STATE_METADATA,
      lastProcessedMetaData, // TODO: this is a bug
      publicKey
    )
  }
  sendDataRequest(newSender, dataRequest)
}

/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
export function createContactTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey'], msg: string = '', timeout: number = 1 *  currentCycleDuration
) {
  // TODO: check what is the best contact timeout
  const ms = timeout ? timeout : 1 * currentCycleDuration || (1 * 30 * 1000) + timeoutPadding
  const contactTimeout = setTimeout(() => {
    console.log('REPLACING sender due to CONTACT timeout', msg)
    replaceDataSender(publicKey)
  }, ms)

  // console.log(`${new Date()}: Created CONTACT timeout of ${Math.round(ms / 1000)} s for ${publicKey}`)
  // console.log(`${new Date()}: Data sender ${publicKey} is set to be replaced at ${new Date(Date.now() + ms)}`)

  return contactTimeout
}

export function createReplaceTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  const ms = config.DATASENDER_TIMEOUT || 1000 * 60 * 20
  const replaceTimeout = setTimeout(() => {
    console.log('ROTATING sender due to REPLACE timeout')
    replaceDataSender(publicKey)
  }, ms)

  // console.log(`${new Date()}: Created REPLACE timeout of ${Math.round(ms / 1000)} s for ${publicKey}`)
  // console.log(`${new Date()}: Data sender ${publicKey} is set to be replaced at ${new Date(Date.now() + ms)}`)

  return replaceTimeout
}

export function addDataSenders(...senders: DataSender[]) {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
    currentDataSender = sender.nodeInfo.publicKey
  }
}

function removeDataSenders (
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  console.log(`${new Date()}: Removing data sender ${publicKey}`)
  const removedSenders = []
  const sender = dataSenders.get(publicKey)
  if (sender) {
    // Clear contactTimeout associated with this sender
    if (sender.contactTimeout) {
      clearTimeout(sender.contactTimeout)
      sender.contactTimeout = null
    }
    if (sender.replaceTimeout) {
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
    }

    // Record which sender was removed
    removedSenders.push(sender)

    // Delete sender from dataSenders
    dataSenders.delete(publicKey)
  } else {
    console.log('Unable to find sender in the list', dataSenders)
  }
  return removedSenders
}


function selectNewDataSender() {
  // Randomly pick an active node
  const activeList = NodeList.getActiveList()
  const newSender = activeList[Math.floor(Math.random() * activeList.length)]
  console.log('New data sender is selected', newSender)
  if(newSender) {
    unsubscribeDataSender()
    initSocketClient(newSender)
  }
  return newSender
}

export function sendDataRequest(
  sender: DataSender,
  dataRequest: any
) {
  // TODO: crypto.tag cannot handle array type. To change something else
  const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey)
  emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest)
}

export async function sendJoinRequest (
  nodeInfo: NodeList.ConsensusNodeInfo,
  cycle: Cycles.Cycle
) {
  let joinRequest = P2P.createArchiverJoinRequest()
  let nextQ1Start = cycle.start * 1000 + cycle.duration * 1000
  let nextQ2Start = nextQ1Start + cycle.duration * 1000 * 0.25
  let now = Date.now()

  console.log('nextQ1Start', nextQ1Start, new Date(nextQ1Start))
  console.log('nextQ2Start', nextQ2Start, new Date(nextQ2Start))
  console.log('Now', now, new Date(now))

  return new Promise((resolve: any) => {
    if (nextQ1Start > now) {
      let waitTime = nextQ1Start - now
      console.log('waitTime', waitTime, `${waitTime / 1000} sec`)

      if (waitTime > 0) {
        setTimeout(() => {
          emitter.emit('submitJoinRequest', nodeInfo, joinRequest)
          resolve(true)
        }, waitTime)
      }
    } else if (now > nextQ1Start && now < nextQ2Start) {
      console.log(
        'Now is within first quarter of cycle. Immediately submitting join request'
      )
      emitter.emit('submitJoinRequest', nodeInfo, joinRequest)
      resolve(true)
    } else {
      let waitTime = nextQ1Start + (cycle.duration * 1000) - now
      console.log('waitTime', waitTime, `${waitTime / 1000} sec`)
      setTimeout(() => {
        emitter.emit('submitJoinRequest', nodeInfo, joinRequest)
        resolve(true)
      }, waitTime)
    }
  })
}

export function sendLeaveRequest (
  nodeInfo: NodeList.ConsensusNodeInfo,
  cycle: Cycles.Cycle
) {

  let leaveRequest = P2P.createArchiverLeaveRequest()
  console.log('Emitting submitLeaveRequest event')
  emitter.emit('submitLeaveRequest', nodeInfo, leaveRequest)
  return true
}


export async function getCycleDuration () {
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`)
  if (response && response.cycleInfo) {
    return response.cycleInfo[0].duration
  }
}

export async function getNewestCycleRecord (nodeInfo: NodeList.ConsensusNodeInfo) {
  if (!nodeInfo) return
  let response: any = await P2P.getJson(
    `http://${nodeInfo.ip}:${nodeInfo.port}/sync-newest-cycle`)
  if (response && response.newestCycle) {
    return response.newestCycle
  }
}

export function checkJoinStatus (cycleDuration: number): Promise<boolean> {
  console.log('Checking join status')
  if (!cycleDuration) {
    console.log('No cycle duration provided')
    throw new Error('No cycle duration provided')
  }
  const ourNodeInfo = State.getNodeInfo()
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)

  return new Promise(async resolve => {
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`
    )
    try {
      if (
        response &&
        response.cycleInfo[0] &&
        response.cycleInfo[0].joinedArchivers
      ) {
        let joinedArchivers = response.cycleInfo[0].joinedArchivers
        let refreshedArchivers = response.cycleInfo[0].refreshedArchivers
        console.log('cycle counter', response.cycleInfo[0].counter)
        console.log('Joined archivers', joinedArchivers)

        let isJoind = [...joinedArchivers, ...refreshedArchivers].find(
          (a: any) => a.publicKey === ourNodeInfo.publicKey
        )
        console.log('isJoind', isJoind)
        resolve(isJoind)
      } else {
        resolve(false)
      }
    } catch (e) {
      console.log(e)
      resolve(false)
    }
  })
}

async function sendDataQuery(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: any
) {
  // TODO: crypto.tag cannot handle array type. To change something else
  const taggedDataQuery = Crypto.tag(dataQuery, consensorNode.publicKey)
  let isSuccess = await queryDataFromNode(consensorNode, taggedDataQuery)
  return isSuccess
}

async function processData(newData: DataResponse<ValidTypes> & Crypto.TaggedMessage) {
  // Get sender entry
  const sender = dataSenders.get(newData.publicKey)

  // If no sender entry, remove publicKey from senders, END
  if (!sender) {
    console.log('No sender found for this data')
    return
  }

  if (sender.nodeInfo.publicKey !== currentDataSender) {
    console.log(`Sender ${sender.nodeInfo.publicKey} is not current data sender.`)
  }

  // Clear senders contactTimeout, if it has one
  if (sender.contactTimeout) {
    clearTimeout(sender.contactTimeout)
    sender.contactTimeout = null
  }

  const newDataTypes = Object.keys(newData.responses)
  for (const type of newDataTypes as (keyof typeof TypeNames)[]) {

    // Process data depending on type
    switch (type) {
      case TypeNames.CYCLE: {
        console.log('Processing CYCLE data')
        processCycles(newData.responses.CYCLE as Cycle[])
        if (newData.responses.CYCLE.length > 0) {
          for (let cycle of newData.responses.CYCLE) {
            let archivedCycle: any = {}
            archivedCycle.cycleRecord = cycle
            archivedCycle.cycleMarker = cycle.marker
            Cycles.CycleChain.set(cycle.counter, cycle)
            await Storage.insertArchivedCycle(archivedCycle)
          }
        } else {
          console.log('Recieved empty newData.responses.CYCLE', newData.responses)
        }
        break
      }
      case TypeNames.STATE_METADATA: {
        console.log('Processing STATE_METADATA')
        processStateMetaData(newData.responses.STATE_METADATA)
        break
      }
      default: {
        // If data type not recognized, remove sender from dataSenders
        console.log('Unknow data type detected', type)
        removeDataSenders(newData.publicKey)
      }
    }
  }

  // Set new contactTimeout for sender. Postpone sender removal because data is still received from consensor
  if (currentCycleDuration > 0) {
    sender.contactTimeout = createContactTimeout(sender.nodeInfo.publicKey, "This timeout is created after processing data")
  }
}

export async function processStateMetaData (STATE_METADATA: any) {
  if (!processStateMetaData) {
    console.log(
      'Invalid STATE_METADATA provided to processStateMetaData function',
      STATE_METADATA
    )
    return
  }
  for (let stateMetaData of STATE_METADATA) {
    let data, receipt, summary
    // [TODO] validate the state data by robust querying other nodes

    // store state hashes to archivedCycle
    stateMetaData.stateHashes.forEach(async (stateHashesForCycle: any) => {
      let parentCycle = Cycles.CycleChain.get(stateHashesForCycle.counter)
      if (!parentCycle) {
        console.log(
          'Unable to find parent cycle for cycle',
          stateHashesForCycle.counter
        )
        return
      }
      data = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: stateHashesForCycle.networkHash,
        partitionHashes: stateHashesForCycle.partitionHashes,
      }
      await Storage.updateArchivedCycle(data.parentCycle, 'data', data)
      Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)
    })

    // store receipt hashes to archivedCycle
    stateMetaData.receiptHashes.forEach(async (receiptHashesForCycle: any) => {
      let parentCycle = Cycles.CycleChain.get(receiptHashesForCycle.counter)
      if (!parentCycle) {
        console.log(
          'Unable to find parent cycle for cycle',
          receiptHashesForCycle.counter
        )
        return
      }
      receipt = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: receiptHashesForCycle.networkReceiptHash,
        partitionHashes: receiptHashesForCycle.receiptMapHashes,
        partitionMaps: {},
        partitionTxs: {},
      }
      await Storage.updateArchivedCycle(receipt.parentCycle, 'receipt', receipt)
      Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)

      // Query receipt maps from other nodes and store it
      if (receiptHashesForCycle.receiptMapHashes) {
        let querySuccess = false
        while(!querySuccess) {
          let randomConsensor = NodeList.getRandomActiveNode()
          const queryRequest = createQueryRequest(
            'RECEIPT_MAP',
            receiptHashesForCycle.counter - 1,
            randomConsensor.publicKey
          )
          querySuccess = await sendDataQuery(randomConsensor, queryRequest)
          if (querySuccess) {
            console.log('Data query for receipt map is completed')
          }
        }
      }
    })

    // store summary hashes to archivedCycle
    stateMetaData.summaryHashes.forEach(async (summaryHashesForCycle: any) => {
      let parentCycle = Cycles.CycleChain.get(summaryHashesForCycle.counter)
      if (!parentCycle) {
        console.log(
          'Unable to find parent cycle for cycle',
          summaryHashesForCycle.counter
        )
        return
      }
      summary = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: summaryHashesForCycle.networkSummaryHash,
        partitionHashes: summaryHashesForCycle.summaryHashes,
        partitionBlobs: {},
      }
      await Storage.updateArchivedCycle(summary.parentCycle, 'summary', summary)
      Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)

      // Query summary blobs from other nodes and store it
      let activeNodes = NodeList.getActiveList()
      for (let node of activeNodes) {
        const queryRequest = createQueryRequest(
          'SUMMARY_BLOB',
          summaryHashesForCycle.counter,
          node.publicKey
        )
        let isSuccess = await sendDataQuery(node, queryRequest)
        if (isSuccess) {
          console.log('Data query for summary blob is completed')
          break
        }
      }
    })
  }
}


export async function fetchStateHashes (archivers: any) {
  function _isSameStateHashes (info1: any, info2: any ) {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(
      `http://${node.ip}:${node.port}/statehashes`
    )
    return response.stateHashes
  }
  const stateHashes:any = await Utils.robustQuery(
    archivers,
    queryFn,
    _isSameStateHashes
  )
  return stateHashes[0]
}

export async function fetchCycleRecords(activeArchivers: State.ArchiverNodeInfo[], start:number, end: number): Promise<any> {
  function isSameCyceInfo (info1: any, info2: any) {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(
      `http://${node.ip}:${node.port}/cycleinfo?start=${start}&end=${end}`
    )
    return response.cycleInfo
  }
  const { result } = await Utils.sequentialQuery(activeArchivers, queryFn)
  return result
}

export async function getNewestCycle(activeArchivers: State.ArchiverNodeInfo[]): Promise<any> {
  function isSameCyceInfo (info1: any, info2: any) {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(
      `http://${node.ip}:${node.port}/cycleinfo/1`
    )
    return response.cycleInfo
  }
  let cycleInfo: any = await Utils.robustQuery(
    activeArchivers,
    queryFn,
    isSameCyceInfo
  )
  return cycleInfo[0]
}

export function activeNodeCount(cycle: Cycle) {
  return (
    cycle.active +
    cycle.activated.length -
    cycle.apoptosized.length -
    cycle.removed.length -
    cycle.lost.length
  )
}

export function totalNodeCount(cycle: Cycle) {
  return (
    cycle.syncing +
    cycle.joinedConsensors.length +
    cycle.active +
    //    cycle.activated.length -      // don't count activated because it was already counted in syncing
    cycle.apoptosized.length -
    cycle.removed.length -
    cycle.lost.length
  )
}

export interface JoinedConsensor extends NodeList.ConsensusNodeInfo {
  cycleJoined: string
  counterRefreshed: number
  id: string
}

export enum NodeStatus {
  ACTIVE = 'active',
  SYNCING = 'syncing',
  REMOVED = 'removed',
}

export interface Node extends JoinedConsensor {
  curvePublicKey: string
  status: NodeStatus
}


type OptionalExceptFor<T, TRequired extends keyof T> = Partial<T> &
  Pick<T, TRequired>

export type Update = OptionalExceptFor<Node, 'id'>

export interface Change {
  added: JoinedConsensor[] // order joinRequestTimestamp [OLD, ..., NEW]
  removed: Array<string> // order doesn't matter
  updated: Update[] // order doesn't matter
}

export function reversed<T>(thing: Iterable<T>) {
  const arr = Array.isArray(thing) ? thing : Array.from(thing)
  let i = arr.length - 1
  const reverseIterator = {
    next: () => {
      const done = i < 0
      const value = done ? undefined : arr[i]
      i--
      return { value, done }
    },
  }
  return {
    [Symbol.iterator]: () => reverseIterator,
  }
}

export class ChangeSquasher {
  final: Change
  removedIds: Set<Node['id']>
  seenUpdates: Map<Update['id'], Update>
  addedIds: Set<Node['id']>
  constructor () {
    this.final = {
      added: [],
      removed: [],
      updated: [],
    }
    this.addedIds = new Set()
    this.removedIds = new Set()
    this.seenUpdates = new Map()
  }

  addChange (change: Change) {
    for (const id of change.removed) {
      // Ignore if id is already removed
      if (this.removedIds.has(id)) continue
      // Mark this id as removed
      this.removedIds.add(id)
    }

    for (const update of change.updated) {
      // Ignore if update.id is already removed
      if (this.removedIds.has(update.id)) continue
      // Mark this id as updated
      this.seenUpdates.set(update.id, update)
    }

    for (const joinedConsensor of reversed(change.added)) {
      // Ignore if it's already been added
      if (this.addedIds.has(joinedConsensor.id)) continue

      // Ignore if joinedConsensor.id is already removed
      if (this.removedIds.has(joinedConsensor.id)) {
        continue
      }
      // Check if this id has updates
      const update = this.seenUpdates.get(joinedConsensor.id)
      if (update) {
        // If so, put them into final.updated
        this.final.updated.unshift(update)
        this.seenUpdates.delete(joinedConsensor.id)
      }
      // Add joinedConsensor to final.added
      this.final.added.unshift(joinedConsensor)
      // Mark this id as added
      this.addedIds.add(joinedConsensor.id)
    }
  }
}

export function parseRecord (record: any): Change {
  // For all nodes described by activated, make an update to change their status to active
  const activated = record.activated.map((id: string) => ({
    id,
    activeTimestamp: record.start,
    status: NodeStatus.ACTIVE,
  }))

  const refreshAdded: Change['added'] = []
  const refreshUpdated: Change['updated'] = []
  for (const refreshed of record.refreshedConsensors) {
    // const node = NodeList.nodes.get(refreshed.id)
    const node = NodeList.getNodeInfoById(refreshed.id) as JoinedConsensor
    if (node) {
      // If it's in our node list, we update its counterRefreshed
      // (IMPORTANT: update counterRefreshed only if its greater than ours)
      if (record.counter > node.counterRefreshed) {
        refreshUpdated.push({
          id: refreshed.id,
          counterRefreshed: record.counter,
        })
      }
    } else {
      // If it's not in our node list, we add it...
      refreshAdded.push(refreshed)
      // and immediately update its status to ACTIVE
      // (IMPORTANT: update counterRefreshed to the records counter)
      refreshUpdated.push({
        id: refreshed.id,
        status: NodeStatus.ACTIVE,
        counterRefreshed: record.counter,
      })
    }
  }

  return {
    added: [...record.joinedConsensors],
    removed: [...record.apoptosized],
    updated: [...activated, refreshUpdated],
  }
}

export function parse (record: any): Change {
  const changes = parseRecord(record)
  // const mergedChange = deepmerge.all<Change>(changes)
  // return mergedChange
  return changes
}

function applyNodeListChange(change: Change) {
  if (change.added.length > 0) {
    const consensorInfos = change.added.map((jc: any) => ({
      ip: jc.externalIp,
      port: jc.externalPort,
      publicKey: jc.publicKey,
      id: jc.id,
    }))

    NodeList.addNodes(NodeList.Statuses.ACTIVE, change.added[0].cycleJoined, consensorInfos)
  }
  if (change.removed.length > 0) {
    NodeList.removeNodes(change.removed)
  }
}

export async function syncCyclesAndNodeList (activeArchivers: State.ArchiverNodeInfo[]) {
  // Get the networks newest cycle as the anchor point for sync
  console.log('Getting newest cycle...')
  const [cycleToSyncTo] = await getNewestCycle(activeArchivers)
  console.log('cycleToSyncTo', cycleToSyncTo)
  console.log(`Syncing till cycle ${cycleToSyncTo.counter}...`)
  const cyclesToGet = 2 * Math.floor(Math.sqrt(cycleToSyncTo.active)) + 2
  console.log(`Cycles to get is ${cyclesToGet}`)

  let CycleChain = []
  const squasher = new ChangeSquasher()

  CycleChain.unshift(cycleToSyncTo)
  squasher.addChange(parse(CycleChain[0]))

  do {
    // Get prevCycles from the network
    const end: number = CycleChain[0].counter - 1
    const start: number = end - cyclesToGet
    console.log(`Getting cycles ${start} - ${end}...`)
    const prevCycles = await fetchCycleRecords(activeArchivers, start, end)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')

    // Add prevCycles to our cycle chain
    let prepended = 0
    for (const prevCycle of prevCycles) {
      // Stop prepending prevCycles if one of them is invalid
      if (validateCycle(prevCycle, CycleChain[0]) === false) {
        console.log(`Record ${prevCycle.counter} failed validation`)
        break
      }
      // Prepend the cycle to our cycle chain
      CycleChain.unshift(prevCycle)
      squasher.addChange(parse(prevCycle))
      prepended++

      if (
        squasher.final.updated.length >= activeNodeCount(cycleToSyncTo) &&
        squasher.final.added.length >= totalNodeCount(cycleToSyncTo)
      ) {
        break
      }
    }

    console.log(
      `Got ${
        squasher.final.updated.length
      } active nodes, need ${activeNodeCount(cycleToSyncTo)}`
    )
    console.log(
      `Got ${squasher.final.added.length} total nodes, need ${totalNodeCount(
        cycleToSyncTo
      )}`
    )
    if (squasher.final.added.length < totalNodeCount(cycleToSyncTo))
      console.log(
        'Short on nodes. Need to get more cycles. Cycle:' +
          cycleToSyncTo.counter
      )

    // If you weren't able to prepend any of the prevCycles, start over
    if (prepended < 1) throw new Error('Unable to prepend any previous cycles')
  } while (
    squasher.final.updated.length < activeNodeCount(cycleToSyncTo) ||
    squasher.final.added.length < totalNodeCount(cycleToSyncTo)
  )

  applyNodeListChange(squasher.final)
  console.log('NodeList after sync', NodeList.getActiveList())

  for (let i = 0; i < CycleChain.length; i++) {
    let record = CycleChain[i]
    console.log('Inserting archived cycle for counter', record.counter)
    Cycles.CycleChain.set(record.counter, {...record})
    const archivedCycle = createArchivedCycle(record)
    await Storage.insertArchivedCycle(archivedCycle)
    Cycles.setCurrentCycleCounter(record.counter)
  }
  console.log('Cycle chain is synced. Size of CycleChain', Cycles.CycleChain.size)
  return true
}

function createArchivedCycle(cycleRecord: Cycle) {
  let archivedCycle: any = {
    cycleRecord: cycleRecord,
    cycleMarker: cycleRecord.marker,
    data: {},
    receipt: {},
    summary: {}
  }
  return archivedCycle
}

async function downloadArchivedCycles(archiver: State.ArchiverNodeInfo) {
  let response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/full-archive`)
  if (response && response.archivedCycles) {
    return response.archivedCycles
  }
}

export async function syncStateMetaData (activeArchivers: State.ArchiverNodeInfo[]) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)
  let downloadedArchivedCycles = await downloadArchivedCycles(randomArchiver)
  let allCycleRecords = await Storage.queryAllCycleRecords()
  let networkReceiptHashesFromRecords = new Map()
  let networkDataHashesFromRecords = new Map()
  let networkSummaryHashesFromRecords = new Map()

  allCycleRecords.forEach((cycleRecord: any) => {
    if (cycleRecord.networkReceiptHash.length > 0) {
      cycleRecord.networkReceiptHash.forEach((hash: any) => {
        networkReceiptHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
    if (cycleRecord.networkDataHash.length > 0) {
      cycleRecord.networkDataHash.forEach((hash: any) => {
        networkDataHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
    if (cycleRecord.networkSummaryHash.length > 0) {
      cycleRecord.networkSummaryHash.forEach((hash: any) => {
        networkSummaryHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
  })

  for (let i = 0; i < allCycleRecords.length; i++) {
    let marker = allCycleRecords[i].marker
    let counter = allCycleRecords[i].counter
    let foundArchiveCycle = downloadedArchivedCycles.find(
      (archive: any) => archive.cycleMarker === marker
    )

    if (!foundArchiveCycle) {
      console.log('Unable to download archivedCycle for counter', counter)
      return
    }

    let isDataSynced = false
    let isReceiptSynced = false
    let isSummarySynced = false
  
    // Check and store data hashes
    if (foundArchiveCycle.data) {
      let downloadedNetworkDataHash = foundArchiveCycle.data.networkHash
      if (downloadedNetworkDataHash === networkDataHashesFromRecords.get(counter)) {
        await Storage.updateArchivedCycle(marker, 'data', foundArchiveCycle.data)
        isDataSynced = true
      }
    } else {
      console.log(`ArchivedCycle ${foundArchiveCycle.cycleRecord.counter}, ${foundArchiveCycle.cycleMarker} does not have data field`)
    }

    // Check and store receipt hashes + receiptMap
    if (foundArchiveCycle.receipt) {
      // TODO: calcuate the network hash by hashing downloaded receipt Map instead of using downloadedNetworkReceiptHash
      let downloadedNetworkReceiptHash = foundArchiveCycle.receipt.networkHash
      if (downloadedNetworkReceiptHash === networkReceiptHashesFromRecords.get(counter)) {
        await Storage.updateArchivedCycle(marker, 'receipt', foundArchiveCycle.receipt)
        isReceiptSynced = true
      }
    } else {
      console.log(`ArchivedCycle ${foundArchiveCycle.cycleRecord.counter}, ${foundArchiveCycle.cycleMarker} does not have receipt field`)
    }

    // Check and store summary hashes
    if (foundArchiveCycle.summary) {
      // TODO: calcuate the network hash by hashing downloaded summary Blobs instead of using downloadedNetworkSummaryHash
      let downloadedNetworkSummaryHash = foundArchiveCycle.summary.networkHash
      if (downloadedNetworkSummaryHash === networkSummaryHashesFromRecords.get(counter)) {
        await Storage.updateArchivedCycle(marker, 'summary', foundArchiveCycle.summary)
        isSummarySynced = true
      }
    } else {
      console.log(`ArchivedCycle ${foundArchiveCycle.cycleRecord.counter}, ${foundArchiveCycle.cycleMarker} does not have summary field`)
    }
    if (isDataSynced && isReceiptSynced && isSummarySynced) {
      console.log(`Successfully synced statemetadata for counter ${counter}`)
      if(counter > Cycles.lastProcessedMetaData) Cycles.setLastProcessedMetaDataCounter(counter)
    }
  }
}

async function queryDataFromNode (
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: any
) {
  let request = {
    ...dataQuery,
    nodeInfo: State.getNodeInfo(),
  }
  try {
    let response = await P2P.postJson(
      `http://${consensorNode.ip}:${consensorNode.port}/querydata`,
      request
    )
    if (response && request.type === 'RECEIPT_MAP') {
      for (let counter in response.data) {
        await validateAndStoreReceiptMaps(response.data)
      }
    } else if (response && request.type === 'SUMMARY_BLOB') {
      for (let counter in response.data) {
        await validateAndStoreSummaryBlobs(Object.values(response.data))
      }
    }
    return true
  } catch(e) {
    console.log(e)
    console.log(`Unable to query complete querying ${request.type} from node`, consensorNode)
    return false
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
      let reciptMapHash = await Storage.queryReceiptMapHash(parseInt(counter), partition)
      if (!reciptMapHash) {
        console.log(`Unable to find receipt hash for counter ${counter}, partition ${partition}`)
        // throw new Error(`Unable to find receipt hash for counter ${counter}, partition ${partition}`)
        continue
      }
      let calculatedReceiptMapHash = Crypto.hashObj(partitionBlock)
      if (calculatedReceiptMapHash === reciptMapHash) {
        await Storage.updateReceiptMap(partitionBlock)
        socketServer.emit('RECEIPT_MAP', partitionBlock)
      } else {
        console.log('calculatedReceiptMapHash === reciptMapHash', calculatedReceiptMapHash === reciptMapHash)
        throw new Error('Different hash while downloading receipt maps')
      }
      // console.log(`Validated and stored receipt maps for cycle ${counter}, partition ${partition}`, )
    }
  }
}

async function validateAndStoreSummaryBlobs (
  statsClumpForCycles: StatsClump[]
) {
  for (let statsClump of statsClumpForCycles) {
    let { cycle, dataStats, txStats, covered } = statsClump
    let blobsToForward: any = []
    for (let partition of covered) {
      let summaryBlob
      let dataBlob = dataStats.find(d => d.partition === partition)
      let txBlob = txStats.find(t => t.partition === partition)
      let summaryHash = await Storage.querySummaryHash(cycle, partition)
      if (!summaryHash) {
        console.log(`Unable to find summary hash for counter ${cycle}, partition ${partition}`)
        // throw new Error(`Unable to find receipt hash for counter ${cycle}, partition ${partition}`)
        continue
      }
      let calculatedSummaryHash = Crypto.hashObj({
        dataStat: dataBlob,
        txStats: txBlob,
      })
      if (summaryHash !== calculatedSummaryHash) {
        console.log('Summary hash is different from calculatedSummaryHash', summaryHash, calculatedSummaryHash)
        continue
      }
      if (dataBlob) {
        summaryBlob = {
          ...dataBlob,
        }
      }
      if (txBlob) {
        if (!summaryBlob) {
          summaryBlob = {
            ...txBlob
          }
        } else if (summaryBlob) {
          summaryBlob.latestCycle = txBlob.latestCycle
          summaryBlob.opaqueBlob = {
            ...summaryBlob.opaqueBlob,
            ...txBlob.opaqueBlob,
          }
        }
      }
      if (summaryBlob) {
        blobsToForward.push(summaryBlob)
        try {
          await Storage.updateSummaryBlob(summaryBlob, cycle)
        } catch (e) {
          console.log('Unable to store summary blob', e)
          throw new Error('Unable to store summary blob')
        }
      }
    }
    if (blobsToForward.length > 0) {
      socketServer.emit('SUMMARY_BLOB', {blobs: blobsToForward, cycle})
    }
  }
  // console.log("Validated and stored summary blobs", )
}

emitter.on(
  'selectNewDataSender',
  async (
    newSenderInfo: NodeList.ConsensusNodeInfo,
    dataRequest: any
  ) => {
    let request = {
      ...dataRequest,
      nodeInfo: State.getNodeInfo()
    }
    console.log('dataRequest', JSON.stringify(dataRequest))
    // console.log('Sending data request to: ', newSenderInfo.port)
    let response = await P2P.postJson(
      `http://${newSenderInfo.ip}:${newSenderInfo.port}/requestdata`,
      request
    )
    console.log('Data request response:', response)
  }
)

emitter.on(
  'submitJoinRequest',
  async (
    newSenderInfo: NodeList.ConsensusNodeInfo,
    joinRequest: any
  ) => {
    let request = {
      ...joinRequest,
      nodeInfo: State.getNodeInfo()
    }
    console.log('join request', request)
    console.log('Sending join request to: ', newSenderInfo.port)
    let response = await P2P.postJson(
      `http://${newSenderInfo.ip}:${newSenderInfo.port}/joinarchiver`,
      request
    )
    console.log('Join request response:', response)
  }
)

emitter.on(
  'submitLeaveRequest',
  async (
    consensorInfo: NodeList.ConsensusNodeInfo,
    leaveRequest: any
  ) => {
    let request = {
      ...leaveRequest,
      nodeInfo: State.getNodeInfo()
    }
    console.log('Sending leave request to: ', consensorInfo.port)
    let response = await P2P.postJson(
      `http://${consensorInfo.ip}:${consensorInfo.port}/leavingarchivers`,
      request
    )
    console.log('Leave request response:', response)
  }
)
