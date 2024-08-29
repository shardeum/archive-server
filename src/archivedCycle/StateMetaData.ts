import { EventEmitter } from 'events'
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Storage from './Storage'
import * as Cycles from '../Data/Cycles'
import {
  currentCycleDuration,
  lastProcessedMetaData,
  processCycles,
  validateCycle,
  fetchCycleRecords,
  getNewestCycleFromArchivers,
} from '../Data/Cycles'
import {
  ChangeSquasher,
  parse,
  totalNodeCount,
  activeNodeCount,
  applyNodeListChange,
} from '../Data/CycleParser'
import { publicKey } from '@shardus/crypto-utils'

import * as State from '../State'
import * as P2P from '../P2P'
import * as Utils from '../Utils'
import * as Gossip from './Gossip'
import { isDeepStrictEqual } from 'util'
import { config } from '../Config'
import { BaseModel } from 'tydb'
import { P2P as P2PTypes, StateManager } from '@shardus/types'
import * as Logger from '../Logger'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import { profilerInstance } from '../profiler/profiler'
import { Node } from '../ShardFunctions'

// Socket modules
import { Socket } from "socket.io-client";
import {Server} from "http";
export let socketServer: Server
import * as ioclient from 'socket.io-client'
import { Utils as StringUtils } from '@shardus/types'
let socketClient: Socket
export const socketClients: Map<string, Socket> = new Map()
const socketConnectionsTracker: Map<string, string> = new Map()
let lastSentCycleCounterToExplorer = 0

// let processedCounters = {}

// Data network messages

export interface DataRequest<T extends P2PTypes.SnapshotTypes.ValidTypes> {
  type: P2PTypes.SnapshotTypes.TypeName<T>
  lastData: P2PTypes.SnapshotTypes.TypeIndex<T>
}

interface DataResponse<T extends P2PTypes.SnapshotTypes.ValidTypes> {
  type: P2PTypes.SnapshotTypes.TypeName<T>
  data: T[]
}

// interface DataKeepAlive {
//   keepAlive: boolean
// }

export interface ReceiptMapQueryResponse {
  success: boolean
  data: { [key: number]: StateManager.StateManagerTypes.ReceiptMapResult[] }
}
export interface StatsClumpQueryResponse {
  success: boolean
  data: { [key: number]: StateManager.StateManagerTypes.StatsClump }
}
export interface SummaryBlobQueryResponse {
  success: boolean
  data: { [key: number]: StateManager.StateManagerTypes.SummaryBlob[] }
}

export interface DataQueryResponse {
  success: boolean
  data: unknown
}

export class ArchivedCycle extends BaseModel {
  cycleRecord!: P2PTypes.CycleCreatorTypes.CycleData
  cycleMarker!: StateManager.StateMetaDataTypes.CycleMarker
  data!: StateManager.StateMetaDataTypes.StateData
  receipt!: StateManager.StateMetaDataTypes.Receipt
  summary!: StateManager.StateMetaDataTypes.Summary
}

interface RequestType {
  nodeInfo: State.ArchiverNodeInfo
  publicKey: string
  tag: string
  type?: string
}

interface ResponseType {
  STATE_METADATA?: P2PTypes.SnapshotTypes.StateMetaData[]
  [key: string]: unknown
}

interface ValidationAndStorageResult {
  success: boolean
  completed: number[]
  failed: number[]
  covered: number[]
  blobs: unknown
}

interface NodeWithPort extends Node {
  ip: string
  port: number
}

export const StateMetaDataMap = new Map()
export let currentDataSender = ''

export function initSocketServer(io: Server): void {
  socketServer = io
  //`socket: SocketIO.Socket` removed as an argument
  socketServer.on('connection', () => {
    Logger.mainLogger.debug('Explorer has connected')
  })
}

export function unsubscribeDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']): void {
  Logger.mainLogger.debug('Disconnecting previous connection', publicKey)
  if (!socketClient) return
  socketClient.emit('UNSUBSCRIBE', config.ARCHIVER_PUBLIC_KEY)
  socketClient.disconnect()
  if (config.VERBOSE) console.log('Killing the connection to', publicKey)
  removeDataSenders(publicKey)
  currentDataSender = ''
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo): void {
  if (config.VERBOSE) Logger.mainLogger.debug('Node Info to socker connect', node)
  if (config.VERBOSE) console.log('Node Info to socker connect', node)
  const socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)

  socketClient.on('connect', () => {
    Logger.mainLogger.debug('Connection to consensus node was made')
    // Send ehlo event right after connect:
    socketClient.emit('ARCHIVER_PUBLIC_KEY', config.ARCHIVER_PUBLIC_KEY)
  })

  socketClient.once('disconnect', async () => {
    Logger.mainLogger.debug(`Connection request is refused by the consensor node ${node.ip}:${node.port}`)
    console.log(`Connection request is refused by the consensor node ${node.ip}:${node.port}`)
    // socketClients.delete(node.publicKey)
    // await Utils.sleep(3000)
    // if (socketClients.has(node.publicKey)) replaceDataSender(node.publicKey)
    socketConnectionsTracker.set(node.publicKey, 'disconnected')
  })

  socketClient.on('DATA', (data: string) => {
    const newData: DataResponse<P2PTypes.SnapshotTypes.ValidTypes> & Crypto.TaggedMessage =
      StringUtils.safeJsonParse(data)
    if (!newData || !newData.responses) return
    if (newData.recipient !== State.getNodeInfo().publicKey) {
      Logger.mainLogger.debug('This data is not meant for this archiver')
      return
    }

    // If tag is invalid, dont keepAlive, END
    if (Crypto.authenticate(newData) === false) {
      Logger.mainLogger.debug('This data cannot be authenticated')
      console.log('Unsubscribe 1', node.publicKey)
      unsubscribeDataSender(node.publicKey)
      return
    }

    if (newData.responses.STATE_METADATA.length > 0) Logger.mainLogger.debug('New DATA', newData.responses)
    else Logger.mainLogger.debug('State metadata is empty')

    currentDataSender = newData.publicKey
    if (newData.responses && newData.responses.STATE_METADATA) {
      // Logger.mainLogger.debug('New DATA from consensor STATE_METADATA', newData.publicKey, newData.responses.STATE_METADATA)
      // let hashArray: any = Gossip.convertStateMetadataToHashArray(newData.responses.STATE_METADATA[0])
      for (const stateMetadata of newData.responses.STATE_METADATA) {
        StateMetaDataMap.set(stateMetadata.counter, stateMetadata)
        Gossip.sendGossip('hashes', stateMetadata)
      }
    }

    socketServer.emit('DATA', newData)
    const sender = dataSenders.get(newData.publicKey)
    // If publicKey is not in dataSenders, dont keepAlive, END
    if (!sender) {
      Logger.mainLogger.debug('NO SENDER')
      return
    }

    // If unexpected data type from sender, dont keepAlive, END
    const newDataTypes = Object.keys(newData.responses)
    for (const type of newDataTypes as (keyof typeof P2PTypes.SnapshotTypes.TypeNames)[]) {
      if (sender.types.includes(type) === false) {
        Logger.mainLogger.debug(
          `NEW DATA type ${type} not included in sender's types: ${StringUtils.safeStringify(sender.types)}`
        )
        return
      }
    }
    setImmediate(processData, newData)
  })
}

export function createDataRequest<T extends P2PTypes.SnapshotTypes.ValidTypes>(
  type: P2PTypes.SnapshotTypes.TypeName<T>,
  lastData: P2PTypes.SnapshotTypes.TypeIndex<T>,
  recipientPk: publicKey
): DataRequest<T> & Crypto.TaggedMessage {
  return Crypto.tag<DataRequest<T>>(
    {
      type,
      lastData,
    },
    recipientPk
  )
}

export function createQueryRequest(
  type: string,
  lastData: number,
  recipientPk: publicKey
): Crypto.TaggedMessage & { type: string; lastData: number } {
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
  types: (keyof typeof P2PTypes.SnapshotTypes.TypeNames)[]
  contactTimeout?: NodeJS.Timeout | null
  replaceTimeout?: NodeJS.Timeout | null
}

export const dataSenders: Map<NodeList.ConsensusNodeInfo['publicKey'], DataSender> = new Map()

const timeoutPadding = 1000

export const emitter = new EventEmitter()

export async function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']): Promise<void> {
  nestedCountersInstance.countEvent('archiver', 'replace_data_sender')
  if (NodeList.getActiveNodeCount() < 2) {
    Logger.mainLogger.debug('There is only one active node in the network. Unable to replace data sender')
    const sender = dataSenders.get(publicKey)
    if (sender && sender.replaceTimeout) {
      nestedCountersInstance.countEvent('archiver', 'clear_replace_timeout')
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
      sender.replaceTimeout = createReplaceTimeout(publicKey)
    }
    return
  }
  Logger.mainLogger.debug(`replaceDataSender: replacing ${publicKey}`)

  // Remove old dataSender
  const removedSenders = removeDataSenders(publicKey)
  if (removedSenders.length < 1) {
    Logger.mainLogger.debug('replaceDataSender failed: old sender not removed')
  }

  // Pick a new dataSender
  const newSenderInfo = selectNewDataSender(publicKey)
  if (!newSenderInfo) {
    Logger.mainLogger.error('Unable to select a new data sender.')
    return
  }
  Logger.mainLogger.debug('Before sleep', publicKey, newSenderInfo)
  await Utils.sleep(1000) // Wait about 1s to be sure that socket client connection is killed
  initSocketClient(newSenderInfo)
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
    contactTimeout: createContactTimeout(
      newSenderInfo.publicKey,
      'This timeout is created during newSender selection',
      2 * currentCycleDuration
    ),
    replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
  }

  // Add new dataSender to dataSenders
  addDataSenders(newSender)
  Logger.mainLogger.debug(`replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`)

  // Send dataRequest to new dataSender
  const dataRequest = {
    dataRequestCycle: createDataRequest<P2PTypes.CycleCreatorTypes.CycleRecord>(
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      Cycles.getCurrentCycleCounter(),
      publicKey
    ),
    dataRequestStateMetaData: createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
      P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
      lastProcessedMetaData,
      publicKey
    ),
    publicKey: State.getNodeInfo().publicKey,
    nodeInfo: State.getNodeInfo(),
  }
  sendDataRequest(newSender, dataRequest)
}

export async function subscribeRandomNodeForDataTransfer(): Promise<void> {
  let retry = 0
  let nodeSubscribedFail = true
  // Set randomly select a consensor as dataSender
  while (nodeSubscribedFail && retry < 10) {
    const randomConsensor = NodeList.getRandomActiveNodes()[0]
    const connectionStatus = await createDataTransferConnection(randomConsensor)
    if (connectionStatus) nodeSubscribedFail = false
    else retry++
  }
  if (nodeSubscribedFail) {
    Logger.mainLogger.error(
      'The archiver fails to subscribe to any node for data transfer! and exit the network.'
    )
    await State.exitArchiver()
  }
}

/**
 * Sets timeout to current cycle duration + some padding
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
export function createContactTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey'],
  msg = '',
  timeout: number | null = null
): NodeJS.Timeout {
  // TODO: check what is the best contact timeout
  let ms: number
  if (timeout) ms = timeout
  else if (currentCycleDuration > 0) ms = 1.5 * currentCycleDuration + timeoutPadding
  else ms = 1.5 * 60 * 1000 + timeoutPadding
  if (config.experimentalSnapshot) ms = 15 * 1000 // Change contact timeout to 15s for now
  Logger.mainLogger.debug('Created contact timeout: ' + ms)
  nestedCountersInstance.countEvent('archiver', 'contact_timeout_created')
  return setTimeout(() => {
    // Logger.mainLogger.debug('nestedCountersInstance', nestedCountersInstance)
    if (nestedCountersInstance) nestedCountersInstance.countEvent('archiver', 'contact_timeout')
    Logger.mainLogger.debug('REPLACING sender due to CONTACT timeout', msg)
    console.log('REPLACING sender due to CONTACT timeout', msg, publicKey)
    replaceDataSender(publicKey)
  }, ms)
}

export function createReplaceTimeout(publicKey: NodeList.ConsensusNodeInfo['publicKey']): NodeJS.Timeout {
  const ms = config.DATASENDER_TIMEOUT || 1000 * 60 * 60
  return setTimeout(() => {
    nestedCountersInstance.countEvent('archiver', 'replace_timeout')
    Logger.mainLogger.debug('ROTATING sender due to ROTATION timeout')
    replaceDataSender(publicKey)
  }, ms)
}

export function addDataSenders(...senders: DataSender[]): void {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
    currentDataSender = sender.nodeInfo.publicKey
  }
}

function removeDataSenders(publicKey: NodeList.ConsensusNodeInfo['publicKey']): DataSender[] {
  Logger.mainLogger.debug(`${new Date()}: Removing data sender ${publicKey}`)
  const removedSenders = []
  // console.log('removeDataSenders', dataSenders)
  // Logger.mainLogger.debug('removeDataSenders', dataSenders)
  for (const [key, sender] of dataSenders) {
    // if (config.VERBOSE) Logger.mainLogger.debug(publicKey, key)
    if (key === publicKey && sender) {
      // Clear contactTimeout associated with this sender
      if (sender.contactTimeout) {
        clearTimeout(sender.contactTimeout)
        sender.contactTimeout = null
      }
      if (sender.replaceTimeout) {
        clearTimeout(sender.replaceTimeout)
        sender.replaceTimeout = null
      }
      nestedCountersInstance.countEvent('archiver', 'remove_data_sender')

      // Record which sender was removed
      removedSenders.push(sender)

      // Delete sender from dataSenders
      dataSenders.delete(key)
    }
  }

  return removedSenders
}

function selectNewDataSender(publicKey: string): NodeList.ConsensusNodeInfo | undefined {
  // Randomly pick an active node
  const activeList = NodeList.getActiveList()
  const newSender = activeList[Math.floor(Math.random() * activeList.length)]
  Logger.mainLogger.debug('New data sender is selected', newSender)
  if (newSender) {
    unsubscribeDataSender(publicKey)
    // initSocketClient(newSender)
  }
  return newSender
}

export async function createDataTransferConnection(
  newSenderInfo: NodeList.ConsensusNodeInfo
): Promise<boolean> {
  initSocketClient(newSenderInfo)
  let count = 0
  while (!socketClients.has(newSenderInfo.publicKey) && count <= 50) {
    await Utils.sleep(100)
    count++
    if (count === 50) {
      // This means the socket connection to the node is not successful.
      return false
    }
  }
  if (socketConnectionsTracker.get(newSenderInfo.publicKey) === 'disconnected') return false
  await Utils.sleep(200) // Wait 1s before sending data request to be sure if the connection is not refused
  if (socketConnectionsTracker.get(newSenderInfo.publicKey) === 'disconnected') return false
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
    contactTimeout: createContactTimeout(
      newSenderInfo.publicKey,
      'This timeout is created during newSender selection',
      2 * currentCycleDuration
    ),
    replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
  }

  // Add new dataSender to dataSenders
  addDataSenders(newSender)
  Logger.mainLogger.debug(`replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`)

  // Send dataRequest to new dataSender
  const dataRequest = {
    dataRequestCycle: createDataRequest<P2PTypes.CycleCreatorTypes.CycleRecord>(
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      Cycles.getCurrentCycleCounter(),
      State.getNodeInfo().publicKey
    ),
    dataRequestStateMetaData: createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
      P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
      lastProcessedMetaData,
      State.getNodeInfo().publicKey
    ),
    publicKey: State.getNodeInfo().publicKey,
    nodeInfo: State.getNodeInfo(),
  }
  sendDataRequest(newSender, dataRequest)
  return true
}

export function sendDataRequest(sender: DataSender, dataRequest: unknown): void {
  const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey)
  Logger.mainLogger.info('Sending tagged data request to consensor.', sender)
  if (socketClients.has(sender.nodeInfo.publicKey))
    emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest)
}

export async function subscribeMoreConsensors(numbersToSubscribe: number): Promise<void> {
  Logger.mainLogger.info(`Subscribing ${numbersToSubscribe} more consensors for tagged data request`)
  console.log('numbersToSubscribe', numbersToSubscribe)
  for (let i = 0; i < numbersToSubscribe; i++) {
    await Utils.sleep(60000)
    // Pick a new dataSender
    const activeList = NodeList.getActiveList()
    const newSenderInfo = activeList[Math.floor(Math.random() * activeList.length)]
    Logger.mainLogger.debug('New data sender is selected', newSenderInfo)
    if (!newSenderInfo) {
      Logger.mainLogger.error('Unable to select a new data sender.')
      continue
    }
    initSocketClient(newSenderInfo)
    const newSender: DataSender = {
      nodeInfo: newSenderInfo,
      types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
      contactTimeout: createContactTimeout(
        newSenderInfo.publicKey,
        'This timeout is created during newSender selection',
        2 * currentCycleDuration
      ),
      replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
    }

    // Add new dataSender to dataSenders
    addDataSenders(newSender)
    Logger.mainLogger.debug(`replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`)

    // Send dataRequest to new dataSender
    const dataRequest = {
      dataRequestCycle: createDataRequest<P2PTypes.CycleCreatorTypes.CycleRecord>(
        P2PTypes.SnapshotTypes.TypeNames.CYCLE,
        Cycles.getCurrentCycleCounter(),
        State.getNodeInfo().publicKey
      ),
      dataRequestStateMetaData: createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
        P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
        lastProcessedMetaData,
        State.getNodeInfo().publicKey
      ),
      publicKey: State.getNodeInfo().publicKey,
      nodeInfo: State.getNodeInfo(),
    }
    sendDataRequest(newSender, dataRequest)
  }
}

async function sendDataQuery(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: unknown,
  validateFn: (cycle: number, partition: number) => boolean
): Promise<unknown> {
  const taggedDataQuery = Crypto.tag(dataQuery, consensorNode.publicKey)
  const result = await queryDataFromNode(consensorNode, taggedDataQuery, validateFn)
  return result
}

async function processData(
  newData: DataResponse<P2PTypes.SnapshotTypes.ValidTypes> & Crypto.TaggedMessage
): Promise<void> {
  // Get sender entry
  const sender = dataSenders.get(newData.publicKey)

  // If no sender entry, remove publicKey from senders, END
  if (!sender) {
    Logger.mainLogger.error('No sender found for this data')
    return
  }

  if (sender.nodeInfo.publicKey !== currentDataSender) {
    Logger.mainLogger.error(`Sender ${sender.nodeInfo.publicKey} is not current data sender.`)
  }

  // Clear senders contactTimeout, if it has one
  if (sender.contactTimeout) {
    Logger.mainLogger.debug('Clearing contact timeout.')
    clearTimeout(sender.contactTimeout)
    sender.contactTimeout = null
    nestedCountersInstance.countEvent('archiver', 'clear_contact_timeout')
  }

  const newDataTypes = Object.keys(newData.responses)
  for (const type of newDataTypes as (keyof typeof P2PTypes.SnapshotTypes.TypeNames)[]) {
    // Process data depending on type
    switch (type) {
      case P2PTypes.SnapshotTypes.TypeNames.CYCLE: {
        Logger.mainLogger.debug('Processing CYCLE data')
        processCycles(newData.responses.CYCLE as P2PTypes.CycleCreatorTypes.CycleData[])
        // socketServer.emit('ARCHIVED_CYCLE', 'CYCLE')
        if (newData.responses.CYCLE.length > 0) {
          for (const cycle of newData.responses.CYCLE) {
            const archivedCycle: ArchivedCycle = {
              cycleRecord: cycle,
              cycleMarker: cycle.marker,
              data: undefined,
              receipt: undefined,
              summary: undefined,
              _id: undefined,
            }
            archivedCycle.cycleRecord = cycle
            archivedCycle.cycleMarker = cycle.marker
            Cycles.CycleChain.set(cycle.counter, cycle)
            await Storage.insertArchivedCycle(archivedCycle)
          }
        } else {
          Logger.mainLogger.error('Received empty newData.responses.CYCLE', newData.responses)
        }
        break
      }
      case P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA: {
        Logger.mainLogger.debug('Processing STATE_METADATA')
        processStateMetaData(newData.responses)
        break
      }
      default: {
        // If data type not recognized, remove sender from dataSenders
        Logger.mainLogger.error('Unknown data type detected', type)
        removeDataSenders(newData.publicKey)
      }
    }
  }

  // Set new contactTimeout for sender. Postpone sender removal because data is still received from consensor
  if (currentCycleDuration > 0) {
    nestedCountersInstance.countEvent('archiver', 'postpone_contact_timeout')
    sender.contactTimeout = createContactTimeout(
      sender.nodeInfo.publicKey,
      'This timeout is created after processing data'
    )
  }
}

export async function processStateMetaData(response: ResponseType): Promise<void> {
  Logger.mainLogger.error('response', response)
  const STATE_METADATA = response.STATE_METADATA
  if (!STATE_METADATA || STATE_METADATA.length === 0) {
    Logger.mainLogger.error(
      'Invalid STATE_METADATA provided to processStateMetaData function',
      STATE_METADATA
    )
    return
  }
  profilerInstance.profileSectionStart('state_metadata')
  for (const stateMetaData of STATE_METADATA) {
    let data: { parentCycle: string; networkHash?: string; partitionHashes?: object }
    let receipt: {
      parentCycle: string
      networkHash?: string
      partitionHashes?: object
      partitionMaps?: object
      partitionTxs?: object
    }
    let summary: {
      parentCycle: string
      networkHash?: string
      partitionHashes?: object
      partitionBlobs?: object
    }
    // [TODO] validate the state data by robust querying other nodes

    // store state hashes to archivedCycle
    for (const stateHashesForCycle of stateMetaData.stateHashes) {
      const parentCycle = Cycles.CycleChain.get(stateHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error('Unable to find parent cycle for cycle', stateHashesForCycle.counter)
        continue
      }
      data = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: stateHashesForCycle.networkHash,
        partitionHashes: stateHashesForCycle.partitionHashes,
      }
      await Storage.updateArchivedCycle(data.parentCycle, 'data', data)
      // if (!processedCounters[parentCycle.counter]) {
      //   processedCounters[parentCycle.counter] = true
      // }
      // Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)
    }

    // store receipt hashes to archivedCycle
    for (const receiptHashesForCycle of stateMetaData.receiptHashes) {
      const parentCycle = Cycles.CycleChain.get(receiptHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error('Unable to find parent cycle for cycle', receiptHashesForCycle.counter)
        continue
      }
      receipt = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: receiptHashesForCycle.networkReceiptHash,
        partitionHashes: receiptHashesForCycle.receiptMapHashes,
        partitionMaps: {},
        partitionTxs: {},
      }
      await Storage.updateArchivedCycle(receipt.parentCycle, 'receipt', receipt)
      // Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)

      // Query receipt maps from other nodes and store it
      if (receiptHashesForCycle.receiptMapHashes) {
        let isDownloadSuccess = false
        let retry = 0
        let sleepCount = 0
        const failedPartitions = new Map()
        const coveredPartitions = new Map()
        const downloadedReceiptMaps = new Map()

        const shouldProcessReceipt = (cycle: number, partition: number): boolean => {
          if (cycle === receiptHashesForCycle.counter - 1)
            if (failedPartitions.has(partition) || !coveredPartitions.has(partition)) return true
          return false
        }
        const cycleActiveNodesSize =
          parentCycle.active + parentCycle.activated.length - parentCycle.removed.length
        while (!isDownloadSuccess && sleepCount < 20) {
          const randomConsensor = NodeList.getRandomActiveNodes()[0]
          const queryRequest = createQueryRequest(
            'RECEIPT_MAP',
            receiptHashesForCycle.counter - 1,
            randomConsensor.publicKey
          )
          const { success, completed, failed, covered, blobs } = (await sendDataQuery(
            randomConsensor,
            queryRequest,
            shouldProcessReceipt
          )) as {
            success: boolean
            completed: number[]
            failed: number[]
            covered: number[]
            blobs: Record<string, unknown>
          }
          if (success) {
            for (const partition of failed) {
              failedPartitions.set(partition, true)
            }
            for (const partition of completed) {
              if (failedPartitions.has(partition)) failedPartitions.delete(partition)
            }
            for (const partition of covered) {
              coveredPartitions.set(partition, true)
            }
            for (const partition in blobs) {
              // eslint-disable-next-line security/detect-object-injection
              downloadedReceiptMaps.set(partition, blobs[partition])
            }
          }
          isDownloadSuccess = failedPartitions.size === 0 && coveredPartitions.size === cycleActiveNodesSize
          if (isDownloadSuccess) {
            Logger.mainLogger.debug('Data query for receipt map is completed for cycle', parentCycle.counter)
            Logger.mainLogger.debug('Total downloaded receipts', downloadedReceiptMaps.size)
            // if (!processedCounters[receiptHashesForCycle.counter - 1]) {
            //   processedCounters[receiptHashesForCycle.counter - 1] = true
            // }
            sendToExplorer(receiptHashesForCycle.counter - 1)

            // let receiptMapsToForward = []
            // for (let [partition, receiptMap] of downloadedReceiptMaps) {
            //   receiptMapsToForward.push(receiptMap)
            // }
            // receiptMapsToForward = receiptMapsToForward.filter(
            //   (receipt) => receipt.cycle === parentCycle.counter
            // )
            // Logger.mainLogger.debug(
            //   'receiptMapsToForward',
            //   receiptMapsToForward.length
            // )
            // socketServer.emit('RECEIPT_MAP', receiptMapsToForward)
            break
          }
          retry += 1
          if (!isDownloadSuccess && retry >= NodeList.getActiveNodeCount()) {
            Logger.mainLogger.debug(
              'Sleeping for 5 sec before retrying download again for cycle',
              parentCycle.counter
            )
            await Utils.sleep(5000)
            retry = 0
            sleepCount += 1
          }
        }
        if (!isDownloadSuccess) {
          Logger.mainLogger.debug(`Downloading receipt map for cycle ${parentCycle.counter} has failed.`)
          Logger.mainLogger.debug(
            `There are ${failedPartitions.size} failed partitions in cycle ${parentCycle.counter}.`
          )
        }
      }
    }

    // store summary hashes to archivedCycle
    for (const summaryHashesForCycle of stateMetaData.summaryHashes) {
      const parentCycle = Cycles.CycleChain.get(summaryHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error('Unable to find parent cycle for cycle', summaryHashesForCycle.counter)
        continue
      }
      summary = {
        parentCycle: parentCycle ? parentCycle.marker : '',
        networkHash: summaryHashesForCycle.networkSummaryHash,
        partitionHashes: summaryHashesForCycle.summaryHashes,
        partitionBlobs: {},
      }
      await Storage.updateArchivedCycle(summary.parentCycle, 'summary', summary)
      Cycles.setLastProcessedMetaDataCounter(parentCycle.counter)

      // // Query summary blobs from other nodes and store it
      // if (summaryHashesForCycle.summaryHashes) {
      //   let isDownloadSuccess = false
      //   let retry = 0
      //   let sleepCount = 0
      //   let failedPartitions = new Map()
      //   let coveredPartitions = new Map()
      //   let downloadedBlobs = new Map()

      //   let shouldProcessBlob = (cycle: number, partition: number) => {
      //     if (cycle === summaryHashesForCycle.counter - 1)
      //       if (
      //         failedPartitions.has(partition) ||
      //         !coveredPartitions.has(partition)
      //       )
      //         return true
      //     return false
      //   }

      //   while (!isDownloadSuccess && sleepCount < 20) {
      //     let randomConsensor = NodeList.getRandomActiveNode()
      //     const queryRequest = createQueryRequest(
      //       'SUMMARY_BLOB',
      //       summaryHashesForCycle.counter - 1,
      //       randomConsensor.publicKey
      //     )
      //     let { success, completed, failed, covered, blobs } =
      //       await sendDataQuery(
      //         randomConsensor,
      //         queryRequest,
      //         shouldProcessBlob
      //       )
      //     if (success) {
      //       for (let partition of failed) {
      //         failedPartitions.set(partition, true)
      //       }
      //       for (let partition of completed) {
      //         if (failedPartitions.has(partition))
      //           failedPartitions.delete(partition)
      //       }
      //       for (let partition of covered) {
      //         coveredPartitions.set(partition, true)
      //       }
      //       for (let partition in blobs) {
      //         downloadedBlobs.set(partition, blobs[partition])
      //       }
      //     }
      //     isDownloadSuccess =
      //       failedPartitions.size === 0 && coveredPartitions.size === 4096
      //     if (isDownloadSuccess) {
      //       Logger.mainLogger.debug(
      //         'Data query for summary blob is completed for cycle',
      //         parentCycle.counter
      //       )
      //       Logger.mainLogger.debug(
      //         'Total downloaded blobs',
      //         downloadedBlobs.size
      //       )
      //       break
      //     }

      //     retry += 1
      //     if (!isDownloadSuccess && retry >= NodeList.getActiveNodeCount()) {
      //       Logger.mainLogger.debug(
      //         'Sleeping for 5 sec before retrying download again for cycle',
      //         parentCycle.counter
      //       )
      //       await Utils.sleep(5000)
      //       retry = 0
      //       sleepCount += 1
      //     }
      //   }
      //   if (!isDownloadSuccess) {
      //     Logger.mainLogger.debug(
      //       `Downloading summary blob for cycle ${parentCycle.counter} has failed.`
      //     )
      //     Logger.mainLogger.debug(
      //       `There are ${failedPartitions.size} failed partitions in cycle ${parentCycle.counter}.`
      //     )
      //   }
      // }
      // if (!processedCounters[parentCycle.counter]) {
      //   processedCounters[parentCycle.counter] = true
      // }
    }
  }
  // if (socketServer)  {
  //   Logger.mainLogger.debug('Finished processing state metadata...', processedCounters, lastSentCycleCounterToExplorer)
  //   for (let counter of Object.keys(processedCounters)) {
  //     let start = parseInt(counter)
  //     let end = parseInt(counter)
  //     if (lastSentCycleCounterToExplorer === 0 || parseInt(counter) > lastSentCycleCounterToExplorer + 1) {
  //       start = lastSentCycleCounterToExplorer + 1
  //     } else {
  //       continue
  //     }
  //     Logger.mainLogger.debug('start, end', start, end)
  //     Logger.mainLogger.debug(start,end)
  //     let completedArchivedCycle = await Storage.queryAllArchivedCyclesBetween(start, end)
  //     Logger.mainLogger.debug('completedArchivedCycle', completedArchivedCycle.length, completedArchivedCycle)
  //     let signedDataToSend = Crypto.sign({
  //       archivedCycles: completedArchivedCycle,
  //     })
  //     Logger.mainLogger.debug('Sending completed archived_cycle to explorer', signedDataToSend)
  //     lastSentCycleCounterToExplorer = end
  //     if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
  //   }
  // }
  profilerInstance.profileSectionEnd('state_metadata')
}

export async function sendToExplorer(counter: number): Promise<void> {
  if (socketServer) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let completedArchivedCycle: any[]
    if (lastSentCycleCounterToExplorer === 0) {
      completedArchivedCycle = await Storage.queryAllArchivedCyclesBetween(
        lastSentCycleCounterToExplorer,
        counter
      )
      Logger.mainLogger.debug('start, end', lastSentCycleCounterToExplorer, counter)
    } else {
      completedArchivedCycle = await Storage.queryAllArchivedCyclesBetween(counter, counter)
      Logger.mainLogger.debug('start, end', counter, counter)
    }
    const signedDataToSend = Crypto.sign({
      archivedCycles: completedArchivedCycle,
    })
    Logger.mainLogger.debug('Sending completed archived_cycle to explorer', signedDataToSend)
    lastSentCycleCounterToExplorer = counter
    if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
  }
}

export async function fetchStateHashes(archivers: Node[]): Promise<unknown> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function _isSameStateHashes(info1: any, info2: any): boolean {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: NodeWithPort): Promise<unknown[]> => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await P2P.getJson(`http://${node.ip}:${node.port}/statehashes`)
    return response.stateHashes
  }
  const stateHashes = await Utils.robustQuery(archivers, queryFn, _isSameStateHashes)
  return stateHashes.value[0]
}

export async function buildNodeListFromStoredCycle(
  lastStoredCycle: P2PTypes.CycleCreatorTypes.CycleData
): Promise<void> {
  Logger.mainLogger.debug('lastStoredCycle', lastStoredCycle)
  Logger.mainLogger.debug(`Syncing till cycle ${lastStoredCycle.counter}...`)
  const cyclesToGet = 2 * Math.floor(Math.sqrt(lastStoredCycle.active)) + 2
  Logger.mainLogger.debug(`Cycles to get is ${cyclesToGet}`)

  const CycleChain = []
  const squasher = new ChangeSquasher()

  CycleChain.unshift(lastStoredCycle)
  squasher.addChange(parse(CycleChain[0]))

  do {
    // Get prevCycles from the network
    let end: number = CycleChain[0].counter - 1
    let start: number = end - cyclesToGet
    if (start < 0) start = 0
    if (end < start) end = start
    Logger.mainLogger.debug(`Getting cycles ${start} - ${end}...`)
    const prevCycles = await Storage.queryCycleRecordsBetween(start, end)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')

    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    let prepended = 0
    for (const prevCycle of prevCycles) {
      // Prepend the cycle to our cycle chain
      CycleChain.unshift(prevCycle)
      squasher.addChange(parse(prevCycle))
      prepended++

      if (
        squasher.final.updated.length >= activeNodeCount(lastStoredCycle) &&
        squasher.final.added.length >= totalNodeCount(lastStoredCycle)
      ) {
        break
      }
    }

    Logger.mainLogger.debug(
      `Got ${squasher.final.updated.length} active nodes, need ${activeNodeCount(lastStoredCycle)}`
    )
    Logger.mainLogger.debug(
      `Got ${squasher.final.added.length} total nodes, need ${totalNodeCount(lastStoredCycle)}`
    )
    if (squasher.final.added.length < totalNodeCount(lastStoredCycle))
      Logger.mainLogger.debug('Short on nodes. Need to get more cycles. Cycle:' + lastStoredCycle.counter)

    // If you weren't able to prepend any of the prevCycles, start over
    if (prepended < 1) throw new Error('Unable to prepend any previous cycles')
  } while (
    squasher.final.updated.length < activeNodeCount(lastStoredCycle) ||
    squasher.final.added.length < totalNodeCount(lastStoredCycle)
  )

  applyNodeListChange(squasher.final)
  Logger.mainLogger.debug('NodeList after sync', NodeList.getActiveList())
  Cycles.setCurrentCycleCounter(lastStoredCycle.counter)
  Cycles.setCurrentCycleMarker(lastStoredCycle.marker)
  Cycles.setCurrentCycleDuration(lastStoredCycle.duration)
  Logger.mainLogger.debug('Latest cycle after sync', lastStoredCycle.counter)
}

export async function syncCyclesAndNodeList(lastStoredCycleCount = 0): Promise<void> {
  // Get the networks newest cycle as the anchor point for sync
  Logger.mainLogger.debug('Getting newest cycle...')
  const cycleToSyncTo = await getNewestCycleFromArchivers()
  Logger.mainLogger.debug('cycleToSyncTo', cycleToSyncTo)
  Logger.mainLogger.debug(`Syncing till cycle ${cycleToSyncTo.counter}...`)
  const cyclesToGet = 2 * Math.floor(Math.sqrt(cycleToSyncTo.active)) + 2
  Logger.mainLogger.debug(`Cycles to get is ${cyclesToGet}`)

  const CycleChain = []
  const squasher = new ChangeSquasher()

  CycleChain.unshift(cycleToSyncTo)
  squasher.addChange(parse(CycleChain[0]))

  do {
    // Get prevCycles from the network
    let end: number = CycleChain[0].counter - 1
    let start: number = end - cyclesToGet
    if (start < 0) start = 0
    if (end < start) end = start
    Logger.mainLogger.debug(`Getting cycles ${start} - ${end}...`)
    const prevCycles = await fetchCycleRecords(start, end)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')

    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    let prepended = 0
    for (const prevCycle of prevCycles) {
      // Stop prepending prevCycles if one of them is invalid
      if (validateCycle(prevCycle, CycleChain[0]) === false) {
        Logger.mainLogger.error(`Record ${prevCycle.counter} failed validation`)
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

    Logger.mainLogger.debug(
      `Got ${squasher.final.updated.length} active nodes, need ${activeNodeCount(cycleToSyncTo)}`
    )
    Logger.mainLogger.debug(
      `Got ${squasher.final.added.length} total nodes, need ${totalNodeCount(cycleToSyncTo)}`
    )
    if (squasher.final.added.length < totalNodeCount(cycleToSyncTo))
      Logger.mainLogger.debug('Short on nodes. Need to get more cycles. Cycle:' + cycleToSyncTo.counter)

    // If you weren't able to prepend any of the prevCycles, start over
    if (prepended < 1) throw new Error('Unable to prepend any previous cycles')
  } while (
    squasher.final.updated.length < activeNodeCount(cycleToSyncTo) ||
    squasher.final.added.length < totalNodeCount(cycleToSyncTo)
  )

  applyNodeListChange(squasher.final)
  Logger.mainLogger.debug('NodeList after sync', NodeList.getActiveList())

  for (let i = 0; i < CycleChain.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const record = CycleChain[i]
    Cycles.CycleChain.set(record.counter, { ...record })
    Logger.mainLogger.debug('Inserting archived cycle for counter', record.counter)
    const archivedCycle = createArchivedCycle(record)
    await Storage.insertArchivedCycle(archivedCycle)
    Cycles.setCurrentCycleCounter(record.counter)
    Cycles.setCurrentCycleMarker(record.marker)
  }
  Logger.mainLogger.debug('Cycle chain is synced. Size of CycleChain', Cycles.CycleChain.size)

  // Download old cycle Records
  let endCycle = CycleChain[0].counter - 1
  Logger.mainLogger.debug('endCycle counter', endCycle, 'lastStoredCycleCount', lastStoredCycleCount)
  if (endCycle > lastStoredCycleCount) {
    Logger.mainLogger.debug(
      `Downloading old cycles from cycles ${lastStoredCycleCount} to cycle ${endCycle}!`
    )
  }
  let savedCycleRecord = CycleChain[0]
  while (endCycle > lastStoredCycleCount) {
    let nextEnd: number = endCycle - 10000 // Downloading max 1000 cycles each time
    if (nextEnd < 0) nextEnd = 0
    Logger.mainLogger.debug(`Getting cycles ${nextEnd} - ${endCycle} ...`)
    const prevCycles = await fetchCycleRecords(nextEnd, endCycle)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')
    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    // commented out because not currently used
    // const combineCycles = []
    for (const prevCycle of prevCycles) {
      // Stop saving prevCycles if one of them is invalid
      if (validateCycle(prevCycle, savedCycleRecord) === false) {
        Logger.mainLogger.error(`Record ${prevCycle.counter} failed validation`)
        Logger.mainLogger.debug('fail', prevCycle, savedCycleRecord)
        break
      }
      savedCycleRecord = prevCycle
      Logger.mainLogger.debug('Inserting archived cycle for counter', prevCycle.counter)
      const archivedCycle = createArchivedCycle(prevCycle)
      await Storage.insertArchivedCycle(archivedCycle)
    }
    endCycle = nextEnd - 1
  }
}

function createArchivedCycle(cycleRecord: P2PTypes.CycleCreatorTypes.CycleData): ArchivedCycle {
  const archivedCycle: ArchivedCycle = {
    _id: '',
    cycleRecord: cycleRecord,
    cycleMarker: cycleRecord.marker,
    data: {},
    receipt: {},
    summary: {},
  }
  return archivedCycle
}

async function downloadArchivedCycles(
  archiver: State.ArchiverNodeInfo,
  cycleToSyncTo: number,
  startCycle = 0
): Promise<ArchivedCycle[]> {
  let complete = false
  let lastData = startCycle
  let collector: ArchivedCycle[] = []
  let count = 0
  const maxCount = Math.ceil((cycleToSyncTo - startCycle) / 5)
  while (!complete && count < maxCount) {
    Logger.mainLogger.debug(`Downloading archive from cycle ${lastData} to cycle ${lastData + 5}`)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await P2P.getJson(
      `http://${archiver.ip}:${archiver.port}/full-archive?start=${lastData}&end=${lastData + 5}`
    )
    if (response && response.archivedCycles) {
      collector = collector.concat(response.archivedCycles)
      if (response.archivedCycles.length < 5) {
        complete = true
        Logger.mainLogger.debug('Download completed')
      }
    } else {
      Logger.mainLogger.debug('Invalid download response')
    }
    count += 1
    lastData += 5
  }
  Logger.mainLogger.debug(`Downloaded archived cycles`, collector.length)
  return collector
}

export async function compareWithOldCyclesData(
  archiver: State.ArchiverNodeInfo,
  lastCycleCounter = 0
): Promise<{ success: boolean; cycle: number }> {
  let downloadedCycles
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/cycleinfo?start=${lastCycleCounter - 10}&end=${
      lastCycleCounter - 1
    }`
  )
  if (response && response.cycleInfo) {
    downloadedCycles = response.cycleInfo
  } else {
    throw Error(
      `Can't fetch data from cycle ${lastCycleCounter - 10} to cycle ${
        lastCycleCounter - 1
      }  from archiver ${archiver}`
    )
  }
  const oldCycles = await Storage.queryCycleRecordsBetween(lastCycleCounter - 10, lastCycleCounter + 1)
  downloadedCycles.sort((a, b) => (a.counter > b.counter ? 1 : -1))
  oldCycles.sort((a, b) => (a.counter > b.counter ? 1 : -1))
  let success = false
  let cycle = 0
  for (let i = 0; i < downloadedCycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const downloadedCycle = downloadedCycles[i]
    // eslint-disable-next-line security/detect-object-injection
    const oldCycle = oldCycles[i]
    console.log(downloadedCycle, oldCycle)
    if (StringUtils.safeStringify(downloadedCycle) !== StringUtils.safeStringify(oldCycle)) {
      return {
        success,
        cycle,
      }
    }
    success = true
    cycle = downloadedCycle.counter
  }
  return { success, cycle }
}

export async function syncStateMetaData(activeArchivers: State.ArchiverNodeInfo[]): Promise<boolean> {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  const allCycleRecords = await Storage.queryAllCycleRecords()
  const lastCycleCounter = allCycleRecords[0].counter
  const downloadedArchivedCycles = await downloadArchivedCycles(randomArchiver, lastCycleCounter)

  const networkReceiptHashesFromRecords = new Map()
  const networkDataHashesFromRecords = new Map()
  const networkSummaryHashesFromRecords = new Map()

  allCycleRecords.forEach((cycleRecord: P2PTypes.CycleCreatorTypes.CycleRecord) => {
    if (cycleRecord.networkReceiptHash.length > 0) {
      cycleRecord.networkReceiptHash.forEach((hash: P2PTypes.SnapshotTypes.NetworkHash) => {
        networkReceiptHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
    if (cycleRecord.networkDataHash.length > 0) {
      cycleRecord.networkDataHash.forEach((hash: P2PTypes.SnapshotTypes.NetworkHash) => {
        networkDataHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
    if (cycleRecord.networkSummaryHash.length > 0) {
      cycleRecord.networkSummaryHash.forEach((hash: P2PTypes.SnapshotTypes.NetworkHash) => {
        networkSummaryHashesFromRecords.set(hash.cycle, hash.hash)
      })
    }
  })

  for (let i = 0; i < downloadedArchivedCycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const marker = downloadedArchivedCycles[i].cycleRecord.marker
    // eslint-disable-next-line security/detect-object-injection
    const counter = downloadedArchivedCycles[i].cycleRecord.counter
    // eslint-disable-next-line security/detect-object-injection
    const downloadedArchivedCycle = downloadedArchivedCycles[i]

    if (!downloadedArchivedCycle) {
      Logger.mainLogger.debug('Unable to download archivedCycle for counter', counter)
      continue
    }

    let isDataSynced = false
    let isReceiptSynced = false
    let isSummarySynced = false

    // Check and store data hashes
    if (downloadedArchivedCycle.data) {
      const downloadedNetworkDataHash = downloadedArchivedCycle.data.networkHash
      const calculatedDataHash = calculateNetworkHash(downloadedArchivedCycle.data.partitionHashes)
      if (downloadedNetworkDataHash === calculatedDataHash) {
        if (downloadedNetworkDataHash !== networkDataHashesFromRecords.get(counter)) {
          Logger.mainLogger.debug('Different with hash from downloaded Cycle Records', 'state data', counter)
        }
        await Storage.updateArchivedCycle(marker, 'data', downloadedArchivedCycle.data)
        isDataSynced = true
      } else {
        Logger.mainLogger.error('Different network data hash for cycle', counter)
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have data field`
      )
    }

    // Check and store receipt hashes + receiptMap
    if (downloadedArchivedCycle.receipt) {
      // TODO: calcuate the network hash by hashing downloaded receipt Map instead of using downloadedNetworkReceiptHash
      const downloadedNetworkReceiptHash = downloadedArchivedCycle.receipt.networkHash
      const calculatedReceiptHash = calculateNetworkHash(downloadedArchivedCycle.receipt.partitionHashes)
      if (
        downloadedNetworkReceiptHash === networkReceiptHashesFromRecords.get(counter) ||
        downloadedNetworkReceiptHash === calculatedReceiptHash
      ) {
        if (downloadedNetworkReceiptHash !== networkReceiptHashesFromRecords.get(counter)) {
          Logger.mainLogger.debug('Different with hash from downloaded Cycle Records', 'receipt', counter)
        }
        await Storage.updateArchivedCycle(marker, 'receipt', downloadedArchivedCycle.receipt)
        isReceiptSynced = true
      } else {
        Logger.mainLogger.error('Different network receipt hash for cycle', counter)
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have receipt field`
      )
    }

    // Check and store summary hashes
    if (downloadedArchivedCycle.summary) {
      // TODO: calcuate the network hash by hashing downloaded summary Blobs instead of using downloadedNetworkSummaryHash
      const downloadedNetworkSummaryHash = downloadedArchivedCycle.summary.networkHash
      const calculatedSummaryHash = calculateNetworkHash(downloadedArchivedCycle.summary.partitionHashes)
      if (downloadedNetworkSummaryHash === calculatedSummaryHash) {
        if (downloadedNetworkSummaryHash !== networkSummaryHashesFromRecords.get(counter)) {
          Logger.mainLogger.debug('Different with hash from downloaded Cycle Records', 'summary', counter)
        }
        await Storage.updateArchivedCycle(marker, 'summary', downloadedArchivedCycle.summary)
        isSummarySynced = true
      } else {
        Logger.mainLogger.error('Different network summary hash for cycle', counter)
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have summary field`
      )
    }
    if (isDataSynced && isReceiptSynced && isSummarySynced) {
      Logger.mainLogger.debug(`Successfully synced statemetadata for counter ${counter}`)
      if (counter > Cycles.lastProcessedMetaData) {
        Cycles.setLastProcessedMetaDataCounter(counter)
      }
    }
  }
  return false
}

export const calculateNetworkHash = (data: object): string => {
  let hashArray = []
  if (data) {
    for (const hash of Object.values(data)) {
      hashArray.push(hash)
    }
  }
  hashArray = hashArray.sort()
  const calculatedHash = Crypto.hashObj(hashArray)
  return calculatedHash
}

export type QueryDataResponse = ReceiptMapQueryResponse | StatsClumpQueryResponse

async function queryDataFromNode(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: ReturnType<typeof Crypto.tag>,
  validateFn: (cycle: number, partition: number) => boolean
): Promise<ValidationAndStorageResult> {
  const request: RequestType = {
    ...dataQuery,
    nodeInfo: State.getNodeInfo(),
  }
  let result: ValidationAndStorageResult = {
    success: false,
    completed: [],
    failed: [],
    covered: [],
    blobs: {},
  }
  try {
    const response = (await P2P.postJson(
      `http://${consensorNode.ip}:${consensorNode.port}/querydata`,
      request
    )) as QueryDataResponse
    if (response && request.type === 'RECEIPT_MAP') {
      const receiptMapData = response.data as ReceiptMapQueryResponse['data']
      // for (let counter in response.data) {
      result = await validateAndStoreReceiptMaps(receiptMapData, validateFn)
      // }
    } else if (response && request.type === 'SUMMARY_BLOB') {
      // for (let counter in response.data) {
      const summaryBlobData = response.data as StatsClumpQueryResponse['data']
      result = await validateAndStoreSummaryBlobs(Object.values(summaryBlobData), validateFn)
      // }
    }
    return result
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(`Unable to query complete querying ${request.type} from node`, consensorNode)
    return result
  }
}

async function validateAndStoreReceiptMaps(
  receiptMapResultsForCycles: {
    [key: number]: StateManager.StateManagerTypes.ReceiptMapResult[]
  },
  validateFn: (cycle: number, partition: number) => boolean
): Promise<{
  success: boolean
  completed: number[]
  failed: number[]
  covered: number[]
  blobs: unknown
}> {
  const completed: number[] = []
  const failed: number[] = []
  const coveredPartitions: number[] = []
  const receiptMaps: unknown = {}
  for (const counter in receiptMapResultsForCycles) {
    const receiptMapResults: StateManager.StateManagerTypes.ReceiptMapResult[] =
      // eslint-disable-next-line security/detect-object-injection
      receiptMapResultsForCycles[counter]
    for (const receiptMap of receiptMapResults) {
      const { cycle, partition } = receiptMap
      if (validateFn) {
        const shouldProcess = validateFn(cycle, partition)
        if (!shouldProcess) {
          continue
        }
      }
      coveredPartitions.push(partition)
      const reciptMapHash = await Storage.queryReceiptMapHash(parseInt(counter), partition)
      if (!reciptMapHash) {
        Logger.mainLogger.error(`Unable to find receipt hash for counter ${counter}, partition ${partition}`)
        continue
      }
      const calculatedReceiptMapHash = Crypto.hashObj(receiptMap)
      if (calculatedReceiptMapHash === reciptMapHash) {
        await Storage.updateReceiptMap(receiptMap)
        completed.push(partition)
        // eslint-disable-next-line security/detect-object-injection
        receiptMaps[partition] = receiptMap
      } else {
        Logger.mainLogger.error(
          `Different hash while downloading receipt maps for counter ${counter}, partition ${partition}`
        )
        failed.push(partition)
      }
    }
  }
  return {
    success: true,
    completed,
    failed,
    covered: coveredPartitions,
    blobs: receiptMaps,
  }
}

async function validateAndStoreSummaryBlobs(
  statsClumpForCycles: StateManager.StateManagerTypes.StatsClump[],
  validateFn: (cycle: number, partition: number) => boolean
): Promise<{
  success: boolean
  completed: number[]
  failed: number[]
  covered: number[]
  blobs: unknown
}> {
  const completed: number[] = []
  const failed: number[] = []
  const coveredPartitions: number[] = []
  const blobs: unknown = {}

  for (const statsClump of statsClumpForCycles) {
    const { cycle, dataStats, txStats, covered } = statsClump
    if (!covered) continue
    for (const partition of covered) {
      if (validateFn) {
        const shouldProcess = validateFn(cycle, partition)
        if (!shouldProcess) {
          continue
        }
      }
      coveredPartitions.push(partition)
      let summaryBlob
      const dataBlob = dataStats.find((d) => d.partition === partition)
      const txBlob = txStats.find((t) => t.partition === partition)
      const summaryHash = await Storage.querySummaryHash(cycle, partition)
      if (!summaryHash) {
        continue
      }
      const summaryObj = {
        dataStats: dataBlob ? dataBlob.opaqueBlob : {},
        txStats: txBlob ? txBlob.opaqueBlob : {},
      }
      const calculatedSummaryHash = Crypto.hashObj(summaryObj)
      if (summaryHash !== calculatedSummaryHash) {
        failed.push(partition)
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
            ...txBlob,
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
        try {
          await Storage.updateSummaryBlob(summaryBlob, cycle)
          completed.push(partition)
          // eslint-disable-next-line security/detect-object-injection
          blobs[partition] = summaryBlob
        } catch (e) {
          Logger.mainLogger.error('Unable to store summary blob', e)
          throw new Error('Unable to store summary blob')
        }
      }
    }
  }
  return {
    success: true,
    completed,
    failed,
    covered: coveredPartitions,
    blobs,
  }
}

emitter.on(
  'selectNewDataSender',
  async (newSenderInfo: NodeList.ConsensusNodeInfo, taggedDataRequest: object) => {
    //let request = {
    //...dataRequest,
    //nodeInfo: State.getNodeInfo()
    //}
    if (socketClients.has(newSenderInfo.publicKey)) {
      const response = await P2P.postJson(
        `http://${newSenderInfo.ip}:${newSenderInfo.port}/requestdata`,
        taggedDataRequest
      )
      Logger.mainLogger.debug('/requestdata response', response)
    }
  }
)
