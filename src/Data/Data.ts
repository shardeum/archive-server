import { EventEmitter } from 'events'
import { publicKey, SignedObject } from '@shardus/crypto-utils'
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Cycles from './Cycles'
import {
  getCurrentCycleCounter,
  currentCycleDuration,
  processCycles,
  validateCycle,
  validateCycleData,
  fetchCycleRecords,
  getNewestCycleFromArchivers,
  getNewestCycleFromConsensors,
} from './Cycles'
import { ChangeSquasher, parse, totalNodeCount, activeNodeCount, applyNodeListChange } from './CycleParser'
import * as State from '../State'
import * as P2P from '../P2P'
import * as Utils from '../Utils'
import { config, updateConfig } from '../Config'
import { P2P as P2PTypes } from '@shardus/types'
import * as Logger from '../Logger'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import {
  storeReceiptData,
  storeCycleData,
  storeAccountData,
  storingAccountData,
  storeOriginalTxData,
} from './Collector'
import * as CycleDB from '../dbstore/cycles'
import * as ReceiptDB from '../dbstore/receipts'
import * as OriginalTxDB from '../dbstore/originalTxsData'
import * as StateMetaData from '../archivedCycle/StateMetaData'
import fetch from 'node-fetch'
import { syncV2 } from '../sync-v2'
import { queryFromArchivers, RequestDataType } from '../API'
import ioclient = require('socket.io-client')
import { Transaction } from '../dbstore/transactions'
import { AccountCopy } from '../dbstore/accounts'
import { getJson } from '../P2P'
import { robustQuery } from '../Utils'
import { Utils as StringUtils } from '@shardus/types'

export const socketClients: Map<string, SocketIOClientStatic['Socket']> = new Map()
export let combineAccountsData = {
  accounts: [],
  receipts: [],
}
const forwardGenesisAccounts = true
export let currentConsensusRadius = 0
export let nodesPerConsensusGroup = 0
export let nodesPerEdge = 0
let subsetNodesMapByConsensusRadius: Map<number, NodeList.ConsensusNodeInfo[]> = new Map()
const maxCyclesInCycleTracker = 5
const receivedCycleTracker = {}
const QUERY_TIMEOUT_MAX = 30 // 30seconds
const {
  MAX_ACCOUNTS_PER_REQUEST,
  MAX_RECEIPTS_PER_REQUEST,
  MAX_ORIGINAL_TXS_PER_REQUEST,
  MAX_CYCLES_PER_REQUEST,
  MAX_BETWEEN_CYCLES_PER_REQUEST,
} = config.REQUEST_LIMIT

export enum DataRequestTypes {
  SUBSCRIBE = 'SUBSCRIBE',
  UNSUBSCRIBE = 'UNSUBSCRIBE',
}

export interface DataRequest<T extends P2PTypes.SnapshotTypes.ValidTypes> {
  type: P2PTypes.SnapshotTypes.TypeName<T>
  lastData: P2PTypes.SnapshotTypes.TypeIndex<T>
}

interface DataResponse<T extends P2PTypes.SnapshotTypes.ValidTypes> {
  type: P2PTypes.SnapshotTypes.TypeName<T>
  data: T[]
}

export interface CompareResponse {
  success: boolean
  matchedCycle: number
}

interface ArchiverCycleResponse {
  cycleInfo: P2PTypes.CycleCreatorTypes.CycleData[]
}

interface ArchiverTransactionResponse {
  totalTransactions: number
  transactions: Transaction[]
}

interface ArchiverAccountResponse {
  totalAccounts: number
  accounts: AccountCopy[]
}

interface ArchiverTotalDataResponse {
  totalCycles: number
  totalAccounts: number
  totalTransactions: number
  totalOriginalTxs: number
  totalReceipts: number
}

interface ArchiverReceiptResponse {
  receipts: (ReceiptDB.Receipt | ReceiptDB.ReceiptCount)[] | number
}

interface ArchiverReceiptCountResponse {
  receipts: number
}

interface ArchiverOriginalTxResponse {
  originalTxs: (OriginalTxDB.OriginalTxData | OriginalTxDB.OriginalTxDataCount)[] | number
}

interface ArchiverOriginalTxCountResponse {
  originalTxs: number
}
interface IncomingTimes {
  quarterDuration: number
  startQ1: number
  startQ2: number
  startQ3: number
  startQ4: number
  end: number
}

interface JoinStatus {
  isJoined: boolean
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

export async function unsubscribeDataSender(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
): Promise<void> {
  Logger.mainLogger.debug('Disconnecting previous connection', publicKey)
  const sender = dataSenders.get(publicKey)
  if (sender) {
    // Clear contactTimeout associated with this sender
    if (sender.contactTimeout) {
      clearTimeout(sender.contactTimeout)
      sender.contactTimeout = null
    }
    sendDataRequest(sender.nodeInfo, DataRequestTypes.UNSUBSCRIBE)
    // Delete sender from dataSenders
    dataSenders.delete(publicKey)
  }
  const socketClient = socketClients.get(publicKey)
  if (socketClient) {
    socketClient.emit('UNSUBSCRIBE', config.ARCHIVER_PUBLIC_KEY)
    socketClient.close()
    socketClients.delete(publicKey)
  }
  nestedCountersInstance.countEvent('archiver', 'remove_data_sender')
  Logger.mainLogger.debug(
    'Subscribed dataSenders',
    dataSenders.size,
    'Connected socketClients',
    socketClients.size
  )
  if (config.VERBOSE)
    Logger.mainLogger.debug(
      'Subscribed dataSenders',
      dataSenders.keys(),
      'Connected socketClients',
      socketClients.keys()
    )
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo): void {
  if (config.VERBOSE) Logger.mainLogger.debug('Node Info to socket connect', node)
  const socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)
  socketClients.set(node.publicKey, socketClient)

  let archiverKeyisEmitted = false

  socketClient.on('connect', () => {
    Logger.mainLogger.debug(
      `${!archiverKeyisEmitted ? 'New connection' : 'Reconnection'} to consensus node ${node.ip}:${
        node.port
      } is made`
    )
    if (archiverKeyisEmitted) return
    // Send ehlo event right after connect:
    socketClient.emit('ARCHIVER_PUBLIC_KEY', config.ARCHIVER_PUBLIC_KEY)
    archiverKeyisEmitted = true
    if (config.VERBOSE) Logger.mainLogger.debug('Connected node', node)
    if (config.VERBOSE) Logger.mainLogger.debug('Init socketClients', socketClients.size, dataSenders.size)
  })

  socketClient.once('disconnect', async () => {
    Logger.mainLogger.debug(`Connection request is refused by the consensor node ${node.ip}:${node.port}`)
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
      unsubscribeDataSender(node.publicKey)
      return
    }

    if (config.experimentalSnapshot) {
      // Get sender entry
      let sender = dataSenders.get(newData.publicKey)
      // If no sender entry, remove publicKey from senders, END
      if (!sender) {
        Logger.mainLogger.error('This sender is not in the subscribed nodes list', newData.publicKey)
        // unsubscribeDataSender(newData.publicKey)
        return
      }
      // Clear senders contactTimeout, if it has one
      if (sender.contactTimeout) {
        if (config.VERBOSE) Logger.mainLogger.debug('Clearing contact timeout.')
        clearTimeout(sender.contactTimeout)
        sender.contactTimeout = null
        nestedCountersInstance.countEvent('archiver', 'clear_contact_timeout')
      }

      if (config.VERBOSE)
        console.log('DATA', sender.nodeInfo.publicKey, sender.nodeInfo.ip, sender.nodeInfo.port)

      if (newData.responses && newData.responses.ORIGINAL_TX_DATA) {
        if (config.VERBOSE)
          Logger.mainLogger.debug(
            'ORIGINAL_TX_DATA',
            sender.nodeInfo.publicKey,
            sender.nodeInfo.ip,
            sender.nodeInfo.port,
            newData.responses.ORIGINAL_TX_DATA.length
          )
        storeOriginalTxData(
          newData.responses.ORIGINAL_TX_DATA,
          sender.nodeInfo.ip + ':' + sender.nodeInfo.port,
          config.saveOnlyGossipData
        )
      }
      if (newData.responses && newData.responses.RECEIPT) {
        if (config.VERBOSE)
          Logger.mainLogger.debug(
            'RECEIPT',
            sender.nodeInfo.publicKey,
            sender.nodeInfo.ip,
            sender.nodeInfo.port,
            newData.responses.RECEIPT.length
          )
        storeReceiptData(
          newData.responses.RECEIPT,
          sender.nodeInfo.ip + ':' + sender.nodeInfo.port,
          true,
          config.saveOnlyGossipData
        )
      }
      if (newData.responses && newData.responses.CYCLE) {
        collectCycleData(newData.responses.CYCLE, sender.nodeInfo.ip + ':' + sender.nodeInfo.port)
      }
      if (newData.responses && newData.responses.ACCOUNT) {
        console.log(
          'RECEIVED ACCOUNTS DATA',
          sender.nodeInfo.publicKey,
          sender.nodeInfo.ip,
          sender.nodeInfo.port
        )
        Logger.mainLogger.debug(
          'RECEIVED ACCOUNTS DATA',
          sender.nodeInfo.publicKey,
          sender.nodeInfo.ip,
          sender.nodeInfo.port
        )
        nestedCountersInstance.countEvent('genesis', 'accounts', 1)
        if (!forwardGenesisAccounts) {
          console.log('Genesis Accounts To Sycn', newData.responses.ACCOUNT)
          Logger.mainLogger.debug('Genesis Accounts To Sycn', newData.responses.ACCOUNT)
          syncGenesisAccountsFromConsensor(newData.responses.ACCOUNT, sender.nodeInfo)
        } else {
          if (storingAccountData) {
            console.log('Storing Data')
            let newCombineAccountsData = { ...combineAccountsData }
            if (newData.responses.ACCOUNT.accounts)
              newCombineAccountsData.accounts = [
                ...newCombineAccountsData.accounts,
                ...newData.responses.ACCOUNT.accounts,
              ]
            if (newData.responses.ACCOUNT.receipts)
              newCombineAccountsData.receipts = [
                ...newCombineAccountsData.receipts,
                ...newData.responses.ACCOUNT.receipts,
              ]
            combineAccountsData = { ...newCombineAccountsData }
            newCombineAccountsData = {
              accounts: [],
              receipts: [],
            }
          } else storeAccountData(newData.responses.ACCOUNT)
        }
      }

      // Set new contactTimeout for sender. Postpone sender removal because data is still received from consensor
      if (currentCycleDuration > 0) {
        nestedCountersInstance.countEvent('archiver', 'postpone_contact_timeout')
        // To make sure that the sender is still in the subscribed list
        sender = dataSenders.get(newData.publicKey)
        if (sender)
          sender.contactTimeout = createContactTimeout(
            sender.nodeInfo.publicKey,
            'This timeout is created after processing data'
          )
      }
      return
    }
  })
}

export function collectCycleData(
  cycleData: P2PTypes.CycleCreatorTypes.CycleData[],
  senderInfo: string
): void {
  for (const cycle of cycleData) {
    // Logger.mainLogger.debug('Cycle received', cycle.counter, senderInfo)
    let cycleToSave = []
    if (receivedCycleTracker[cycle.counter]) {
      if (receivedCycleTracker[cycle.counter][cycle.marker]) {
        if (!receivedCycleTracker[cycle.counter][cycle.marker]['senderNodes'].includes(senderInfo)) {
          receivedCycleTracker[cycle.counter][cycle.marker]['receivedTimes']++
          receivedCycleTracker[cycle.counter][cycle.marker]['senderNodes'].push(senderInfo)
        }
      } else {
        if (!validateCycleData(cycle)) continue
        receivedCycleTracker[cycle.counter][cycle.marker] = {
          cycleInfo: cycle,
          receivedTimes: 1,
          saved: false,
          senderNodes: [senderInfo],
        }
        if (config.VERBOSE) Logger.mainLogger.debug('Different Cycle Record received', cycle.counter)
      }
    } else {
      if (!validateCycleData(cycle)) continue
      receivedCycleTracker[cycle.counter] = {
        [cycle.marker]: {
          cycleInfo: cycle,
          receivedTimes: 1,
          saved: false,
          senderNodes: [senderInfo],
        },
      }
    }
    if (config.VERBOSE)
      Logger.mainLogger.debug('Cycle received', cycle.counter, receivedCycleTracker[cycle.counter])
    const minCycleConfirmations =
      Math.min(Math.ceil(NodeList.getActiveNodeCount() / currentConsensusRadius), 5) || 1

    for (const value of Object.values(receivedCycleTracker[cycle.counter])) {
      if (value['saved']) {
        // If there is a saved cycle, clear the cycleToSave of this counter; This is to prevent saving the another cycle of the same counter
        for (let i = 0; i < cycleToSave.length; i++) {
          // eslint-disable-next-line security/detect-object-injection
          receivedCycleTracker[cycle.counter][cycleToSave[i].marker]['saved'] = false
        }
        cycleToSave = []
        break
      }
      if (value['receivedTimes'] >= minCycleConfirmations) {
        cycleToSave.push(cycle)
        value['saved'] = true
      }
    }
    if (cycleToSave.length > 0) {
      processCycles(cycleToSave)
    }
  }
  if (Object.keys(receivedCycleTracker).length > maxCyclesInCycleTracker) {
    for (const counter of Object.keys(receivedCycleTracker)) {
      // Clear cycles that are older than last maxCyclesInCycleTracker cycles
      if (parseInt(counter) < getCurrentCycleCounter() - maxCyclesInCycleTracker) {
        let totalTimes = 0
        let logCycle = false
        // If there is more than one marker for this cycle, output the cycle log
        // eslint-disable-next-line security/detect-object-injection
        if (Object.keys(receivedCycleTracker[counter]).length > 1) logCycle = true
        // eslint-disable-next-line security/detect-object-injection
        for (const key of Object.keys(receivedCycleTracker[counter])) {
          Logger.mainLogger.debug(
            'Cycle',
            counter,
            key, // marker
            /* eslint-disable security/detect-object-injection */
            receivedCycleTracker[counter][key]['receivedTimes'],
            logCycle ? StringUtils.safeStringify(receivedCycleTracker[counter][key]['senderNodes']) : '',
            logCycle ? receivedCycleTracker[counter][key] : ''
            /* eslint-enable security/detect-object-injection */
          )
          // eslint-disable-next-line security/detect-object-injection
          totalTimes += receivedCycleTracker[counter][key]['receivedTimes']
        }
        if (logCycle) Logger.mainLogger.debug(`Cycle ${counter} has different markers!`)
        Logger.mainLogger.debug(`Received ${totalTimes} times for cycle counter ${counter}`)
        // eslint-disable-next-line security/detect-object-injection
        delete receivedCycleTracker[counter]
      }
    }
  }
}

export function clearCombinedAccountsData(): void {
  combineAccountsData = {
    accounts: [],
    receipts: [],
  }
}

export interface DataSender {
  nodeInfo: NodeList.ConsensusNodeInfo
  types: (keyof typeof P2PTypes.SnapshotTypes.TypeNames)[]
  contactTimeout?: NodeJS.Timeout | null
  replaceTimeout?: NodeJS.Timeout | null
}

export const dataSenders: Map<NodeList.ConsensusNodeInfo['publicKey'], DataSender> = new Map()

export const emitter = new EventEmitter()

export async function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']): Promise<void> {
  nestedCountersInstance.countEvent('archiver', 'replace_data_sender')
  if (NodeList.getActiveNodeCount() < 2) {
    Logger.mainLogger.debug('There is only one active node in the network. Unable to replace data sender')
    return
  }
  Logger.mainLogger.debug(`replaceDataSender: replacing ${publicKey}`)

  if (!socketClients.has(publicKey) || !dataSenders.has(publicKey)) {
    Logger.mainLogger.debug(
      'This data sender is not in the subscribed list! and unsubscribing it',
      publicKey,
      socketClients.has(publicKey),
      dataSenders.has(publicKey)
    )
    unsubscribeDataSender(publicKey)
    return
  }
  unsubscribeDataSender(publicKey)
  // eslint-disable-next-line security/detect-object-injection
  const node = NodeList.byPublicKey.get(publicKey)
  if (node) {
    const nodeIndex = NodeList.activeListByIdSorted.findIndex((node) => node.publicKey === publicKey)
    if (nodeIndex > -1) {
      const subsetIndex = Math.floor(nodeIndex / currentConsensusRadius)
      const subsetNodesList = subsetNodesMapByConsensusRadius.get(subsetIndex)
      if (!subsetNodesList) {
        Logger.mainLogger.error(
          `There is no nodes in the index ${subsetIndex} of subsetNodesMapByConsensusRadius!`
        )
        return
      }
      subscribeNodeFromThisSubset(subsetNodesList)
    }
  }
}

export async function subscribeNodeForDataTransfer(): Promise<void> {
  if (config.experimentalSnapshot) {
    await subscribeConsensorsByConsensusRadius()
  } else {
    await StateMetaData.subscribeRandomNodeForDataTransfer()
  }
}

/**
 * Sets 15s timeout
 * Removes sender from dataSenders on timeout
 * Select a new dataSender
 */
export function createContactTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey'],
  msg = ''
): NodeJS.Timeout {
  const CONTACT_TIMEOUT_MS = 10 * 1000 // Change contact timeout to 10s
  if (config.VERBOSE)
    Logger.mainLogger.debug('Created contact timeout: ' + CONTACT_TIMEOUT_MS, `for ${publicKey}`)
  nestedCountersInstance.countEvent('archiver', 'contact_timeout_created')
  return setTimeout(() => {
    // Logger.mainLogger.debug('nestedCountersInstance', nestedCountersInstance)
    if (nestedCountersInstance) nestedCountersInstance.countEvent('archiver', 'contact_timeout')
    Logger.mainLogger.debug('REPLACING sender due to CONTACT timeout', msg, publicKey)
    replaceDataSender(publicKey)
  }, CONTACT_TIMEOUT_MS)
}

export function addDataSender(sender: DataSender): void {
  dataSenders.set(sender.nodeInfo.publicKey, sender)
}

async function getConsensusRadius(): Promise<number> {
  // If there is no node, return existing currentConsensusRadius
  if (NodeList.isEmpty()) return currentConsensusRadius

  // Define the query function to get the network config from a node
  const queryFn = async (node): Promise<object> => {
    const REQUEST_NETCONFIG_TIMEOUT_SECOND = 2 // 2s timeout
    try {
      const response = await P2P.getJson(
        `http://${node.ip}:${node.port}/netconfig`,
        REQUEST_NETCONFIG_TIMEOUT_SECOND
      )
      return response
    } catch (error) {
      Logger.mainLogger.error(`Error querying node ${node.ip}:${node.port}: ${error}`)
      return null
    }
  }

  // Define the equality function to compare two responses
  const equalityFn = (responseA, responseB): boolean => {
    return (
      responseA?.config?.sharding?.nodesPerConsensusGroup ===
      responseB?.config?.sharding?.nodesPerConsensusGroup
    )
  }

  // Get the list of 10 max random active nodes or the first node if no active nodes are available
  const nodes =
    NodeList.getActiveNodeCount() > 0 ? NodeList.getRandomActiveNodes(10) : [NodeList.getFirstNode()]

  // Use robustQuery to get the consensusRadius from multiple nodes
  const tallyItem = await robustQuery(
    nodes,
    queryFn,
    equalityFn,
    3 // Redundancy (minimum 3 nodes should return the same result to reach consensus)
  )

  // Check if a consensus was reached
  if (tallyItem && tallyItem.value && tallyItem.value.config) {
    nodesPerConsensusGroup = tallyItem.value.config.sharding.nodesPerConsensusGroup
    nodesPerEdge = tallyItem.value.config.sharding.nodesPerEdge
    const devPublicKeys = tallyItem.value.config.debug.devPublicKeys
    const updateConfigProps = {
      newPOQReceipt: tallyItem.value.config.stateManager.useNewPOQ,
      DevPublicKey: Object.keys(devPublicKeys).find((key) => devPublicKeys[key] === 3),
    }
    updateConfig(updateConfigProps)
    // Upgrading consensus size to an odd number
    if (nodesPerConsensusGroup % 2 === 0) nodesPerConsensusGroup++
    const consensusRadius = Math.floor((nodesPerConsensusGroup - 1) / 2)
    // Validation: Ensure consensusRadius is a number and greater than zero
    if (typeof consensusRadius !== 'number' || isNaN(consensusRadius) || consensusRadius <= 0) {
      Logger.mainLogger.error('Invalid consensusRadius:', consensusRadius)
      return currentConsensusRadius // Return the existing currentConsensusRadius in case of invalid consensusRadius
    }
    Logger.mainLogger.debug(
      'consensusRadius',
      consensusRadius,
      'nodesPerConsensusGroup',
      nodesPerConsensusGroup,
      'nodesPerEdge',
      nodesPerEdge
    )
    return consensusRadius
  }
  Logger.mainLogger.error('Failed to get consensusRadius from the network')
  // If no consensus was reached, return the existing currentConsensusRadius
  return currentConsensusRadius
}

export async function createDataTransferConnection(
  newSenderInfo: NodeList.ConsensusNodeInfo
): Promise<boolean> {
  // // Verify node before subscribing for data transfer
  // const status = await verifyNode(newSenderInfo)
  // if (!status) return false
  // Subscribe this node for dataRequest
  const response = await sendDataRequest(newSenderInfo, DataRequestTypes.SUBSCRIBE)
  if (response) {
    initSocketClient(newSenderInfo)
    // Add new dataSender to dataSenders
    const newSender: DataSender = {
      nodeInfo: newSenderInfo,
      types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
      contactTimeout: createContactTimeout(
        newSenderInfo.publicKey,
        'This timeout is created during newSender selection'
      ),
    }
    addDataSender(newSender)
    Logger.mainLogger.debug(`added new sender ${newSenderInfo.publicKey} to dataSenders`)
  }
  return response
}

export async function createNodesGroupByConsensusRadius(): Promise<void> {
  const consensusRadius = await getConsensusRadius()
  if (consensusRadius === 0) {
    Logger.mainLogger.error('Consensus radius is 0, unable to create nodes group.')
    return // Early return to prevent further execution
  }
  currentConsensusRadius = consensusRadius
  const activeList = [...NodeList.activeListByIdSorted]
  if (config.VERBOSE) Logger.mainLogger.debug('activeList', activeList.length, activeList)
  let totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
  // Only if there are less than 4 activeArchivers and if the consensusRadius is greater than 5
  if (config.subscribeToMoreConsensors && State.activeArchivers.length < 4 && currentConsensusRadius > 5) {
    totalNumberOfNodesToSubscribe += totalNumberOfNodesToSubscribe * config.extraConsensorsToSubscribe
  }
  Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  subsetNodesMapByConsensusRadius = new Map()
  let round = 0
  for (let i = 0; i < activeList.length; i += consensusRadius) {
    const subsetList: NodeList.ConsensusNodeInfo[] = activeList.slice(i, i + consensusRadius)
    subsetNodesMapByConsensusRadius.set(round, subsetList)
    round++
  }
  if (config.VERBOSE)
    Logger.mainLogger.debug('subsetNodesMapByConsensusRadius', subsetNodesMapByConsensusRadius)
}

export async function subscribeConsensorsByConsensusRadius(): Promise<void> {
  await createNodesGroupByConsensusRadius()
  for (const [i, subsetList] of subsetNodesMapByConsensusRadius) {
    if (config.VERBOSE) Logger.mainLogger.debug('Round', i, 'subsetList', subsetList, dataSenders.keys())
    subscribeNodeFromThisSubset(subsetList)
  }
}

export async function subscribeNodeFromThisSubset(nodeList: NodeList.ConsensusNodeInfo[]): Promise<void> {
  // First check if there is any subscribed node from this subset
  const subscribedNodesFromThisSubset = []
  for (const node of nodeList) {
    if (dataSenders.has(node.publicKey)) {
      if (config.VERBOSE)
        Logger.mainLogger.debug('This node from the subset is in the subscribed list!', node.publicKey)
      subscribedNodesFromThisSubset.push(node.publicKey)
    }
  }
  let numberOfNodesToSubsribe = 1
  // Only if there are less than 4 activeArchivers and if the consensusRadius is greater than 5
  if (config.subscribeToMoreConsensors && State.activeArchivers.length < 4 && currentConsensusRadius > 5) {
    numberOfNodesToSubsribe += config.extraConsensorsToSubscribe
  }
  if (subscribedNodesFromThisSubset.length > numberOfNodesToSubsribe) {
    // If there is more than one subscribed node from this subset, unsubscribe the extra ones
    for (const publicKey of subscribedNodesFromThisSubset.splice(numberOfNodesToSubsribe)) {
      Logger.mainLogger.debug('Unsubscribing extra node from this subset', publicKey)
      unsubscribeDataSender(publicKey)
    }
  }
  if (config.VERBOSE)
    Logger.mainLogger.debug('Subscribed nodes from this subset', subscribedNodesFromThisSubset)
  if (subscribedNodesFromThisSubset.length === numberOfNodesToSubsribe) return
  Logger.mainLogger.debug('Subscribing node from this subset!')
  // Pick a new dataSender from this subset
  let subsetList = [...nodeList]
  // Pick a random dataSender
  let newSenderInfo = nodeList[Math.floor(Math.random() * nodeList.length)]
  let connectionStatus = false
  let retry = 0
  const MAX_RETRY_SUBSCRIPTION = 3 * numberOfNodesToSubsribe
  while (retry < MAX_RETRY_SUBSCRIPTION && subscribedNodesFromThisSubset.length < numberOfNodesToSubsribe) {
    if (!dataSenders.has(newSenderInfo.publicKey)) {
      connectionStatus = await createDataTransferConnection(newSenderInfo)
      if (connectionStatus) {
        // Check if the newSender is in the subscribed nodes of this subset
        if (!subscribedNodesFromThisSubset.includes(newSenderInfo.publicKey))
          subscribedNodesFromThisSubset.push(newSenderInfo.publicKey)
      }
    } else {
      // Add the newSender to the subscribed nodes of this subset
      if (!subscribedNodesFromThisSubset.includes(newSenderInfo.publicKey))
        subscribedNodesFromThisSubset.push(newSenderInfo.publicKey)
    }
    subsetList = subsetList.filter((node) => node.publicKey !== newSenderInfo.publicKey)
    if (subsetList.length > 0) {
      newSenderInfo = subsetList[Math.floor(Math.random() * subsetList.length)]
    } else {
      subsetList = [...nodeList]
      retry++
    }
  }
}

// This function is used for both subscribe and unsubscribe for data request
export async function sendDataRequest(
  nodeInfo: NodeList.ConsensusNodeInfo,
  dataRequestType: DataRequestTypes
): Promise<boolean> {
  const dataRequest = {
    dataRequestCycle: getCurrentCycleCounter(),
    dataRequestType,
    publicKey: State.getNodeInfo().publicKey,
    nodeInfo: State.getNodeInfo(),
  }
  const taggedDataRequest = Crypto.tag(dataRequest, nodeInfo.publicKey)
  Logger.mainLogger.info(
    `Sending ${dataRequestType} data request to consensor.`,
    nodeInfo.ip + ':' + nodeInfo.port
  )
  let reply = false
  const REQUEST_DATA_TIMEOUT_SECOND = 2 // 2s timeout
  const response = await P2P.postJson(
    `http://${nodeInfo.ip}:${nodeInfo.port}/requestdata`,
    taggedDataRequest,
    REQUEST_DATA_TIMEOUT_SECOND
  )
  Logger.mainLogger.debug('/requestdata response', response, nodeInfo.ip + ':' + nodeInfo.port)
  if (response && response.success) reply = response.success
  return reply
}

export const clearDataSenders = async (): Promise<void> => {
  for (const [publicKey] of dataSenders) {
    unsubscribeDataSender(publicKey)
  }
  await Utils.sleep(2000) // Wait for 2s to make sure all dataSenders are unsubscribed
  dataSenders.clear()
  socketClients.clear()
  subsetNodesMapByConsensusRadius.clear()
}

export function calcIncomingTimes(record: P2PTypes.CycleCreatorTypes.CycleRecord): IncomingTimes {
  const SECOND = 1000
  const cycleDuration = record.duration * SECOND
  const quarterDuration = cycleDuration / 4
  const start = record.start * SECOND + cycleDuration
  const startQ1 = start
  const startQ2 = start + quarterDuration
  const startQ3 = start + 2 * quarterDuration
  const startQ4 = start + 3 * quarterDuration
  const end = start + cycleDuration
  return { quarterDuration, startQ1, startQ2, startQ3, startQ4, end }
}

export async function joinNetwork(
  nodeList: NodeList.ConsensusNodeInfo[],
  isFirstTime: boolean
): Promise<boolean> {
  Logger.mainLogger.debug('Is firstTime', isFirstTime)
  if (!isFirstTime) {
    const isJoined: boolean = await checkJoinStatus(nodeList)
    if (isJoined) {
      return isJoined
    }
  }
  Logger.mainLogger.debug('nodeList To Submit Join Request', nodeList)
  // try to get latestCycleRecord with a robust query
  const latestCycle = await getNewestCycleFromConsensors(nodeList)

  // Figure out when Q1 is from the latestCycle
  const { startQ1 } = calcIncomingTimes(latestCycle)
  const shuffledNodes = [...nodeList]
  Utils.shuffleArray(shuffledNodes)

  // Wait until a Q1 then send join request to active nodes
  let untilQ1 = startQ1 - Date.now()
  while (untilQ1 < 0) {
    untilQ1 += latestCycle.duration * 1000
  }

  Logger.mainLogger.debug(`Waiting ${untilQ1 + 500} ms for Q1 before sending join...`)
  await Utils.sleep(untilQ1 + 500) // Not too early

  // Create a fresh join request, so that the request timestamp range is acceptable
  const request = P2P.createArchiverJoinRequest()
  await submitJoin(nodeList, request)

  // Wait approx. one cycle then check again
  Logger.mainLogger.debug('Waiting approx. one cycle then checking again...')
  await Utils.sleep(latestCycle.duration * 1000 + 500)
  return false
}

export async function submitJoin(
  nodes: NodeList.ConsensusNodeInfo[],
  joinRequest: P2P.ArchiverJoinRequest & SignedObject
): Promise<void> {
  // Send the join request to a handful of the active node all at once:w
  const selectedNodes = Utils.getRandom(nodes, Math.min(nodes.length, 5))
  Logger.mainLogger.debug(`Sending join request to ${selectedNodes.map((n) => `${n.ip}:${n.port}`)}`)
  for (const node of selectedNodes) {
    const response = await P2P.postJson(`http://${node.ip}:${node.port}/joinarchiver`, joinRequest)
    Logger.mainLogger.debug('Join request response:', response)
  }
}

export async function sendLeaveRequest(nodes: NodeList.ConsensusNodeInfo[]): Promise<void> {
  const leaveRequest = P2P.createArchiverLeaveRequest()
  Logger.mainLogger.debug(`Sending leave request to ${nodes.map((n) => `${n.ip}:${n.port}`)}`)

  const promises = nodes.map((node) =>
    fetch(`http://${node.ip}:${node.port}/leavingarchivers`, {
      method: 'post',
      body: StringUtils.safeStringify(leaveRequest),
      headers: { 'Content-Type': 'application/json' },
      timeout: 2 * 1000, // 2s timeout
    }).then((res) => res.json())
  )

  await Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      let isLeaveRequestSent = false
      for (const response of responses) {
        // eslint-disable-next-line security/detect-object-injection
        const node = nodes[i]
        if (response.status === 'fulfilled') {
          const res = response.value
          if (res.success) isLeaveRequestSent = true
          Logger.mainLogger.debug(`Leave request response from ${node.ip}:${node.port}:`, res)
        } else Logger.mainLogger.debug(`Node is not responding ${node.ip}:${node.port}`)
        i++
      }
      Logger.mainLogger.debug('isLeaveRequestSent', isLeaveRequestSent)
    })
    .catch((error) => {
      // Handle any errors that occurred
      console.error(error)
    })
}

export async function sendActiveRequest(): Promise<void> {
  Logger.mainLogger.debug('Sending Active Request to the network!')
  const latestCycleInfo = await CycleDB.queryLatestCycleRecords(1)
  const latestCycle = latestCycleInfo[0]
  // Figure out when Q1 is from the latestCycle
  const { startQ1 } = calcIncomingTimes(latestCycle)

  // Wait until a Q1 then send active request to active nodes
  let untilQ1 = startQ1 - Date.now()
  while (untilQ1 < 0) {
    untilQ1 += latestCycle.duration * 1000
  }

  Logger.mainLogger.debug(`Waiting ${untilQ1 + 500} ms for Q1 before sending active...`)
  await Utils.sleep(untilQ1 + 500) // Not too early

  const activeRequest = P2P.createArchiverActiveRequest()
  // Send the active request to a handful of the active node all at once:w
  const nodes = NodeList.getRandomActiveNodes(5)
  Logger.mainLogger.debug(`Sending active request to ${nodes.map((n) => `${n.ip}:${n.port}`)}`)

  const promises = nodes.map((node) =>
    fetch(`http://${node.ip}:${node.port}/activearchiver`, {
      method: 'post',
      body: StringUtils.safeStringify(activeRequest),
      headers: { 'Content-Type': 'application/json' },
      timeout: 2 * 1000, // 2s timeout
    }).then((res) => res.json())
  )

  await Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      for (const response of responses) {
        // eslint-disable-next-line security/detect-object-injection
        const node = nodes[i]
        if (response.status === 'fulfilled') {
          const res = response.value
          Logger.mainLogger.debug(`Active request response from ${node.ip}:${node.port}:`, res)
        } else Logger.mainLogger.debug(`Node is not responding ${node.ip}:${node.port}`)
        i++
      }
    })
    .catch((error) => {
      // Handle any errors that occurred
      console.error(error)
    })

  // Wait approx. one cycle then check again
  Logger.mainLogger.debug('Waiting approx. one cycle then checking again...')
  await Utils.sleep(latestCycle.duration * 1000 + 500)
}

export async function getCycleDuration(): Promise<number> {
  const response = (await queryFromArchivers(RequestDataType.CYCLE, { count: 1 })) as ArchiverCycleResponse
  if (response && response.cycleInfo) {
    return response.cycleInfo[0].duration
  }
  return 0
}

/*
  checkJoinStatus checks if the current archiver node is joined to a network. 
  This queries by the /joinedArchiver endpoint on the nodes and returns joining status based on majority response.
*/
export async function checkJoinStatus(activeNodes: NodeList.ConsensusNodeInfo[]): Promise<boolean> {
  Logger.mainLogger.debug('checkJoinStatus: Checking join status')
  const ourNodeInfo = State.getNodeInfo()

  const queryFn = async (node: NodeList.ConsensusNodeInfo): Promise<JoinStatus> => {
    const url = `http://${node.ip}:${node.port}/joinedArchiver/${ourNodeInfo.publicKey}`
    try {
      return (await getJson(url)) as JoinStatus
    } catch (e) {
      Logger.mainLogger.error(`Error querying node ${node.ip}:${node.port}: ${e}`)
      throw e
    }
  }

  try {
    const joinStatus = await robustQuery(activeNodes, queryFn)
    Logger.mainLogger.debug(`checkJoinStatus: Join status: ${joinStatus.value.isJoined}`)
    return joinStatus.value.isJoined
  } catch (e) {
    Logger.mainLogger.error(`Error in checkJoinStatus: ${e}`)
    return false
  }
}

// This will be used once activeArchivers field is added to the cycle record
export async function checkActiveStatus(): Promise<boolean> {
  Logger.mainLogger.debug('Checking active status')
  const ourNodeInfo = State.getNodeInfo()
  try {
    const latestCycle = await getNewestCycleFromArchivers()

    if (latestCycle && latestCycle['activeArchivers']) {
      const activeArchivers = latestCycle['activeArchivers']
      Logger.mainLogger.debug('cycle counter', latestCycle.counter)
      Logger.mainLogger.debug('Active archivers', activeArchivers)

      const isActive = activeArchivers.some(
        (a: State.ArchiverNodeInfo) => a.publicKey === ourNodeInfo.publicKey
      )
      Logger.mainLogger.debug('isActive', isActive)
      return isActive
    } else {
      return false
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    return false
  }
}

export async function getTotalDataFromArchivers(): Promise<ArchiverTotalDataResponse | null> {
  return (await queryFromArchivers(
    RequestDataType.TOTALDATA,
    {},
    QUERY_TIMEOUT_MAX
  )) as ArchiverTotalDataResponse | null
}

export async function syncGenesisAccountsFromArchiver(): Promise<void> {
  let complete = false
  let startAccount = 0
  let endAccount = startAccount + MAX_ACCOUNTS_PER_REQUEST
  let totalGenesisAccounts = 0
  // const totalExistingGenesisAccounts =
  //   await AccountDB.queryAccountCountBetweenCycles(0, 5);
  // if (totalExistingGenesisAccounts > 0) {
  //   // Let's assume it has synced data for now, update to sync account count between them
  //   return;
  // }
  const res = (await queryFromArchivers(
    RequestDataType.ACCOUNT,
    { startCycle: 0, endCycle: 5 },
    QUERY_TIMEOUT_MAX
  )) as ArchiverAccountResponse
  if (res && (res.totalAccounts || res.totalAccounts === 0)) {
    totalGenesisAccounts = res.totalAccounts
    Logger.mainLogger.debug('TotalGenesis Accounts', totalGenesisAccounts)
  } else {
    Logger.mainLogger.error('Genesis Total Accounts Query', 'Invalid download response')
    return
  }
  if (totalGenesisAccounts <= 0) return
  let page = 1
  while (!complete) {
    Logger.mainLogger.debug(`Downloading accounts from ${startAccount} to ${endAccount}`)
    const response = (await queryFromArchivers(
      RequestDataType.ACCOUNT,
      {
        startCycle: 0,
        endCycle: 5,
        page,
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverAccountResponse
    if (response && response.accounts) {
      if (response.accounts.length < MAX_ACCOUNTS_PER_REQUEST) {
        complete = true
        Logger.mainLogger.debug('Download completed for accounts')
      }
      Logger.mainLogger.debug(`Downloaded accounts`, response.accounts.length)
      await storeAccountData({ accounts: response.accounts })
      startAccount = endAccount + 1
      endAccount += MAX_ACCOUNTS_PER_REQUEST
      page++
    } else {
      Logger.mainLogger.debug('Genesis Accounts Query', 'Invalid download response')
    }
    // await sleep(1000);
  }
  Logger.mainLogger.debug('Sync genesis accounts completed!')
}

export async function syncGenesisTransactionsFromArchiver(): Promise<void> {
  let complete = false
  let startTransaction = 0
  let endTransaction = startTransaction + MAX_ACCOUNTS_PER_REQUEST // Sames as number of accounts per request
  let totalGenesisTransactions = 0

  const res = (await queryFromArchivers(
    RequestDataType.TRANSACTION,
    {
      startCycle: 0,
      endCycle: 5,
    },
    QUERY_TIMEOUT_MAX
  )) as ArchiverTransactionResponse
  if (res && (res.totalTransactions || res.totalTransactions === 0)) {
    totalGenesisTransactions = res.totalTransactions
    Logger.mainLogger.debug('TotalGenesis Transactions', totalGenesisTransactions)
  } else {
    Logger.mainLogger.error('Genesis Total Transaction Query', 'Invalid download response')
    return
  }
  if (totalGenesisTransactions <= 0) return
  let page = 1
  while (!complete) {
    Logger.mainLogger.debug(`Downloading transactions from ${startTransaction} to ${endTransaction}`)
    const response = (await queryFromArchivers(
      RequestDataType.TRANSACTION,
      {
        startCycle: 0,
        endCycle: 5,
        page,
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverTransactionResponse
    if (response && response.transactions) {
      if (response.transactions.length < MAX_ACCOUNTS_PER_REQUEST) {
        complete = true
        Logger.mainLogger.debug('Download completed for transactions')
      }
      Logger.mainLogger.debug(`Downloaded transactions`, response.transactions.length)
      await storeAccountData({ receipts: response.transactions })
      startTransaction = endTransaction + 1
      endTransaction += MAX_ACCOUNTS_PER_REQUEST
      page++
    } else {
      Logger.mainLogger.debug('Genesis Transactions Query', 'Invalid download response')
    }
    // await sleep(1000);
  }
  Logger.mainLogger.debug('Sync genesis transactions completed!')
}

export async function syncGenesisAccountsFromConsensor(
  totalGenesisAccounts = 0,
  firstConsensor: NodeList.ConsensusNodeInfo
): Promise<void> {
  if (totalGenesisAccounts <= 0) return
  let startAccount = 0
  // let combineAccountsData = [];
  let totalDownloadedAccounts = 0
  while (startAccount <= totalGenesisAccounts) {
    Logger.mainLogger.debug(`Downloading accounts from ${startAccount}`)
    const response = (await P2P.getJson(
      `http://${firstConsensor.ip}:${firstConsensor.port}/genesis_accounts?start=${startAccount}`,
      QUERY_TIMEOUT_MAX
    )) as ArchiverAccountResponse
    if (response && response.accounts) {
      if (response.accounts.length < MAX_ACCOUNTS_PER_REQUEST) {
        Logger.mainLogger.debug('Download completed for accounts')
      }
      Logger.mainLogger.debug(`Downloaded accounts`, response.accounts.length)
      // TODO - update to include receipts data also
      await storeAccountData({ accounts: response.accounts })
      // combineAccountsData = [...combineAccountsData, ...response.accounts];
      totalDownloadedAccounts += response.accounts.length
      startAccount += MAX_ACCOUNTS_PER_REQUEST
    } else {
      Logger.mainLogger.debug('Genesis Accounts Query', 'Invalid download response')
    }
    // await sleep(1000);
  }
  Logger.mainLogger.debug(`Total downloaded accounts`, totalDownloadedAccounts)
  // await storeAccountData(combineAccountsData);
  Logger.mainLogger.debug('Sync genesis accounts completed!')
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
    const prevCycles = await CycleDB.queryCycleRecordsBetween(start, end)

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
    if (i === CycleChain.length - 1) await storeCycleData(CycleChain)
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
    let nextEnd: number = endCycle - MAX_CYCLES_PER_REQUEST
    if (nextEnd < 0) nextEnd = 0
    Logger.mainLogger.debug(`Getting cycles ${nextEnd} - ${endCycle} ...`)
    const prevCycles = await fetchCycleRecords(nextEnd, endCycle)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')
    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    const combineCycles = []
    for (const prevCycle of prevCycles) {
      // Stop saving prevCycles if one of them is invalid
      if (validateCycle(prevCycle, savedCycleRecord) === false) {
        Logger.mainLogger.error(`Record ${prevCycle.counter} failed validation`)
        Logger.mainLogger.debug('fail', prevCycle, savedCycleRecord)
        break
      }
      savedCycleRecord = prevCycle
      combineCycles.push(prevCycle)
    }
    await storeCycleData(combineCycles)
    endCycle = nextEnd - 1
  }
}

export async function syncCyclesAndNodeListV2(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredCycleCount = 0
): Promise<boolean> {
  // Sync validator list and get the latest cycle from the network
  Logger.mainLogger.debug('Syncing validators and latest cycle...')
  const syncResult = await syncV2(activeArchivers)
  let cycleToSyncTo: P2PTypes.CycleCreatorTypes.CycleData
  if (syncResult.isOk()) {
    cycleToSyncTo = syncResult.value
  } else {
    throw syncResult.error
  }

  Logger.mainLogger.debug('cycleToSyncTo', cycleToSyncTo)
  Logger.mainLogger.debug(`Syncing till cycle ${cycleToSyncTo.counter}...`)

  currentConsensusRadius = await getConsensusRadius()
  await processCycles([cycleToSyncTo])

  // Download old cycle Records
  await downloadOldCycles(cycleToSyncTo, lastStoredCycleCount)

  return true
}

export async function syncCyclesBetweenCycles(lastStoredCycle = 0, cycleToSyncTo = 0): Promise<void> {
  let startCycle = lastStoredCycle
  let endCycle = startCycle + MAX_CYCLES_PER_REQUEST
  while (cycleToSyncTo > startCycle) {
    if (endCycle > cycleToSyncTo) endCycle = cycleToSyncTo
    Logger.mainLogger.debug(`Downloading cycles from ${startCycle} to ${endCycle}`)
    const res = (await queryFromArchivers(
      RequestDataType.CYCLE,
      {
        start: startCycle,
        end: endCycle,
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverCycleResponse
    if (res && res.cycleInfo) {
      const cycles = res.cycleInfo as P2PTypes.CycleCreatorTypes.CycleData[]
      Logger.mainLogger.debug(`Downloaded cycles`, cycles.length)
      for (const cycle of cycles) {
        if (!validateCycleData(cycle)) {
          Logger.mainLogger.debug('Found invalid cycle data')
          continue
        }
        processCycles([cycle])
      }
      if (res.cycleInfo.length < MAX_CYCLES_PER_REQUEST) {
        startCycle += res.cycleInfo.length
        endCycle = startCycle + MAX_CYCLES_PER_REQUEST
        break
      }
    } else {
      Logger.mainLogger.debug('Cycle', 'Invalid download response')
    }
    startCycle = endCycle + 1
    endCycle += MAX_CYCLES_PER_REQUEST
  }
}

export async function syncReceipts(): Promise<void> {
  let response: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
  if (!response || response.totalReceipts < 0) {
    return
  }
  let { totalReceipts } = response
  if (totalReceipts < 1) return
  let complete = false
  let start = 0
  let end = start + MAX_RECEIPTS_PER_REQUEST
  while (!complete) {
    if (end >= totalReceipts) {
      response = await getTotalDataFromArchivers()
      if (response && response.totalReceipts > 0) {
        if (response.totalReceipts > totalReceipts) totalReceipts = response.totalReceipts
        Logger.mainLogger.debug('totalReceiptsToSync', totalReceipts)
      }
    }
    Logger.mainLogger.debug(`Downloading receipts from ${start} to  ${end}`)
    const res = (await queryFromArchivers(
      RequestDataType.RECEIPT,
      {
        start: start,
        end: end,
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverReceiptResponse
    if (res && res.receipts) {
      const downloadedReceipts = res.receipts as ReceiptDB.Receipt[]
      Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
      await storeReceiptData(downloadedReceipts)
      if (downloadedReceipts.length < MAX_RECEIPTS_PER_REQUEST) {
        start += downloadedReceipts.length
        end = start + MAX_RECEIPTS_PER_REQUEST
        response = await getTotalDataFromArchivers()
        if (response && response.totalReceipts > 0) {
          if (response.totalReceipts > totalReceipts) totalReceipts = response.totalReceipts
          if (start === totalReceipts) {
            complete = true
            Logger.mainLogger.debug('Download receipts completed')
          }
          continue
        }
      }
    } else {
      Logger.mainLogger.debug('Invalid download response')
    }
    start = end
    end += MAX_RECEIPTS_PER_REQUEST
  }
  Logger.mainLogger.debug('Sync receipts data completed!')
}

export async function syncReceiptsByCycle(lastStoredReceiptCycle = 0, cycleToSyncTo = 0): Promise<boolean> {
  let totalCycles = cycleToSyncTo
  let totalReceipts = 0
  if (cycleToSyncTo === 0) {
    const response: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
    if (!response || response.totalReceipts < 0) {
      return false
    }
    totalCycles = response.totalCycles
    totalReceipts = response.totalReceipts
  }
  const complete = false
  let startCycle = lastStoredReceiptCycle
  let endCycle = startCycle + MAX_BETWEEN_CYCLES_PER_REQUEST
  let receiptsCountToSyncBetweenCycles = 0
  let savedReceiptsCountBetweenCycles = 0
  let totalSavedReceiptsCount = 0
  while (!complete) {
    if (endCycle > totalCycles) {
      endCycle = totalCycles
      totalSavedReceiptsCount = await ReceiptDB.queryReceiptCount()
    }
    if (cycleToSyncTo > 0) {
      if (startCycle > cycleToSyncTo) {
        Logger.mainLogger.debug(`Sync receipts data completed!`)
        break
      }
    } else {
      if (totalSavedReceiptsCount >= totalReceipts) {
        const res: any = await getTotalDataFromArchivers()
        if (res && res.totalReceipts > 0) {
          if (res.totalReceipts > totalReceipts) totalReceipts = res.totalReceipts
          if (res.totalCycles > totalCycles) totalCycles = res.totalCycles
          Logger.mainLogger.debug(
            'totalReceiptsToSync',
            totalReceipts,
            'totalSavedReceipts',
            totalSavedReceiptsCount
          )
          if (totalSavedReceiptsCount === totalReceipts) {
            Logger.mainLogger.debug('Sync receipts data completed!')
            break
          }
        }
      }
    }
    if (startCycle > endCycle) {
      Logger.mainLogger.error(
        `Got some issues in syncing receipts. Receipts query startCycle ${startCycle} is greater than endCycle ${endCycle}`
      )
      break
    }
    Logger.mainLogger.debug(`Downloading receipts from cycle ${startCycle} to cycle ${endCycle}`)
    let response = (await queryFromArchivers(
      RequestDataType.RECEIPT,
      {
        startCycle,
        endCycle,
        type: 'count',
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverReceiptCountResponse
    if (response && response.receipts > 0) {
      receiptsCountToSyncBetweenCycles = response.receipts
      let page = 1
      savedReceiptsCountBetweenCycles = 0
      while (savedReceiptsCountBetweenCycles < receiptsCountToSyncBetweenCycles) {
        const res = (await queryFromArchivers(
          RequestDataType.RECEIPT,
          {
            startCycle,
            endCycle,
            page,
          },
          QUERY_TIMEOUT_MAX
        )) as ArchiverReceiptResponse
        if (res && res.receipts) {
          const downloadedReceipts = res.receipts as ReceiptDB.Receipt[]
          Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
          await storeReceiptData(downloadedReceipts)
          savedReceiptsCountBetweenCycles += downloadedReceipts.length
          if (savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles) {
            response = (await queryFromArchivers(
              RequestDataType.RECEIPT,
              {
                startCycle,
                endCycle,
                type: 'count',
              },
              QUERY_TIMEOUT_MAX
            )) as ArchiverReceiptCountResponse
            if (response && response.receipts) receiptsCountToSyncBetweenCycles = response.receipts
            if (receiptsCountToSyncBetweenCycles > savedReceiptsCountBetweenCycles) {
              savedReceiptsCountBetweenCycles -= downloadedReceipts.length
              continue
            }
          }
          Logger.mainLogger.debug(
            'savedReceiptsCountBetweenCycles',
            savedReceiptsCountBetweenCycles,
            'receiptsCountToSyncBetweenCycles',
            receiptsCountToSyncBetweenCycles
          )
          if (savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles) {
            Logger.mainLogger.debug(
              `It has downloaded more receipts than it has in cycles between ${startCycle} and ${endCycle} !`
            )
          }
          totalSavedReceiptsCount += downloadedReceipts.length
          page++
        } else {
          Logger.mainLogger.debug('Invalid download response')
          continue
        }
      }
      Logger.mainLogger.debug(`Download receipts completed for ${startCycle} - ${endCycle}`)
      startCycle = endCycle + 1
      endCycle += MAX_BETWEEN_CYCLES_PER_REQUEST
    } else {
      receiptsCountToSyncBetweenCycles = response.receipts
      if (receiptsCountToSyncBetweenCycles === 0) {
        startCycle = endCycle + 1
        endCycle += MAX_BETWEEN_CYCLES_PER_REQUEST
        continue
      }
      Logger.mainLogger.debug('Invalid download response')
      continue
    }
  }
  return false
}

export const syncOriginalTxs = async (): Promise<void> => {
  let response: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
  let { totalOriginalTxs } = response
  if (totalOriginalTxs < 1) return
  let complete = false
  let start = 0
  let end = start + MAX_ORIGINAL_TXS_PER_REQUEST
  while (!complete) {
    if (end >= totalOriginalTxs) {
      // If the number of new original txs to sync is within MAX_ORIGINAL_TXS_PER_REQUEST => Update to the latest totalOriginalTxs.
      response = await getTotalDataFromArchivers()
      if (response && response.totalOriginalTxs > 0) {
        if (response.totalOriginalTxs > totalOriginalTxs) totalOriginalTxs = response.totalOriginalTxs
        Logger.mainLogger.debug('totalOriginalTxs: ', totalOriginalTxs)
      }
    }
    Logger.mainLogger.debug(`Downloading Original-Txs from ${start} to ${end}`)
    const res: any = await queryFromArchivers(
      RequestDataType.ORIGINALTX,
      {
        start: start,
        end: end,
      },
      QUERY_TIMEOUT_MAX
    )
    if (res && res.originalTxs) {
      const downloadedOriginalTxs = res.originalTxs
      Logger.mainLogger.debug('Downloaded Original-Txs: ', downloadedOriginalTxs.length)
      await storeOriginalTxData(downloadedOriginalTxs)
      if (downloadedOriginalTxs.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
        start += downloadedOriginalTxs.length
        end = start + MAX_ORIGINAL_TXS_PER_REQUEST
        response = await getTotalDataFromArchivers()
        if (response && response.totalOriginalTxs > 0) {
          if (response.totalOriginalTxs > totalOriginalTxs) totalOriginalTxs = response.totalOriginalTxs
          if (start === totalOriginalTxs) {
            complete = true
            Logger.mainLogger.debug('Download Original-Txs Completed!')
          }
          continue
        }
      }
    } else {
      Logger.mainLogger.debug('Invalid Original-Txs download response')
    }
    start = end
    end += MAX_ORIGINAL_TXS_PER_REQUEST
  }
  Logger.mainLogger.debug('Sync Original-Txs Data Completed!')
}

export const syncOriginalTxsByCycle = async (
  lastStoredOriginalTxCycle = 0,
  cycleToSyncTo = 0
): Promise<void> => {
  let totalCycles = cycleToSyncTo
  let totalOriginalTxs = 0
  if (cycleToSyncTo === 0) {
    const response: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
    if (!response || response.totalOriginalTxs < 1) {
      return
    }
    totalCycles = response.totalCycles
    totalOriginalTxs = response.totalOriginalTxs
  }
  const complete = false
  let startCycle = lastStoredOriginalTxCycle
  let endCycle = startCycle + MAX_BETWEEN_CYCLES_PER_REQUEST
  let originalTxCountToSyncBetweenCycles = 0
  let savedOriginalTxCountBetweenCycles = 0
  let totalSavedOriginalTxCount = 0
  while (!complete) {
    if (endCycle > totalCycles) {
      endCycle = totalCycles
      totalSavedOriginalTxCount = await OriginalTxDB.queryOriginalTxDataCount()
    }
    if (cycleToSyncTo > 0) {
      if (startCycle > cycleToSyncTo) {
        Logger.mainLogger.debug(`Sync originalTXs data completed!`)
        break
      }
    } else {
      if (totalSavedOriginalTxCount >= totalOriginalTxs) {
        const res: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
        if (res && res.totalOriginalTxs > 0) {
          if (res.totalOriginalTxs > totalOriginalTxs) totalOriginalTxs = res.totalOriginalTxs
          if (res.totalCycles > totalCycles) totalCycles = res.totalCycles
          Logger.mainLogger.debug(
            'totalOriginalTxsToSync: ',
            totalOriginalTxs,
            'totalSavedOriginalTxs: ',
            totalSavedOriginalTxCount
          )
          if (totalSavedOriginalTxCount === totalOriginalTxs) {
            Logger.mainLogger.debug('Sync Original-Tx data completed!')
            break
          }
        }
      }
    }
    if (startCycle > endCycle) {
      Logger.mainLogger.error(
        `Got some issues in syncing Original-Tx data. Original-Tx query startCycle ${startCycle} is greater than endCycle ${endCycle}`
      )
      break
    }
    Logger.mainLogger.debug(`Downloading Original-Tx data from cycle ${startCycle} to cycle ${endCycle}`)
    let response = (await queryFromArchivers(
      RequestDataType.ORIGINALTX,
      {
        startCycle,
        endCycle,
        type: 'count',
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverOriginalTxCountResponse
    if (response && response.originalTxs > 0) {
      originalTxCountToSyncBetweenCycles = response.originalTxs
      let page = 1
      savedOriginalTxCountBetweenCycles = 0
      while (savedOriginalTxCountBetweenCycles < originalTxCountToSyncBetweenCycles) {
        const res = (await queryFromArchivers(
          RequestDataType.ORIGINALTX,
          {
            startCycle,
            endCycle,
            page,
          },
          QUERY_TIMEOUT_MAX
        )) as ArchiverOriginalTxResponse
        if (res && res.originalTxs) {
          const downloadedOriginalTxs = res.originalTxs as OriginalTxDB.OriginalTxData[]
          Logger.mainLogger.debug('Downloaded Original-Txs: ', downloadedOriginalTxs.length)
          await storeOriginalTxData(downloadedOriginalTxs)
          savedOriginalTxCountBetweenCycles += downloadedOriginalTxs.length
          if (savedOriginalTxCountBetweenCycles > originalTxCountToSyncBetweenCycles) {
            response = (await queryFromArchivers(
              RequestDataType.ORIGINALTX,
              {
                startCycle,
                endCycle,
                type: 'count',
              },
              QUERY_TIMEOUT_MAX
            )) as ArchiverOriginalTxCountResponse
            if (response && response.originalTxs) originalTxCountToSyncBetweenCycles = response.originalTxs
            if (originalTxCountToSyncBetweenCycles > savedOriginalTxCountBetweenCycles) {
              savedOriginalTxCountBetweenCycles -= downloadedOriginalTxs.length
              continue
            }
          }
          Logger.mainLogger.debug(
            'savedOriginalTxCountBetweenCycles',
            savedOriginalTxCountBetweenCycles,
            'originalTxCountToSyncBetweenCycles',
            originalTxCountToSyncBetweenCycles
          )
          if (savedOriginalTxCountBetweenCycles > originalTxCountToSyncBetweenCycles) {
            Logger.mainLogger.debug(
              `It has downloaded more originalTxsData than it has in cycles between ${startCycle} and ${endCycle} !`
            )
          }
          totalSavedOriginalTxCount += downloadedOriginalTxs.length
          page++
        } else {
          Logger.mainLogger.debug('Invalid Original-Txs download response')
          continue
        }
      }
      Logger.mainLogger.debug(`Download Original-Txs completed for ${startCycle} - ${endCycle}`)
      startCycle = endCycle + 1
      endCycle += MAX_BETWEEN_CYCLES_PER_REQUEST
    } else {
      originalTxCountToSyncBetweenCycles = response.originalTxs
      if (originalTxCountToSyncBetweenCycles === 0) {
        startCycle = endCycle + 1
        endCycle += MAX_BETWEEN_CYCLES_PER_REQUEST
        continue
      }
      Logger.mainLogger.debug('Invalid Original-Txs download response')
      continue
    }
  }
}

export const syncCyclesAndTxsData = async (
  lastStoredCycleCount = 0,
  lastStoredReceiptCount = 0,
  lastStoredOriginalTxCount = 0
): Promise<void> => {
  let response: ArchiverTotalDataResponse = await getTotalDataFromArchivers()
  if (!response || response.totalCycles < 0 || response.totalReceipts < 0) {
    return
  }
  const { totalCycles, totalReceipts, totalOriginalTxs } = response
  Logger.mainLogger.debug('totalCycles', totalCycles, 'lastStoredCycleCount', lastStoredCycleCount)
  Logger.mainLogger.debug('totalReceipts', totalReceipts, 'lastStoredReceiptCount', lastStoredReceiptCount)
  Logger.mainLogger.debug(
    'totalOriginalTxs',
    totalOriginalTxs,
    'lastStoredOriginalTxCount',
    lastStoredOriginalTxCount
  )
  if (
    totalCycles === lastStoredCycleCount &&
    totalReceipts === lastStoredReceiptCount &&
    totalOriginalTxs === lastStoredOriginalTxCount
  ) {
    Logger.mainLogger.debug('The archiver has synced the lastest cycle ,receipts and originalTxs data!')
    return
  }
  let totalReceiptsToSync = totalReceipts
  let totalOriginalTxsToSync = totalOriginalTxs
  let totalCyclesToSync = totalCycles
  let completeForReceipt = false
  let completeForOriginalTx = false
  let completeForCycle = false
  let startReceipt = lastStoredReceiptCount
  let startOriginalTx = lastStoredOriginalTxCount
  let startCycle = lastStoredCycleCount
  let endReceipt = startReceipt + MAX_RECEIPTS_PER_REQUEST
  let endOriginalTx = startOriginalTx + MAX_ORIGINAL_TXS_PER_REQUEST
  let endCycle = startCycle + MAX_CYCLES_PER_REQUEST

  if (totalCycles === lastStoredCycleCount) completeForCycle = true
  if (totalReceipts === lastStoredReceiptCount) completeForReceipt = true
  if (totalOriginalTxs === lastStoredOriginalTxCount) completeForOriginalTx = true

  while (!completeForReceipt || !completeForCycle || !completeForOriginalTx) {
    if (
      endReceipt >= totalReceiptsToSync ||
      endCycle >= totalCyclesToSync ||
      endOriginalTx >= totalOriginalTxsToSync
    ) {
      response = await getTotalDataFromArchivers()
      if (response && response.totalReceipts && response.totalCycles && response.totalOriginalTxs) {
        if (response.totalReceipts !== totalReceiptsToSync) {
          completeForReceipt = false
          totalReceiptsToSync = response.totalReceipts
        }
        if (response.totalOriginalTxs !== totalOriginalTxsToSync) {
          completeForOriginalTx = false
          totalOriginalTxsToSync = response.totalOriginalTxs
        }
        if (response.totalCycles !== totalCyclesToSync) {
          completeForCycle = false
          totalCyclesToSync = response.totalCycles
        }
        if (totalReceiptsToSync < startReceipt) {
          completeForReceipt = true
        }
        if (totalOriginalTxsToSync < startOriginalTx) {
          completeForOriginalTx = true
        }
        if (totalCyclesToSync < startCycle) {
          completeForCycle = true
        }
        Logger.mainLogger.debug(
          'totalReceiptsToSync',
          totalReceiptsToSync,
          'totalOriginalTxsToSync',
          totalOriginalTxsToSync,
          'totalCyclesToSync',
          totalCyclesToSync
        )
      }
    }
    if (!completeForReceipt) {
      Logger.mainLogger.debug(`Downloading receipts from ${startReceipt} to ${endReceipt}`)
      const res = (await queryFromArchivers(
        RequestDataType.RECEIPT,
        {
          start: startReceipt,
          end: endReceipt,
        },
        QUERY_TIMEOUT_MAX
      )) as ArchiverReceiptResponse
      if (res && res.receipts) {
        const downloadedReceipts = res.receipts as ReceiptDB.Receipt[]
        Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
        await storeReceiptData(downloadedReceipts)
        if (downloadedReceipts.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
          startReceipt += downloadedReceipts.length + 1
          endReceipt += downloadedReceipts.length + MAX_ORIGINAL_TXS_PER_REQUEST
          continue
        }
      } else {
        Logger.mainLogger.debug('Invalid download response')
      }
      startReceipt = endReceipt + 1
      endReceipt += MAX_ORIGINAL_TXS_PER_REQUEST
    }
    if (!completeForOriginalTx) {
      Logger.mainLogger.debug(`Downloading Original-Txs from ${startOriginalTx} to ${endOriginalTx}`)
      const res = (await queryFromArchivers(
        RequestDataType.ORIGINALTX,
        {
          start: startOriginalTx,
          end: endOriginalTx,
        },
        QUERY_TIMEOUT_MAX
      )) as ArchiverOriginalTxResponse
      if (res && res.originalTxs) {
        const downloadedOriginalTxs = res.originalTxs as OriginalTxDB.OriginalTxData[]
        Logger.mainLogger.debug(`Downloaded Original-Txs: `, downloadedOriginalTxs.length)
        await storeOriginalTxData(downloadedOriginalTxs)
        if (downloadedOriginalTxs.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
          startOriginalTx += downloadedOriginalTxs.length + 1
          endOriginalTx += downloadedOriginalTxs.length + MAX_ORIGINAL_TXS_PER_REQUEST
          continue
        }
      } else {
        Logger.mainLogger.debug('Invalid Original-Tx download response')
      }
      startOriginalTx = endOriginalTx + 1
      endOriginalTx += MAX_ORIGINAL_TXS_PER_REQUEST
    }
    if (!completeForCycle) {
      Logger.mainLogger.debug(`Downloading cycles from ${startCycle} to ${endCycle}`)
      const res = (await queryFromArchivers(
        RequestDataType.CYCLE,
        {
          start: startCycle,
          end: endCycle,
        },
        QUERY_TIMEOUT_MAX
      )) as ArchiverCycleResponse
      if (res && res.cycleInfo) {
        const cycles = res.cycleInfo
        Logger.mainLogger.debug(`Downloaded cycles`, cycles.length)
        for (const cycle of cycles) {
          if (!validateCycleData(cycle)) {
            Logger.mainLogger.debug('Found invalid cycle data')
            continue
          }
          processCycles([cycle])
        }
        if (cycles.length < MAX_CYCLES_PER_REQUEST) {
          startCycle += cycles.length + 1
          endCycle += cycles.length + MAX_CYCLES_PER_REQUEST
          continue
        }
      } else {
        Logger.mainLogger.debug('Cycle', 'Invalid download response')
      }
      startCycle = endCycle + 1
      endCycle += MAX_CYCLES_PER_REQUEST
    }
  }
  Logger.mainLogger.debug('Sync Cycle, Receipt & Original-Tx data completed!')
}

export const syncCyclesAndTxsDataBetweenCycles = async (
  lastStoredCycle = 0,
  cycleToSyncTo = 0
): Promise<void> => {
  Logger.mainLogger.debug(
    `Syncing cycles and txs data between cycles ${lastStoredCycle} and ${cycleToSyncTo}`
  )
  await syncCyclesBetweenCycles(lastStoredCycle, cycleToSyncTo)
  await syncReceiptsByCycle(lastStoredCycle, cycleToSyncTo)
  await syncOriginalTxsByCycle(lastStoredCycle, cycleToSyncTo)
}

// // simple method to validate old data; it's not good when there are multiple archivers, the receipts saving order may not be the same
// export async function compareWithOldReceiptsData(
//   archiver: State.ArchiverNodeInfo,
//   lastReceiptCount = 0
// ) {
//   let downloadedReceipts
//   const response: any = await P2P.getJson(
//     `http://${archiver.ip}:${archiver.port}/receipt?start=${
//       lastReceiptCount - 10 > 0 ? lastReceiptCount - 10 : 0
//     }&end=${lastReceiptCount}`
//   )
//   if (response && response.receipts) {
//     downloadedReceipts = response.receipts
//   } else {
//     throw Error(
//       `Can't fetch data from receipt ${
//         lastReceiptCount - 10 > 0 ? lastReceiptCount - 10 : 0
//       } to receipt ${lastReceiptCount}  from archiver ${archiver}`
//     )
//   }
//   let oldReceipts = await ReceiptDB.queryReceipts(
//     lastReceiptCount - 10 > 0 ? lastReceiptCount - 10 : 0,
//     lastReceiptCount
//   )
//   // downloadedReceipts.sort((a, b) =>
//   //   a.cycleRecord.counter > b.cycleRecord.counter ? 1 : -1
//   // );
//   // oldReceipts.sort((a, b) =>
//   //   a.cycleRecord.counter > b.cycleRecord.counter ? 1 : -1
//   // );
//   let success = false
//   let receiptsToMatchCount = 10
//   for (let i = 0; i < downloadedReceipts.length; i++) {
//     let downloadedReceipt = downloadedReceipts[i]
//     const oldReceipt = oldReceipts[i]
//     if (oldReceipt.counter) delete oldReceipt.counter
//     console.log(downloadedReceipt.receiptId, oldReceipt.receiptId)
//     if (downloadedReceipt.receiptId !== oldReceipt.receiptId) {
//       return {
//         success,
//         receiptsToMatchCount,
//       }
//     }
//     success = true
//     receiptsToMatchCount--
//   }
//   return { success, receiptsToMatchCount }
// }
export async function compareWithOldOriginalTxsData(lastStoredOriginalTxCycle = 0): Promise<CompareResponse> {
  const numberOfCyclesTocompare = 10
  let success = false
  let matchedCycle = 0
  const endCycle = lastStoredOriginalTxCycle
  const startCycle = endCycle - numberOfCyclesTocompare > 0 ? endCycle - numberOfCyclesTocompare : 0
  const response = (await queryFromArchivers(
    RequestDataType.ORIGINALTX,
    {
      startCycle,
      endCycle,
      type: 'tally',
    },
    QUERY_TIMEOUT_MAX
  )) as ArchiverOriginalTxResponse

  if (!response || !response.originalTxs) {
    Logger.mainLogger.error(
      `Can't fetch original tx data from cycle ${startCycle} to cycle ${endCycle} from archivers`
    )
    return { success, matchedCycle }
  }
  const downloadedOriginalTxsByCycles = response.originalTxs as OriginalTxDB.OriginalTxDataCount[]

  const oldOriginalTxCountByCycle = await OriginalTxDB.queryOriginalTxDataCountByCycles(startCycle, endCycle)

  for (let i = 0; i < downloadedOriginalTxsByCycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const downloadedOriginalTx = downloadedOriginalTxsByCycles[i]
    // eslint-disable-next-line security/detect-object-injection
    const oldOriginalTx = oldOriginalTxCountByCycle[i]
    Logger.mainLogger.debug(downloadedOriginalTx, oldOriginalTx)
    if (
      !downloadedOriginalTx ||
      !oldOriginalTx ||
      downloadedOriginalTx.cycle !== oldOriginalTx.cycle ||
      downloadedOriginalTx.originalTxDataCount !== oldOriginalTx.originalTxDataCount
    ) {
      return {
        success,
        matchedCycle,
      }
    }
    success = true
    matchedCycle = downloadedOriginalTx.cycle
  }
  success = true
  return { success, matchedCycle }
}

export async function compareWithOldReceiptsData(lastStoredReceiptCycle = 0): Promise<CompareResponse> {
  const numberOfCyclesTocompare = 10
  let success = false
  let matchedCycle = 0
  const endCycle = lastStoredReceiptCycle
  const startCycle = endCycle - numberOfCyclesTocompare > 0 ? endCycle - numberOfCyclesTocompare : 0
  const response = (await queryFromArchivers(
    RequestDataType.RECEIPT,
    {
      startCycle,
      endCycle,
      type: 'tally',
    },
    QUERY_TIMEOUT_MAX
  )) as ArchiverReceiptResponse

  if (!response || !response.receipts) {
    Logger.mainLogger.error(
      `Can't fetch receipts data from cycle ${startCycle} to cycle ${endCycle}  from archivers`
    )
    return { success, matchedCycle }
  }
  const downloadedReceiptCountByCycles = response.receipts as ReceiptDB.ReceiptCount[]

  const oldReceiptCountByCycle = await ReceiptDB.queryReceiptCountByCycles(startCycle, endCycle)
  for (let i = 0; i < downloadedReceiptCountByCycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const downloadedReceipt = downloadedReceiptCountByCycles[i]
    // eslint-disable-next-line security/detect-object-injection
    const oldReceipt = oldReceiptCountByCycle[i]
    Logger.mainLogger.debug(downloadedReceipt, oldReceipt)
    if (
      !downloadedReceipt ||
      !oldReceipt ||
      downloadedReceipt.cycle !== oldReceipt.cycle ||
      downloadedReceipt.receiptCount !== oldReceipt.receiptCount
    ) {
      return {
        success,
        matchedCycle,
      }
    }
    success = true
    matchedCycle = downloadedReceipt.cycle
  }
  success = true
  return { success, matchedCycle }
}

export async function compareWithOldCyclesData(lastCycleCounter = 0): Promise<CompareResponse> {
  const numberOfCyclesTocompare = 10
  const start = lastCycleCounter - numberOfCyclesTocompare
  const end = lastCycleCounter
  const response = (await queryFromArchivers(
    RequestDataType.CYCLE,
    {
      start,
      end,
    },
    QUERY_TIMEOUT_MAX
  )) as ArchiverCycleResponse
  if (!response && !response.cycleInfo) {
    throw Error(`Can't fetch data from cycle ${start} to cycle ${end}  from archivers`)
  }
  const downloadedCycles = response.cycleInfo
  const oldCycles = await CycleDB.queryCycleRecordsBetween(start, end)
  let success = false
  let matchedCycle = 0
  for (let i = 0; i < downloadedCycles.length; i++) {
    // eslint-disable-next-line security/detect-object-injection
    const downloadedCycle = downloadedCycles[i]
    // eslint-disable-next-line security/detect-object-injection
    const oldCycle = oldCycles[i]
    if (
      !downloadedCycle ||
      !oldCycle ||
      StringUtils.safeStringify(downloadedCycle) !== StringUtils.safeStringify(oldCycle)
    ) {
      console.log('Mismatched cycle Number', downloadedCycle.counter, oldCycle.counter)
      return {
        success,
        matchedCycle,
      }
    }
    success = true
    matchedCycle = downloadedCycle.counter
  }
  return { success, matchedCycle }
}

async function downloadOldCycles(
  cycleToSyncTo: P2PTypes.CycleCreatorTypes.CycleData,
  lastStoredCycleCount: number
): Promise<void> {
  let endCycle = cycleToSyncTo.counter - 1
  Logger.mainLogger.debug('endCycle counter', endCycle, 'lastStoredCycleCount', lastStoredCycleCount)
  if (endCycle > lastStoredCycleCount) {
    Logger.mainLogger.debug(
      `Downloading old cycles from cycles ${lastStoredCycleCount} to cycle ${endCycle}!`
    )
  }

  let savedCycleRecord = cycleToSyncTo
  const MAX_RETRY_COUNT = 3
  let retryCount = 0
  while (endCycle > lastStoredCycleCount) {
    let startCycle: number = endCycle - MAX_CYCLES_PER_REQUEST
    if (startCycle < 0) startCycle = 0
    if (startCycle < lastStoredCycleCount) startCycle = lastStoredCycleCount
    Logger.mainLogger.debug(`Getting cycles ${startCycle} - ${endCycle} ...`)
    const res = (await queryFromArchivers(
      RequestDataType.CYCLE,
      {
        start: startCycle,
        end: endCycle,
      },
      QUERY_TIMEOUT_MAX
    )) as ArchiverCycleResponse
    if (!res || !res.cycleInfo) {
      Logger.mainLogger.error(
        `Can't fetch data from cycle ${startCycle} to cycle ${endCycle}  from archivers`
      )
      if (retryCount < MAX_RETRY_COUNT) {
        retryCount++
        continue
      } else {
        endCycle = startCycle - 1
        retryCount = 0
      }
    }
    const prevCycles = res.cycleInfo as P2PTypes.CycleCreatorTypes.CycleData[]
    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    const combineCycles: P2PTypes.CycleCreatorTypes.CycleData[] = []
    for (const prevCycle of prevCycles) {
      if (validateCycle(prevCycle, savedCycleRecord) === false) {
        Logger.mainLogger.error(`Record ${prevCycle.counter} failed validation`)
        Logger.mainLogger.debug('fail', prevCycle, savedCycleRecord)
      }
      savedCycleRecord = prevCycle
      combineCycles.push(prevCycle)
    }
    await storeCycleData(combineCycles)
    endCycle = startCycle - 1
  }
}
