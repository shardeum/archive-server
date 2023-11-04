import { EventEmitter } from 'events'
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Cycles from './Cycles'
import {
  currentCycleCounter,
  currentCycleDuration,
  Cycle,
  processCycles,
  validateCycle,
  fetchCycleRecords,
  getNewestCycleFromArchivers,
  getNewestCycleFromConsensors,
} from './Cycles'
import { ChangeSquasher, parse, totalNodeCount, activeNodeCount, applyNodeListChange } from './CycleParser'
import * as State from '../State'
import * as P2P from '../P2P'
import * as Utils from '../Utils'
import { config } from '../Config'
import { P2P as P2PTypes } from '@shardus/types'
import * as Logger from '../Logger'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import { profilerInstance } from '../profiler/profiler'
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
import {
  MAX_ACCOUNTS_PER_REQUEST,
  MAX_RECEIPTS_PER_REQUEST,
  MAX_ORIGINAL_TXS_PER_REQUEST,
  MAX_CYCLES_PER_REQUEST,
  MAX_BETWEEN_CYCLES_PER_REQUEST,
} from '../server'
import * as GossipData from './GossipData'

// Socket modules
export let socketServer: SocketIO.Server
let ioclient: SocketIOClientStatic = require('socket.io-client')
export let socketClients: Map<string, SocketIOClientStatic['Socket']> = new Map()
export let combineAccountsData = {
  accounts: [],
  receipts: [],
}
let forwardGenesisAccounts = true
let currentConsensusRadius = 0
let subsetNodesMapByConsensusRadius: Map<number, NodeList.ConsensusNodeInfo[]> = new Map()
let receivedCycleTracker = {}
const maxCyclesInCycleTracker = 10
const maxCyclesToPreserve = 5

const QUERY_TIMEOUT_MAX = 30 // 30seconds

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

export interface DataQueryResponse {
  success: boolean
  data: any
}

export function createDataRequest<T extends P2PTypes.SnapshotTypes.ValidTypes>(
  type: P2PTypes.SnapshotTypes.TypeName<T>,
  lastData: P2PTypes.SnapshotTypes.TypeIndex<T>,
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

export function initSocketServer(io: SocketIO.Server) {
  socketServer = io
  socketServer.on('connection', (socket: SocketIO.Socket) => {
    Logger.mainLogger.debug('Explorer has connected')
  })
}

export async function unsubscribeDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
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

export function initSocketClient(node: NodeList.ConsensusNodeInfo) {
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

  socketClient.on(
    'DATA',
    (newData: DataResponse<P2PTypes.SnapshotTypes.ValidTypes> & Crypto.TaggedMessage) => {
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
          Logger.mainLogger.debug('Clearing contact timeout.')
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
            sender.nodeInfo.ip + ':' + sender.nodeInfo.port
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
          storeReceiptData(newData.responses.RECEIPT, sender.nodeInfo.ip + ':' + sender.nodeInfo.port)
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
              let newCombineAccountsData: any = { ...combineAccountsData }
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
              newCombineAccountsData = {}
            }
            // console.log(newData.responses.ACCOUNT)
            else storeAccountData(newData.responses.ACCOUNT)
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
    }
  )
}

export function collectCycleData(cycleData: Cycle[], senderInfo: string = '') {
  for (const cycle of cycleData) {
    // Logger.mainLogger.debug('Cycle received', cycle.counter, senderInfo)
    let cycleToSave = []
    if (receivedCycleTracker[cycle.counter]) {
      if (receivedCycleTracker[cycle.counter][cycle.marker])
        receivedCycleTracker[cycle.counter][cycle.marker]['receivedTimes']++
      else {
        receivedCycleTracker[cycle.counter][cycle.marker] = {
          cycleInfo: cycle,
          receivedTimes: 1,
          saved: false,
        }
      }
    } else {
      receivedCycleTracker[cycle.counter] = {
        [cycle.marker]: {
          cycleInfo: cycle,
          receivedTimes: 1,
          saved: false,
        },
      }
    }
    // Logger.mainLogger.debug('Cycle received', cycle.counter, receivedCycleTracker)
    let maxEqual = 3 // Setting as 3 for now, TODO: set with a better value using consensusRadius
    if (cycle.active < 10) maxEqual = 1
    for (let value of Object.values(receivedCycleTracker[cycle.counter])) {
      if (value['saved']) break
      if (value['receivedTimes'] >= maxEqual) {
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
      if (parseInt(counter) < currentCycleCounter - maxCyclesToPreserve) {
        let totalTimes = 0
        let logCycle = false
        // If there is more than one marker for this cycle, output the cycle log
        if (Object.keys(receivedCycleTracker[counter]).length > 1) logCycle = true
        for (const key of Object.keys(receivedCycleTracker[counter])) {
          Logger.mainLogger.debug(
            'Cycle',
            counter,
            key, // marker
            receivedCycleTracker[counter][key]['receivedTimes'],
            logCycle ? receivedCycleTracker[counter][key] : ''
          )
          totalTimes += receivedCycleTracker[counter][key]['receivedTimes']
        }
        Logger.mainLogger.debug(`Received ${totalTimes} times for cycle counter ${counter}`)
        delete receivedCycleTracker[counter]
      }
    }
  }
}

export function clearCombinedAccountsData() {
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

export async function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  nestedCountersInstance.countEvent('archiver', 'replace_data_sender')
  if (NodeList.getActiveList().length < 2) {
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
  const node = NodeList.byPublicKey[publicKey]
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

      // Check if there is any subscribed node from this subset
      let foundSubscribedNodeFromThisSubset = false
      for (let node of Object.values(subsetNodesList)) {
        if (dataSenders.has(node.publicKey)) {
          if (config.VERBOSE) Logger.mainLogger.debug('This node from the subset is in the subscribed list!')
          if (foundSubscribedNodeFromThisSubset) {
            // Unsubscribe the extra nodes from this subset
            unsubscribeDataSender(node.publicKey)
          }
          foundSubscribedNodeFromThisSubset = true
        }
      }

      if (!foundSubscribedNodeFromThisSubset) {
        Logger.mainLogger.debug('There is no subscribed node from this subset!')
        // Pick a new dataSender from this subset
        subscribeNodeFromThisSubset(subsetNodesList)
      }
    }
  }
}

export async function subscribeNodeForDataTransfer() {
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
  msg: string = '',
  ms: number = 0
) {
  if (!ms) ms = 15 * 1000 // Change contact timeout to 15s for now
  Logger.mainLogger.debug('Created contact timeout: ' + ms, `for ${publicKey}`)
  nestedCountersInstance.countEvent('archiver', 'contact_timeout_created')
  return setTimeout(() => {
    // Logger.mainLogger.debug('nestedCountersInstance', nestedCountersInstance)
    if (nestedCountersInstance) nestedCountersInstance.countEvent('archiver', 'contact_timeout')
    Logger.mainLogger.debug('REPLACING sender due to CONTACT timeout', msg, publicKey)
    replaceDataSender(publicKey)
  }, ms)
}

export function addDataSender(sender: DataSender) {
  dataSenders.set(sender.nodeInfo.publicKey, sender)
}

async function getConsensusRadius() {
  const activeList = NodeList.getActiveList()
  let randomNode = activeList[Math.floor(Math.random() * activeList.length)]
  Logger.mainLogger.debug(`Checking network configs from random node ${randomNode.ip}:${randomNode.port}`)
  // TODO: Should try to get the network config from multiple nodes and use the consensusRadius that has the majority
  const REQUEST_NETCONFIG_TIMEOUT_SECOND = 2 // 2s timeout
  let response: any = await P2P.getJson(
    `http://${randomNode.ip}:${randomNode.port}/netconfig`,
    REQUEST_NETCONFIG_TIMEOUT_SECOND
  )
  if (response && response.config) {
    let nodesPerConsensusGroup = response.config.sharding.nodesPerConsensusGroup
    // Upgrading consensus size to odd number
    if (nodesPerConsensusGroup % 2 === 0) nodesPerConsensusGroup++
    const consensusRadius = Math.floor((nodesPerConsensusGroup - 1) / 2)
    Logger.mainLogger.debug('consensusRadius', consensusRadius)
    if (config.VERBOSE) console.log('consensusRadius', consensusRadius)
    return consensusRadius
  }
  return currentConsensusRadius
}

export async function createDataTransferConnection(newSenderInfo: NodeList.ConsensusNodeInfo) {
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

export async function createNodesGroupByConsensusRadius() {
  // There is only one active node in the network. no need to create groups
  if (NodeList.getActiveList().length < 2) return
  const consensusRadius = await getConsensusRadius()
  currentConsensusRadius = consensusRadius
  const activeList = [...NodeList.activeListByIdSorted]
  if (config.VERBOSE) Logger.mainLogger.debug('activeList', activeList.length, activeList)
  const totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
  Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  subsetNodesMapByConsensusRadius = new Map()
  let round = 0
  for (let i = 0; i < activeList.length; i += consensusRadius) {
    let subsetList: NodeList.ConsensusNodeInfo[] = activeList.slice(i, i + consensusRadius)
    subsetNodesMapByConsensusRadius.set(round, subsetList)
    round++
  }
  if (config.VERBOSE)
    Logger.mainLogger.debug('subsetNodesMapByConsensusRadius', subsetNodesMapByConsensusRadius)
}

export async function subscribeConsensorsByConsensusRadius() {
  await createNodesGroupByConsensusRadius()
  for (const [i, subsetList] of subsetNodesMapByConsensusRadius) {
    if (config.VERBOSE) Logger.mainLogger.debug('Round', i, 'subsetList', subsetList, dataSenders.keys())
    let foundSubscribedNodeFromThisSubset = false
    for (let node of Object.values(subsetList)) {
      if (dataSenders.has(node.publicKey)) {
        if (config.VERBOSE) Logger.mainLogger.debug('This node from the subset is in the subscribed list!')
        if (foundSubscribedNodeFromThisSubset) {
          // Unsubscribe the extra nodes from this subset
          unsubscribeDataSender(node.publicKey)
        }
        foundSubscribedNodeFromThisSubset = true
      }
    }

    if (!foundSubscribedNodeFromThisSubset) {
      Logger.mainLogger.debug('There is no subscribed node from this subset!')
      // Pick a new dataSender from this subset
      subscribeNodeFromThisSubset(subsetList)
    }
  }
}

export async function subscribeNodeFromThisSubset(nodeList: NodeList.ConsensusNodeInfo[]) {
  let subsetList = [...nodeList]
  // Pick a random dataSender
  let newSenderInfo = nodeList[Math.floor(Math.random() * nodeList.length)]
  let connectionStatus = false
  let retry = 0
  const MAX_RETRY_SUBSCRIPTION = 3
  while (retry < MAX_RETRY_SUBSCRIPTION) {
    if (!dataSenders.has(newSenderInfo.publicKey)) {
      connectionStatus = await createDataTransferConnection(newSenderInfo)
      if (connectionStatus) {
        break
      } else {
        subsetList = subsetList.filter((node) => node.publicKey !== newSenderInfo.publicKey)
      }
    } else {
      // This means there is already a subscribed node from this subset
      break
    }
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
) {
  const dataRequest = {
    dataRequestCycle: currentCycleCounter,
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
  let response = await P2P.postJson(
    `http://${nodeInfo.ip}:${nodeInfo.port}/requestdata`,
    taggedDataRequest,
    REQUEST_DATA_TIMEOUT_SECOND
  )
  Logger.mainLogger.debug('/requestdata response', response, nodeInfo.ip + ':' + nodeInfo.port)
  if (response && response.success) reply = response.success
  return reply
}

export const clearDataSenders = async () => {
  for (const [publicKey] of dataSenders) {
    unsubscribeDataSender(publicKey)
  }
  await Utils.sleep(2000) // Wait for 2s to make sure all dataSenders are unsubscribed
  dataSenders.clear()
  socketClients.clear()
  subsetNodesMapByConsensusRadius.clear()
}

function calcIncomingTimes(record: Cycle) {
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
  isFirstTime: boolean,
  checkFromConsensor: boolean = false
): Promise<boolean> {
  Logger.mainLogger.debug('Is firstTime', isFirstTime)
  if (!isFirstTime) {
    let isJoined: boolean
    if (checkFromConsensor) isJoined = await checkJoinStatusFromConsensor(nodeList)
    else isJoined = await checkJoinStatus()
    if (isJoined) {
      return isJoined
    }
  }
  Logger.mainLogger.debug('nodeList To Submit Join Request', nodeList)
  // try to get latestCycleRecord with a robust query
  const latestCycle = await getNewestCycleFromConsensors(nodeList)

  // Figure out when Q1 is from the latestCycle
  const { startQ1 } = calcIncomingTimes(latestCycle)
  let shuffledNodes = [...nodeList]
  Utils.shuffleArray(shuffledNodes)

  // Wait until a Q1 then send join request to active nodes
  let untilQ1 = startQ1 - Date.now()
  while (untilQ1 < 0) {
    untilQ1 += latestCycle.duration * 1000
  }

  Logger.mainLogger.debug(`Waiting ${untilQ1 + 500} ms for Q1 before sending join...`)
  await Utils.sleep(untilQ1 + 500) // Not too early

  // Create a fresh join request, so that the request timestamp range is acceptable
  let request = P2P.createArchiverJoinRequest()
  await submitJoin(nodeList, request)

  // Wait approx. one cycle then check again
  Logger.mainLogger.debug('Waiting approx. one cycle then checking again...')
  await Utils.sleep(latestCycle.duration * 1000 + 500)
  return false
}

export async function submitJoin(
  nodes: NodeList.ConsensusNodeInfo[],
  joinRequest: P2P.ArchiverJoinRequest & Crypto.types.SignedObject
) {
  // Send the join request to a handful of the active node all at once:w
  const selectedNodes = Utils.getRandom(nodes, Math.min(nodes.length, 5))
  Logger.mainLogger.debug(`Sending join request to ${selectedNodes.map((n) => `${n.ip}:${n.port}`)}`)
  for (const node of selectedNodes) {
    let response = await P2P.postJson(`http://${node.ip}:${node.port}/joinarchiver`, joinRequest)
    Logger.mainLogger.debug('Join request response:', response)
  }
}

export async function sendLeaveRequest(nodes: NodeList.ConsensusNodeInfo[]) {
  let leaveRequest = P2P.createArchiverLeaveRequest()
  Logger.mainLogger.debug(`Sending leave request to ${nodes.map((n) => `${n.ip}:${n.port}`)}`)

  const promises = nodes.map((node) =>
    fetch(`http://${node.ip}:${node.port}/leavingarchivers`, {
      method: 'post',
      body: JSON.stringify(leaveRequest),
      headers: { 'Content-Type': 'application/json' },
      timeout: 2 * 1000, // 2s timeout
    }).then((res) => res.json())
  )

  await Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      let isLeaveRequestSent = false
      for (const response of responses) {
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

export async function sendActiveRequest() {
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
      body: JSON.stringify(activeRequest),
      headers: { 'Content-Type': 'application/json' },
      timeout: 2 * 1000, // 2s timeout
    }).then((res) => res.json())
  )

  await Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      for (const response of responses) {
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

export async function getCycleDuration() {
  const randomArchiver = getRandomArchiver()
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`)
  if (response && response.cycleInfo) {
    return response.cycleInfo[0].duration
  }
}

export function checkJoinStatus(): Promise<boolean> {
  Logger.mainLogger.debug('Checking join status')
  const ourNodeInfo = State.getNodeInfo()
  const randomArchiver = getRandomArchiver()

  return new Promise(async (resolve) => {
    let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`)
    try {
      if (response && response.cycleInfo[0] && response.cycleInfo[0].joinedArchivers) {
        let joinedArchivers = response.cycleInfo[0].joinedArchivers
        let refreshedArchivers = response.cycleInfo[0].refreshedArchivers
        Logger.mainLogger.debug('cycle counter', response.cycleInfo[0].counter)
        Logger.mainLogger.debug('Joined archivers', joinedArchivers)

        let isJoind = [...joinedArchivers, ...refreshedArchivers].find(
          (a: any) => a.publicKey === ourNodeInfo.publicKey
        )
        Logger.mainLogger.debug('isJoind', isJoind)
        resolve(isJoind)
      } else {
        resolve(false)
      }
    } catch (e) {
      Logger.mainLogger.error(e)
      resolve(false)
    }
  })
}

export function checkActiveStatus(): Promise<boolean> {
  Logger.mainLogger.debug('Checking active status')
  const ourNodeInfo = State.getNodeInfo()
  const randomArchivers = Utils.getRandomItemFromArr(State.activeArchivers, 0, 5)
  return new Promise(async (resolve) => {
    const latestCycle = await getNewestCycleFromArchivers(randomArchivers)
    try {
      if (latestCycle && latestCycle.activeArchivers) {
        let activeArchivers = latestCycle.activeArchivers
        Logger.mainLogger.debug('cycle counter', latestCycle.counter)
        Logger.mainLogger.debug('Active archivers', activeArchivers)

        let isActive = activeArchivers.some((a: any) => a.publicKey === ourNodeInfo.publicKey)
        Logger.mainLogger.debug('isActive', isActive)
        resolve(isActive)
      } else {
        resolve(false)
      }
    } catch (e) {
      Logger.mainLogger.error(e)
      resolve(false)
    }
  })
}

export function checkJoinStatusFromConsensor(nodeList: NodeList.ConsensusNodeInfo[]): Promise<boolean> {
  Logger.mainLogger.debug('Checking join status from consenosr')
  const ourNodeInfo = State.getNodeInfo()

  return new Promise(async (resolve) => {
    const latestCycle = await getNewestCycleFromConsensors(nodeList)
    try {
      if (latestCycle && latestCycle.joinedArchivers && latestCycle.refreshedArchivers) {
        let joinedArchivers = latestCycle.joinedArchivers
        let refreshedArchivers = latestCycle.refreshedArchivers
        Logger.mainLogger.debug('cycle counter', latestCycle.counter)
        Logger.mainLogger.debug('Joined archivers', joinedArchivers)

        let isJoind: boolean = [...joinedArchivers, ...refreshedArchivers].some(
          (a: any) => a.publicKey === ourNodeInfo.publicKey
        )
        Logger.mainLogger.debug('isJoind', isJoind)
        resolve(isJoind)
      } else {
        resolve(false)
      }
    } catch (e) {
      Logger.mainLogger.error(e)
      resolve(false)
    }
  })
}

export async function getTotalDataFromArchivers() {
  const maxNumberofArchiversToRetry = 3
  const randomArchivers = Utils.getRandomItemFromArr(State.activeArchivers, 0, maxNumberofArchiversToRetry)
  const retry = 0
  while (retry < maxNumberofArchiversToRetry) {
    let randomArchiver = randomArchivers[retry]
    if (!randomArchiver) randomArchiver = randomArchivers[0]
    let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totaldata`)
    if (response && response.totalData) {
      return response.totalData
    }
  }
}

// TODO: Update to use multiple archivers to spread the load among them
export function getRandomArchiver(): State.ArchiverNodeInfo {
  const activeArchivers = State.activeArchivers.filter(
    (archiver) => archiver.publicKey !== State.getNodeInfo().publicKey
  )
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  return randomArchiver
}

export async function syncGenesisAccountsFromArchiver() {
  const randomArchiver = getRandomArchiver()
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
  let res: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/account?startCycle=0&endCycle=5`,
    QUERY_TIMEOUT_MAX
  )
  if (res && res.totalAccounts) {
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
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/account?startCycle=0&endCycle=5&page=${page}`,
      QUERY_TIMEOUT_MAX
    )
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

export async function syncGenesisTransactionsFromArchiver() {
  const randomArchiver = getRandomArchiver()
  let complete = false
  let startTransaction = 0
  let endTransaction = startTransaction + MAX_ACCOUNTS_PER_REQUEST // Sames as number of accounts per request
  let totalGenesisTransactions = 0

  let res: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/transaction?startCycle=0&endCycle=5`,
    QUERY_TIMEOUT_MAX
  )
  if (res && res.totalTransactions) {
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
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/transaction?startCycle=0&endCycle=5&page=${page}`,
      QUERY_TIMEOUT_MAX
    )
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
) {
  if (totalGenesisAccounts <= 0) return
  let startAccount = 0
  // let combineAccountsData = [];
  let totalDownloadedAccounts = 0
  while (startAccount <= totalGenesisAccounts) {
    Logger.mainLogger.debug(`Downloading accounts from ${startAccount}`)
    let response: any = await P2P.getJson(
      `http://${firstConsensor.ip}:${firstConsensor.port}/genesis_accounts?start=${startAccount}`,
      QUERY_TIMEOUT_MAX
    )
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

export async function buildNodeListFromStoredCycle(lastStoredCycle: Cycles.Cycle) {
  Logger.mainLogger.debug('lastStoredCycle', lastStoredCycle)
  Logger.mainLogger.debug(`Syncing till cycle ${lastStoredCycle.counter}...`)
  const cyclesToGet = 2 * Math.floor(Math.sqrt(lastStoredCycle.active)) + 2
  Logger.mainLogger.debug(`Cycles to get is ${cyclesToGet}`)

  let CycleChain = []
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
  Cycles.setCurrentCycleDuration(lastStoredCycle.duration)
  Logger.mainLogger.debug('Latest cycle after sync', lastStoredCycle.counter)
}

export async function syncCyclesAndNodeList(lastStoredCycleCount: number = 0) {
  const activeArchivers = [...State.activeArchivers]
  // Get the networks newest cycle as the anchor point for sync
  Logger.mainLogger.debug('Getting newest cycle...')
  const cycleToSyncTo = await getNewestCycleFromArchivers(activeArchivers)
  Logger.mainLogger.debug('cycleToSyncTo', cycleToSyncTo)
  Logger.mainLogger.debug(`Syncing till cycle ${cycleToSyncTo.counter}...`)
  const cyclesToGet = 2 * Math.floor(Math.sqrt(cycleToSyncTo.active)) + 2
  Logger.mainLogger.debug(`Cycles to get is ${cyclesToGet}`)

  let CycleChain = []
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
    const prevCycles = await fetchCycleRecords(activeArchivers, start, end)

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
    let record = CycleChain[i]
    Cycles.CycleChain.set(record.counter, { ...record })
    if (i === CycleChain.length - 1) await storeCycleData(CycleChain)
    Cycles.setCurrentCycleCounter(record.counter)
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
    const prevCycles = await fetchCycleRecords(activeArchivers, nextEnd, endCycle)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')
    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    let combineCycles = []
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

  return true
}

export async function syncCyclesAndNodeListV2(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredCycleCount: number = 0
) {
  // Sync validator list and get the latest cycle from the network
  Logger.mainLogger.debug('Syncing validators and latest cycle...')
  const syncResult = await syncV2(activeArchivers)
  let cycleToSyncTo: Cycle
  if (syncResult.isOk()) {
    cycleToSyncTo = syncResult.value
  } else {
    throw syncResult.error
  }

  Logger.mainLogger.debug('cycleToSyncTo', cycleToSyncTo)
  Logger.mainLogger.debug(`Syncing till cycle ${cycleToSyncTo.counter}...`)

  // store cycleToSyncTo in the database
  await storeCycleData([cycleToSyncTo])
  Cycles.setCurrentCycleCounter(cycleToSyncTo.counter)
  Cycles.setCurrentCycleDuration(cycleToSyncTo.duration)

  // This might have to removed once archiver sending active request is implemented!
  GossipData.getAdjacentLeftAndRightArchivers()
  Logger.mainLogger.debug('adjacentArchivers', GossipData.adjacentArchivers)

  // Download old cycle Records
  await downloadOldCycles(cycleToSyncTo, lastStoredCycleCount, activeArchivers)

  return true
}

export async function syncCyclesBetweenCycles(lastStoredCycle: number = 0, cycleToSyncTo: number = 0) {
  let startCycle = lastStoredCycle
  let endCycle = startCycle + MAX_CYCLES_PER_REQUEST
  const randomArchiver = getRandomArchiver()
  while (cycleToSyncTo > startCycle) {
    if (endCycle > cycleToSyncTo) endCycle = cycleToSyncTo
    Logger.mainLogger.debug(`Downloading cycles from ${startCycle} to ${endCycle}`)
    const res: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo?start=${startCycle}&end=${endCycle}`,
      QUERY_TIMEOUT_MAX
    )
    if (res && res.cycleInfo) {
      Logger.mainLogger.debug(`Downloaded cycles`, res.cycleInfo.length)
      const cycles = res.cycleInfo
      processCycles(cycles)
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

export async function syncReceipts(lastStoredReceiptCount: number = 0) {
  const randomArchiver = getRandomArchiver()
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
    QUERY_TIMEOUT_MAX
  )
  if (!response || response.totalReceipts < 0) {
    return false
  }
  const { totalCycles, totalReceipts } = response
  if (totalReceipts > 0) await downloadReceipts(totalReceipts, lastStoredReceiptCount, randomArchiver)
  Logger.mainLogger.debug('Sync receipts data completed!')
  return false
}

export const downloadReceipts = async (to: number, from: number = 0, archiver: State.ArchiverNodeInfo) => {
  let complete = false
  let start = from
  let end = start + MAX_RECEIPTS_PER_REQUEST
  while (!complete) {
    if (end >= to) {
      let res: any = await P2P.getJson(`http://${archiver.ip}:${archiver.port}/totalData`, QUERY_TIMEOUT_MAX)
      if (res && res.totalReceipts > 0) {
        if (res.totalReceipts > to) to = res.totalReceipts
        Logger.mainLogger.debug('totalReceiptsToSync', to)
      }
    }
    Logger.mainLogger.debug(`Downloading receipts from ${start} to  ${end}`)
    let response: any = await P2P.getJson(
      `http://${archiver.ip}:${archiver.port}/receipt?start=${start}&end=${end}`,
      QUERY_TIMEOUT_MAX
    )
    if (response && response.receipts) {
      const downloadedReceipts = response.receipts
      Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
      await storeReceiptData(downloadedReceipts, archiver.ip + ':' + archiver.port, true)
      if (response.receipts.length < MAX_RECEIPTS_PER_REQUEST) {
        let res: any = await P2P.getJson(
          `http://${archiver.ip}:${archiver.port}/totalData`,
          QUERY_TIMEOUT_MAX
        )
        start += response.receipts.length
        end = start + MAX_RECEIPTS_PER_REQUEST
        if (res && res.totalReceipts > 0) {
          if (res.totalReceipts > to) to = res.totalReceipts
          if (start === to) {
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
}

export const downloadOriginalTxs = async (to: number, from: number = 0, archiver: State.ArchiverNodeInfo) => {
  let complete = false
  let start = from
  let end = start + MAX_ORIGINAL_TXS_PER_REQUEST
  while (!complete) {
    if (end >= to) {
      // If the number of new original txs to sync is within MAX_ORIGINAL_TXS_PER_REQUEST => Update to the latest totalOriginalTxs.
      let res: any = await P2P.getJson(`http://${archiver.ip}:${archiver.port}/totalData`, QUERY_TIMEOUT_MAX)
      if (res && res.totalOriginalTxs > 0) {
        if (res.totalOriginalTxs > to) to = res.totalOriginalTxs
        Logger.mainLogger.debug('totalOriginalTxs: ', to)
      }
    }
    Logger.mainLogger.debug(`Downloading Original-Txs from ${start} to ${end}`)
    const response: any = await P2P.getJson(
      `http://${archiver.ip}:${archiver.port}/originalTx?start=${start}&end=${end}`,
      QUERY_TIMEOUT_MAX
    )
    if (response && response.originalTxs) {
      const downloadedOriginalTxs = response.originalTxs
      Logger.mainLogger.debug('Downloaded Original-Txs: ', downloadedOriginalTxs.length)
      await storeOriginalTxData(downloadedOriginalTxs, archiver.ip + ':' + archiver.port, true)
      if (response.originalTxs.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
        let totalData: any = await P2P.getJson(
          `http://${archiver.ip}:${archiver.port}/totalData`,
          QUERY_TIMEOUT_MAX
        )
        start += response.originalTxs.length
        end = start + MAX_ORIGINAL_TXS_PER_REQUEST
        if (totalData && totalData.totalOriginalTxs > 0) {
          if (totalData.totalOriginalTxs > to) to = totalData.totalOriginalTxs
          if (start === to) {
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
}

export async function syncReceiptsByCycle(lastStoredReceiptCycle: number = 0, cycleToSyncTo: number = 0) {
  let totalCycles = cycleToSyncTo
  let totalReceipts = 0
  const randomArchiver = getRandomArchiver()
  if (cycleToSyncTo === 0) {
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
      QUERY_TIMEOUT_MAX
    )
    if (!response || response.totalReceipts < 0) {
      return false
    }
    totalCycles = response.totalCycles
    totalReceipts = response.totalReceipts
  }
  let complete = false
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
        let res: any = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
          QUERY_TIMEOUT_MAX
        )
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
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`,
      QUERY_TIMEOUT_MAX
    )
    if (response && response.receipts > 0) {
      receiptsCountToSyncBetweenCycles = response.receipts
      let page = 1
      savedReceiptsCountBetweenCycles = 0
      while (savedReceiptsCountBetweenCycles < receiptsCountToSyncBetweenCycles) {
        response = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&page=${page}`,
          QUERY_TIMEOUT_MAX
        )
        if (response && response.receipts) {
          const downloadedReceipts = response.receipts
          Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
          await storeReceiptData(downloadedReceipts, randomArchiver.ip + ':' + randomArchiver.port, true)
          savedReceiptsCountBetweenCycles += downloadedReceipts.length
          if (savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles) {
            response = await P2P.getJson(
              `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`,
              QUERY_TIMEOUT_MAX
            )
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

export const syncOriginalTxs = async (lastStoredOriginalTxsCount: number = 0) => {
  const randomArchiver = getRandomArchiver()
  const totalData: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
    QUERY_TIMEOUT_MAX
  )
  if (!totalData || totalData.totalOriginalTxs < 0) {
    return false
  }
  const { totalOriginalTxs } = totalData
  if (totalOriginalTxs > 0)
    await downloadOriginalTxs(totalOriginalTxs, lastStoredOriginalTxsCount, randomArchiver)
  Logger.mainLogger.debug('Sync Original-Txs Data Completed!')
  return false
}

export const syncOriginalTxsByCycle = async (
  lastStoredOriginalTxCycle: number = 0,
  cycleToSyncTo: number = 0
): Promise<void> => {
  let totalCycles = cycleToSyncTo
  let totalOriginalTxs = 0
  const randomArchiver = getRandomArchiver()
  if (cycleToSyncTo === 0) {
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
      QUERY_TIMEOUT_MAX
    )
    if (!response || response.totalOriginalTxs < 0) {
      return
    }
    totalCycles = response.totalCycles
    totalOriginalTxs = response.totalReceipts
  }
  let complete = false
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
        let res: any = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
          QUERY_TIMEOUT_MAX
        )
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
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/originalTx?startCycle=${startCycle}&endCycle=${endCycle}&type=count`,
      QUERY_TIMEOUT_MAX
    )
    if (response && response.originalTxs > 0) {
      originalTxCountToSyncBetweenCycles = response.originalTxs
      let page = 1
      savedOriginalTxCountBetweenCycles = 0
      while (savedOriginalTxCountBetweenCycles < originalTxCountToSyncBetweenCycles) {
        response = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/originalTx?startCycle=${startCycle}&endCycle=${endCycle}&page=${page}`,
          QUERY_TIMEOUT_MAX
        )
        if (response && response.originalTxs) {
          const downloadedOriginalTxs = response.originalTxs
          Logger.mainLogger.debug('Downloaded Original-Txs: ', downloadedOriginalTxs.length)
          await storeOriginalTxData(
            downloadedOriginalTxs,
            randomArchiver.ip + ':' + randomArchiver.port,
            true
          )
          savedOriginalTxCountBetweenCycles += downloadedOriginalTxs.length
          if (savedOriginalTxCountBetweenCycles > originalTxCountToSyncBetweenCycles) {
            response = await P2P.getJson(
              `http://${randomArchiver.ip}:${randomArchiver.port}/originalTx?startCycle=${startCycle}&endCycle=${endCycle}&type=count`,
              QUERY_TIMEOUT_MAX
            )
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

export const syncCyclesAndReceiptsData = async (
  lastStoredCycleCount: number = 0,
  lastStoredReceiptCount: number = 0,
  lastStoredOriginalTxCount: number = 0
) => {
  const randomArchiver = getRandomArchiver()
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
    QUERY_TIMEOUT_MAX
  )
  if (!response || response.totalCycles < 0 || response.totalReceipts < 0) {
    return false
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
    return false
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
      response = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`,
        QUERY_TIMEOUT_MAX
      )
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
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?start=${startReceipt}&end=${endReceipt}`,
        QUERY_TIMEOUT_MAX
      )
      if (res && res.receipts) {
        const downloadedReceipts = res.receipts
        Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
        await storeReceiptData(downloadedReceipts, randomArchiver.ip + ':' + randomArchiver.port, true)
        if (downloadedReceipts.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
          startReceipt += downloadedReceipts.length + 1
          endReceipt = downloadedReceipts.length + MAX_ORIGINAL_TXS_PER_REQUEST
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
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/originalTx?start=${startOriginalTx}&end=${endOriginalTx}`,
        QUERY_TIMEOUT_MAX
      )
      if (res && res.originalTxs) {
        const downloadedOriginalTxs = res.originalTxs
        Logger.mainLogger.debug(`Downloaded Original-Txs: `, downloadedOriginalTxs.length)
        await storeOriginalTxData(downloadedOriginalTxs, randomArchiver.ip + ':' + randomArchiver.port, true)
        if (downloadedOriginalTxs.length < MAX_ORIGINAL_TXS_PER_REQUEST) {
          startOriginalTx += downloadedOriginalTxs.length + 1
          endOriginalTx = downloadedOriginalTxs.length + MAX_ORIGINAL_TXS_PER_REQUEST
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
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo?start=${startCycle}&end=${endCycle}`,
        QUERY_TIMEOUT_MAX
      )
      if (res && res.cycleInfo) {
        Logger.mainLogger.debug(`Downloaded cycles`, res.cycleInfo.length)
        const cycles = res.cycleInfo
        processCycles(cycles)
        if (res.cycleInfo.length < MAX_CYCLES_PER_REQUEST) {
          startCycle += res.cycleInfo.length + 1
          endCycle = res.cycleInfo.length + MAX_CYCLES_PER_REQUEST
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
  return false
}

export const syncCyclesAndTxsDataBetweenCycles = async (
  lastStoredCycle: number = 0,
  cycleToSyncTo: number = 0
) => {
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
export async function compareWithOldOriginalTxsData(
  archiver: State.ArchiverNodeInfo,
  lastStoredOriginalTxCycle: number = 0
) {
  let endCycle = lastStoredOriginalTxCycle
  let startCycle = endCycle - 20 > 0 ? endCycle - 20 : 0
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/originalTx?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`,
    QUERY_TIMEOUT_MAX
  )
  let downloadedOriginalTxsByCycles: string | any[]
  if (response && response.originalTxs) {
    downloadedOriginalTxsByCycles = response.originalTxs
  } else {
    throw Error(
      `Can't fetch original tx data from cycle ${startCycle} to cycle ${endCycle} from archiver ${archiver}`
    )
  }
  let oldOriginalTxCountByCycle = await OriginalTxDB.queryOriginalTxDataCountByCycles(startCycle, endCycle)

  let success = false
  let matchedCycle = 0
  for (let i = 0; i < downloadedOriginalTxsByCycles.length; i++) {
    const downloadedOriginalTx = downloadedOriginalTxsByCycles[i]
    const oldOriginalTx = oldOriginalTxCountByCycle[i]
    Logger.mainLogger.debug(downloadedOriginalTx, oldOriginalTx)
    if (
      downloadedOriginalTx.cycle !== oldOriginalTx.cycle ||
      downloadedOriginalTx.originalTxData !== oldOriginalTx.originalTxData
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

export async function compareWithOldReceiptsData(
  archiver: State.ArchiverNodeInfo,
  lastStoredReceiptCycle: number = 0
) {
  let endCycle = lastStoredReceiptCycle
  let startCycle = endCycle - 10 > 0 ? endCycle - 10 : 0
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`,
    QUERY_TIMEOUT_MAX
  )
  let downloadedReceiptCountByCycles: string | any[]
  if (response && response.receipts) {
    downloadedReceiptCountByCycles = response.receipts
  } else {
    throw Error(
      `Can't fetch receipts data from cycle ${startCycle} to cycle ${endCycle}  from archiver ${archiver}`
    )
  }
  let oldReceiptCountByCycle = await ReceiptDB.queryReceiptCountByCycles(startCycle, endCycle)
  let success = false
  let matchedCycle = 0
  for (let i = 0; i < downloadedReceiptCountByCycles.length; i++) {
    const downloadedReceipt = downloadedReceiptCountByCycles[i]
    const oldReceipt = oldReceiptCountByCycle[i]
    Logger.mainLogger.debug(downloadedReceipt, oldReceipt)
    if (downloadedReceipt.cycle !== oldReceipt.cycle || downloadedReceipt.receipts !== oldReceipt.receipts) {
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

export async function compareWithOldCyclesData(archiver: State.ArchiverNodeInfo, lastCycleCounter = 0) {
  let downloadedCycles
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/cycleinfo?start=${lastCycleCounter - 10}&end=${
      lastCycleCounter - 1
    }`,
    QUERY_TIMEOUT_MAX
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
  let oldCycles = await CycleDB.queryCycleRecordsBetween(lastCycleCounter - 10, lastCycleCounter + 1)
  downloadedCycles.sort((a, b) => (a.counter > b.counter ? 1 : -1))
  oldCycles.sort((a, b) => (a.counter > b.counter ? 1 : -1))
  let success = false
  let cycle = 0
  for (let i = 0; i < downloadedCycles.length; i++) {
    let downloadedCycle = downloadedCycles[i]
    const oldCycle = oldCycles[i]
    console.log(downloadedCycle, oldCycle)
    if (JSON.stringify(downloadedCycle) !== JSON.stringify(oldCycle)) {
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

async function downloadOldCycles(
  cycleToSyncTo: P2PTypes.CycleCreatorTypes.CycleRecord,
  lastStoredCycleCount: number,
  activeArchivers: State.ArchiverNodeInfo[]
) {
  let endCycle = cycleToSyncTo.counter - 1
  Logger.mainLogger.debug('endCycle counter', endCycle, 'lastStoredCycleCount', lastStoredCycleCount)
  if (endCycle > lastStoredCycleCount) {
    Logger.mainLogger.debug(
      `Downloading old cycles from cycles ${lastStoredCycleCount} to cycle ${endCycle}!`
    )
  }

  let savedCycleRecord = cycleToSyncTo
  while (endCycle > lastStoredCycleCount) {
    let nextEnd: number = endCycle - MAX_CYCLES_PER_REQUEST
    if (nextEnd < 0) nextEnd = 0
    Logger.mainLogger.debug(`Getting cycles ${nextEnd} - ${endCycle} ...`)
    const prevCycles = await fetchCycleRecords(activeArchivers, nextEnd, endCycle)

    // If prevCycles is empty, start over
    if (prevCycles.length < 1) throw new Error('Got empty previous cycles')
    prevCycles.sort((a, b) => (a.counter > b.counter ? -1 : 1))

    // Add prevCycles to our cycle chain
    let combineCycles = []
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
