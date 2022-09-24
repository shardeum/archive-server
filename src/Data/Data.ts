import { EventEmitter } from 'events'
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Storage from '../Storage'
import * as Cycles from './Cycles'
import {
  currentCycleCounter,
  currentCycleDuration,
  Cycle,
  lastProcessedMetaData,
  processCycles,
  validateCycle,
} from './Cycles'
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
import { queryArchivedCycleByMarker } from '../Storage'
import { queryArchivedCycles } from '../test/api/archivedCycles'
import {
  storeReceiptData,
  storeCycleData,
  storeAccountData,
  storingAccountData,
} from './Collector'
import * as CycleDB from '../dbstore/cycles'
import * as AccountDB from '../dbstore/accounts'
import * as ReceiptDB from '../dbstore/receipts'

// Socket modules
export let socketServer: SocketIO.Server
let ioclient: SocketIOClientStatic = require('socket.io-client')
let socketClient: SocketIOClientStatic['Socket']
let socketClients: Map<string, SocketIOClientStatic['Socket']> = new Map()
let socketConnectionsTracker: Map<string, string> = new Map()
let lastSentCycleCounterToExplorer = 0
export let combineAccountsData = []
let forwardGenesisAccounts = false
let multipleDataSenders = true
let consensorsCountToSubscribe = 3
let receivedCounters = {}
let selectByConsensuRadius = true
let selectingNewDataSender = false
let queueForSelectingNewDataSenders: Map<string, string> = new Map()
let receivedCycleTracker = {}
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

interface DataKeepAlive {
  keepAlive: boolean
}

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
  data: any
}

export class ArchivedCycle extends BaseModel {
  cycleRecord!: Cycle
  cycleMarker!: StateManager.StateMetaDataTypes.CycleMarker
  data!: StateManager.StateMetaDataTypes.StateData
  receipt!: StateManager.StateMetaDataTypes.Receipt
  summary!: StateManager.StateMetaDataTypes.Summary
}

export let StateMetaDataMap = new Map()
export let currentDataSender: string = ''
export let currentDataSenders: string[] = []

export function initSocketServer(io: SocketIO.Server) {
  socketServer = io
  socketServer.on('connection', (socket: SocketIO.Socket) => {
    Logger.mainLogger.debug('Explorer has connected')
  })
}

export function unsubscribeDataSender(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  Logger.mainLogger.debug('Disconnecting previous connection', publicKey)
  if (multipleDataSenders) socketClient = socketClients.get(publicKey)
  if (!socketClient) return
  socketClient.emit('UNSUBSCRIBE', config.ARCHIVER_PUBLIC_KEY)
  socketClient.disconnect()
  if (config.VERBOSE) console.log('Killing the connection to', publicKey)
  removeDataSenders(publicKey)
  if (multipleDataSenders) {
    socketClients.delete(publicKey)
    socketConnectionsTracker.delete(publicKey)
    currentDataSenders = currentDataSenders.filter((item) => item !== publicKey)
    if (config.VERBOSE) console.log('Subscribed socketClients', socketClients)
    Logger.mainLogger.debug(
      'Subscribed socketClients',
      socketClients.size,
      dataSenders.size
    )
  }
  currentDataSender = ''
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo) {
  if (config.VERBOSE)
    Logger.mainLogger.debug('Node Info to socker connect', node)
  if (config.VERBOSE) console.log('Node Info to socker connect', node)
  const socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)

  socketClient.on('connect', () => {
    Logger.mainLogger.debug('Connection to consensus node was made')
    // Send ehlo event right after connect:
    socketClient.emit('ARCHIVER_PUBLIC_KEY', config.ARCHIVER_PUBLIC_KEY)
    if (multipleDataSenders) {
      socketClients.set(node.publicKey, socketClient)
      socketConnectionsTracker.set(node.publicKey, 'connected')
      if (config.VERBOSE) console.log('Connected node', node)
      if (config.VERBOSE) console.log('Init socketClients', socketClients)
      if (config.VERBOSE)
        Logger.mainLogger.debug('Init socketClients', socketClients.size)
    }
  })

  socketClient.once('disconnect', async () => {
    Logger.mainLogger.debug(
      `Connection request is refused by the consensor node ${node.ip}:${node.port}`
    )
    console.log(
      `Connection request is refused by the consensor node ${node.ip}:${node.port}`
    )
    // socketClients.delete(node.publicKey)
    // await Utils.sleep(3000)
    // if (socketClients.has(node.publicKey)) replaceDataSender(node.publicKey)
    socketConnectionsTracker.set(node.publicKey, 'disconnected')
  })

  socketClient.on(
    'DATA',
    (
      newData: DataResponse<P2PTypes.SnapshotTypes.ValidTypes> &
        Crypto.TaggedMessage
    ) => {
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

      if (config.experimentalSnapshot) {
        // Get sender entry
        const sender = dataSenders.get(newData.publicKey)
        // If no sender entry, remove publicKey from senders, END
        if (!sender) {
          Logger.mainLogger.error('No sender found for this data')
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
          console.log(
            'RECEIPT RECEIPT',
            sender.nodeInfo.publicKey,
            sender.nodeInfo.ip,
            sender.nodeInfo.port
          )

        if (newData.responses && newData.responses.RECEIPT) {
          console.log(
            'RECEIPT RECEIPT',
            sender.nodeInfo.publicKey,
            sender.nodeInfo.ip,
            sender.nodeInfo.port,
            newData.responses.RECEIPT.length
          )
          // clearFalseNodes(sender.nodeInfo.publicKey)
          storeReceiptData(
            newData.responses.RECEIPT,
            sender.nodeInfo.ip + ':' + sender.nodeInfo.port
          )
        }
        if (newData.responses && newData.responses.CYCLE) {
          let found = true
          for (const cycle of newData.responses.CYCLE) {
            if (receivedCycleTracker[cycle.counter])
              receivedCycleTracker[cycle.counter]++
            else {
              receivedCycleTracker[cycle.counter] = 1
              found = false
            }
          }
          if (!found) {
            processCycles(newData.responses.CYCLE as Cycle[])
            storeCycleData(newData.responses.CYCLE)
          }
          if (Object.keys(receivedCycleTracker).length > 20) {
            for (const counter of Object.keys(receivedCycleTracker)) {
              if (parseInt(counter) < currentCycleCounter - 10) {
                Logger.mainLogger.debug(
                  `Received ${receivedCycleTracker[counter]} times for cycle counter ${counter}`
                )
                delete receivedCycleTracker[counter]
              }
            }
          }
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
            Logger.mainLogger.debug(
              'Genesis Accounts To Sycn',
              newData.responses.ACCOUNT
            )
            syncGenesisAccountsFromConsensor(
              newData.responses.ACCOUNT,
              sender.nodeInfo
            )
          } else {
            if (storingAccountData) {
              combineAccountsData = [
                ...combineAccountsData,
                ...newData.responses.ACCOUNT,
              ]
            }
            // console.log(newData.responses.ACCOUNT)
            else storeAccountData(newData.responses.ACCOUNT)
          }
        }

        // Set new contactTimeout for sender. Postpone sender removal because data is still received from consensor
        if (currentCycleDuration > 0) {
          nestedCountersInstance.countEvent(
            'archiver',
            'postpone_contact_timeout'
          )
          sender.contactTimeout = createContactTimeout(
            sender.nodeInfo.publicKey,
            'This timeout is created after processing data'
          )
        }
        return
      }

      if (newData.responses.STATE_METADATA.length > 0)
        Logger.mainLogger.debug('New DATA', newData.responses)
      else Logger.mainLogger.debug('State metadata is empty')

      if (multipleDataSenders) currentDataSenders.push(newData.publicKey)
      currentDataSender = newData.publicKey
      if (newData.responses && newData.responses.STATE_METADATA) {
        // Logger.mainLogger.debug('New DATA from consensor STATE_METADATA', newData.publicKey, newData.responses.STATE_METADATA)
        // let hashArray: any = Gossip.convertStateMetadataToHashArray(newData.responses.STATE_METADATA[0])
        for (let stateMetadata of newData.responses.STATE_METADATA) {
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
            `NEW DATA type ${type} not included in sender's types: ${JSON.stringify(
              sender.types
            )}`
          )
          return
        }
      }
      setImmediate(processData, newData)
    }
  )
}

export function clearCombinedAccountsData() {
  combineAccountsData = []
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

export function createQueryRequest<T extends P2PTypes.SnapshotTypes.ValidTypes>(
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
  types: (keyof typeof P2PTypes.SnapshotTypes.TypeNames)[]
  contactTimeout?: NodeJS.Timeout | null
  replaceTimeout?: NodeJS.Timeout | null
}

export const dataSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  DataSender
> = new Map()

const timeoutPadding = 1000

export const emitter = new EventEmitter()

export async function replaceDataSender(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  nestedCountersInstance.countEvent('archiver', 'replace_data_sender')
  if (NodeList.getActiveList().length < 2) {
    Logger.mainLogger.debug(
      'There is only one active node in the network. Unable to replace data sender'
    )
    let sender = dataSenders.get(publicKey)
    if (sender && sender.replaceTimeout) {
      nestedCountersInstance.countEvent('archiver', 'clear_replace_timeout')
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
      sender.replaceTimeout = createReplaceTimeout(publicKey)
    }
    return
  }
  Logger.mainLogger.debug(`replaceDataSender: replacing ${publicKey}`)

  if (selectByConsensuRadius) {
    if (!socketClients.has(publicKey)) removeDataSenders(publicKey)
    // Extend the contactTimeout a bit longer for now to make sure the archiver has already got a new replacer node
    const sender = dataSenders.get(publicKey)
    if (sender && sender.replaceTimeout) {
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
      sender.replaceTimeout = createReplaceTimeout(publicKey)
    }
    if (sender && sender.contactTimeout) {
      clearTimeout(sender.contactTimeout)
      sender.contactTimeout = null
      sender.contactTimeout = createContactTimeout(
        publicKey,
        'This timeout is created to rotate this node',
        2 * currentCycleDuration
      )
    }
    if (selectingNewDataSender) {
      queueForSelectingNewDataSenders.set(publicKey, publicKey)
    } else {
      selectingNewDataSender = true
      selectNewDataSendersByConsensusRadius([publicKey])
    }
    return
  }

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
  if (multipleDataSenders && socketClients.size >= consensorsCountToSubscribe) {
    Logger.mainLogger.debug(
      `There are already ${socketClients.size} nodes that the archiver has picked.`
    )
    console.log(
      `There are already ${socketClients.size} nodes that the archiver has picked.`
    )
    return
  }
  initSocketClient(newSenderInfo)
  let count = 0
  while (!socketClients.has(newSenderInfo.publicKey) && count <= 10) {
    await Utils.sleep(1000)
    count++
    if (count === 10) {
      // This means the socket connection to the node is not successful.
      return
    }
  }
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
    ],
    contactTimeout: createContactTimeout(
      newSenderInfo.publicKey,
      'This timeout is created during newSender selection',
      2 * currentCycleDuration
    ),
    replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
  }

  // Add new dataSender to dataSenders
  addDataSenders(newSender)
  Logger.mainLogger.debug(
    `replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`
  )

  // Send dataRequest to new dataSender
  const dataRequest = {
    dataRequestCycle: createDataRequest<Cycle>(
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      currentCycleCounter,
      publicKey
    ),
    dataRequestStateMetaData:
      createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
        P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
        lastProcessedMetaData,
        publicKey
      ),
    publicKey: State.getNodeInfo().publicKey,
    nodeInfo: State.getNodeInfo(),
  }
  sendDataRequest(newSender, dataRequest)
  if (multipleDataSenders) {
    await Utils.sleep(3000)
    console.log(
      'Current socker IO clients',
      publicKey,
      socketClients.size,
      dataSenders
    )
    Logger.mainLogger.debug(
      'Current socker IO clients',
      publicKey,
      socketClients.size,
      dataSenders
    )
    const activeList = NodeList.getActiveList()
    if (
      activeList.length >= consensorsCountToSubscribe &&
      socketClients.size < consensorsCountToSubscribe
    ) {
      subscribeMoreConsensors(consensorsCountToSubscribe - socketClients.size)
    }
  }
}

export async function subscribeNodeForDataTransfer() {
  if (config.experimentalSnapshot) {
    if (selectByConsensuRadius) await subscribeMoreConsensorsByConsensusRadius()
    else if (multipleDataSenders)
      subscribeMoreConsensors(consensorsCountToSubscribe)
    else await subscribeRandomNodeForDataTransfer()
  } else {
    await subscribeRandomNodeForDataTransfer()
  }
}

export async function subscribeRandomNodeForDataTransfer() {
  let retry = 0
  let nodeSubscribedFail = true
  // Set randomly select a consensor as dataSender
  while (nodeSubscribedFail && retry < 10) {
    let randomConsensor = NodeList.getRandomActiveNode()[0]
    let connectionStatus = await createDataTransferConnection(randomConsensor)
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
  msg: string = '',
  timeout: number | null = null
) {
  // TODO: check what is the best contact timeout
  let ms: number
  if (timeout) ms = timeout
  else if (currentCycleDuration > 0)
    ms = 1.5 * currentCycleDuration + timeoutPadding
  else ms = 1.5 * 60 * 1000 + timeoutPadding
  if (config.experimentalSnapshot) ms = 15 * 1000 // Change contact timeout to 15s for now
  Logger.mainLogger.debug('Created contact timeout: ' + ms)
  nestedCountersInstance.countEvent('archiver', 'contact_timeout_created')
  return setTimeout(() => {
    // Logger.mainLogger.debug('nestedCountersInstance', nestedCountersInstance)
    if (nestedCountersInstance)
      nestedCountersInstance.countEvent('archiver', 'contact_timeout')
    Logger.mainLogger.debug('REPLACING sender due to CONTACT timeout', msg)
    console.log('REPLACING sender due to CONTACT timeout', msg, publicKey)
    replaceDataSender(publicKey)
  }, ms)
}

export function createReplaceTimeout(
  publicKey: NodeList.ConsensusNodeInfo['publicKey']
) {
  const ms = config.DATASENDER_TIMEOUT || 1000 * 60 * 60
  return setTimeout(() => {
    nestedCountersInstance.countEvent('archiver', 'replace_timeout')
    Logger.mainLogger.debug('ROTATING sender due to ROTATION timeout')
    replaceDataSender(publicKey)
  }, ms)
}

export function addDataSenders(...senders: DataSender[]) {
  for (const sender of senders) {
    dataSenders.set(sender.nodeInfo.publicKey, sender)
    if (multipleDataSenders) currentDataSenders.push(sender.nodeInfo.publicKey)
    currentDataSender = sender.nodeInfo.publicKey
  }
}

function removeDataSenders(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  Logger.mainLogger.debug(`${new Date()}: Removing data sender ${publicKey}`)
  const removedSenders = []
  // console.log('removeDataSenders', dataSenders)
  // Logger.mainLogger.debug('removeDataSenders', dataSenders)
  for (let [key, sender] of dataSenders) {
    console.log(publicKey, key)
    Logger.mainLogger.debug(publicKey, key)
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

function clearFalseNodes(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  // Logger.mainLogger.debug(
  //   `${new Date()}: Clearing data not sending node that is in socker list ${publicKey}`
  // )
  // console.log('Clear False Nodes', publicKey)
  if (socketClients.size < 2) {
    return
  }
  for (let [key, sender] of socketClients) {
    if (key != publicKey) {
      if (receivedCounters[key]) receivedCounters[key]++
      else receivedCounters[key] = 1
    } else {
      receivedCounters[key] = 1
    }
  }
  for (let [key, sender] of dataSenders) {
    if (key != publicKey) {
      if (receivedCounters[key]) receivedCounters[key]++
      else receivedCounters[key] = 1
    } else {
      receivedCounters[key] = 1
    }
  }
  for (let pk in receivedCounters) {
    const publicKey: any = pk
    if (receivedCounters[publicKey] > 50) {
      if (socketClients.has(publicKey)) {
        console.log('Unsubscribe 2', publicKey)
        unsubscribeDataSender(publicKey)
      }
      if (dataSenders.has(publicKey)) removeDataSenders(publicKey)
      delete receivedCounters[publicKey]
    }
    if (!socketClients.has(publicKey) || !dataSenders.has(publicKey))
      delete receivedCounters[publicKey]
  }
  // console.log('Clear False Nodes', receivedCounters)
}

function selectNewDataSender(publicKey) {
  // Randomly pick an active node
  const activeList = NodeList.getActiveList()
  let newSender = activeList[Math.floor(Math.random() * activeList.length)]
  if (config.VERBOSE) console.log('newSender 1', newSender)
  if (multipleDataSenders) {
    let retry = 0
    while (true && retry < 5) {
      // Retry 5 times to get the new Sender
      if (!socketClients.has(newSender.publicKey)) {
        break
      }
      newSender = activeList[Math.floor(Math.random() * activeList.length)]
      if (config.VERBOSE) console.log('newSender 2', newSender)
      retry++
    }
    if (socketClients.has(newSender.publicKey)) {
      // if there is still not new node to subscribe, just connect with the current one then
      newSender = activeList.find((node) => node.publicKey === publicKey)
      if (config.VERBOSE) console.log('newSender 3', newSender)
      Logger.mainLogger.debug(
        'Since no new data sender is found, and continue with the current one',
        publicKey,
        newSender
      )
    }
  }
  Logger.mainLogger.debug('New data sender is selected', newSender)
  if (newSender) {
    console.log('Unsubscribe 3', publicKey)
    unsubscribeDataSender(publicKey)
    // initSocketClient(newSender)
  }
  return newSender
}

async function selectNewDataSendersByConsensusRadius(
  publicKeys: NodeList.ConsensusNodeInfo['publicKey'][]
) {
  const calculatedConsensusRadius = await getConsensusRadius()
  let consensusRadius = calculatedConsensusRadius
  if (consensusRadius > 2) consensusRadius-- // Change default to 3 for now assuming nodesPerConsensusGroup 10
  const activeList = NodeList.getActiveList()
  if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
  const totalNumberOfNodesToSubscribe = Math.ceil(
    activeList.length / consensusRadius
  )
  Logger.mainLogger.debug(
    'totalNumberOfNodesToSubscribe',
    totalNumberOfNodesToSubscribe
  )
  console.log('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  for (const publicKey of publicKeys) {
    let nodeIsUnsubscribed = true
    let nodeIsInTheActiveList = false
    for (let i = 0; i < activeList.length; i += consensusRadius) {
      const subsetList = activeList.slice(i, i + consensusRadius)
      if (config.VERBOSE)
        console.log(
          'Round',
          i,
          publicKey,
          'subsetList',
          subsetList,
          socketClients.keys()
        )
      let nodeToRotateIsFromThisSubset = false
      let noNodeFromThisSubset = true
      let extraSubscribedNodesCountFromThisSubset = 0
      for (let node of Object.values(subsetList)) {
        if (socketClients.has(node.publicKey)) {
          if (config.VERBOSE) console.log('The node is found in this subset')
          noNodeFromThisSubset = false
          extraSubscribedNodesCountFromThisSubset++
        }
        if (node.publicKey === publicKey) {
          extraSubscribedNodesCountFromThisSubset--
          nodeToRotateIsFromThisSubset = true
          nodeIsInTheActiveList = true
        }
      }

      if (!nodeToRotateIsFromThisSubset && !noNodeFromThisSubset) {
        Logger.mainLogger.debug(
          'There is already node from this subset or node to rotate is not from this subset!'
        )
        console.log(
          'There is already node from this subset or node to rotate is not from this subset!'
        )
        continue
      }
      if (extraSubscribedNodesCountFromThisSubset >= 1) {
        Logger.mainLogger.debug(
          `There are already ${extraSubscribedNodesCountFromThisSubset} nodes that the archiver has picked from this nodes subset.`
        )
        console.log(
          `There are already ${extraSubscribedNodesCountFromThisSubset} nodes that the archiver has picked from this nodes subset.`
        )
        if (nodeIsUnsubscribed && nodeToRotateIsFromThisSubset) {
          if (config.VERBOSE) console.log('Unsubscribe 4', publicKey)
          unsubscribeDataSender(publicKey)
          nodeIsUnsubscribed = false
        }
        continue
      }
      let newSubsetList = subsetList.filter(
        (node) => node.publicKey !== publicKey
      )
      if (newSubsetList.length === 0) {
        if (subsetList[0].publicKey === publicKey) {
          // This isn't supposed to happen and just to log if it happens
          Logger.mainLogger.error(
            `The node publicKey ${publicKey} is not in the subset list!`
          )
        }
        continue
      }
      // Pick a new dataSender
      let newSenderInfo =
        newSubsetList[Math.floor(Math.random() * newSubsetList.length)]
      let connectionStatus = false
      let retry = 0
      while (true && retry < consensusRadius) {
        if (
          !socketClients.has(newSenderInfo.publicKey) &&
          publicKey !== newSenderInfo.publicKey
        ) {
          connectionStatus = await createDataTransferConnection(newSenderInfo)
          if (connectionStatus) {
            if (nodeIsUnsubscribed && nodeToRotateIsFromThisSubset) {
              if (config.VERBOSE) console.log('Unsubscribe 5', publicKey)
              unsubscribeDataSender(publicKey)
              nodeIsUnsubscribed = false
            }
            // if (noNodeFromThisSubset) await Utils.sleep(30000) // Start another node with 30s difference
            break
          } else {
            if (socketClients.has(newSenderInfo.publicKey))
              socketClients.delete(newSenderInfo.publicKey)
            newSubsetList = newSubsetList.filter(
              (node) => node.publicKey !== newSenderInfo.publicKey
            )
          }
          socketConnectionsTracker.delete(newSenderInfo.publicKey)
        }
        if (newSubsetList.length > 0) {
          newSenderInfo =
            newSubsetList[Math.floor(Math.random() * newSubsetList.length)]
        } else {
          newSenderInfo =
            activeList[Math.floor(Math.random() * activeList.length)]
        }
        retry++
      }
    }
    if (nodeIsUnsubscribed && !nodeIsInTheActiveList) {
      if (config.VERBOSE) console.log('Unsubscribe 6', publicKey)
      unsubscribeDataSender(publicKey)
      nodeIsUnsubscribed = false
    }
  }
  // Temp hack to pick half of the nodes not to miss data at all
  if (calculatedConsensusRadius === 2) {
    // const subsetList = activeList.slice(0, Math.floor(activeList.length / 2))
    // for (let node of Object.values(subsetList)) {
    //   if (!socketClients.has(node.publicKey)) {
    //     let connectionStatus = await createDataTransferConnection(node)
    //     if (connectionStatus) await Utils.sleep(10000) // sleep for 10
    //   }
    // }
    let extraConsensorsToSubscribe = Math.floor((activeList.length / 4) * 3)
    if (socketClients.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= socketClients.size
      Logger.mainLogger.debug(
        'extraConsensorsToSubscribe',
        extraConsensorsToSubscribe
      )
      const retryTimes = 2
      let subscribedSuccess = 0
      let retry = 0

      let remainingActiveList = [...activeList]
      if (subscribedSuccess < extraConsensorsToSubscribe) {
        for (const key of socketClients.keys()) {
          remainingActiveList = remainingActiveList.filter(
            (node) => node.publicKey !== key
          )
        }
        if (config.VERBOSE)
          Logger.mainLogger.debug(
            'remainingActiveList',
            remainingActiveList,
            socketClients.keys()
          )
      }

      while (subscribedSuccess < extraConsensorsToSubscribe) {
        if (retry === retryTimes) {
          break
        }
        if (remainingActiveList.length === 0) {
          remainingActiveList = [...activeList]
          if (subscribedSuccess < extraConsensorsToSubscribe) {
            for (const key of socketClients.keys()) {
              remainingActiveList = remainingActiveList.filter(
                (node) => node.publicKey !== key
              )
            }
          }
          if (remainingActiveList.length === 0) {
            break
          }
          retry++
        }
        let newSenderInfo =
          remainingActiveList[
            Math.floor(Math.random() * remainingActiveList.length)
          ]
        if (!socketClients.has(newSenderInfo.publicKey)) {
          let connectionStatus = await createDataTransferConnection(
            newSenderInfo
          )
          if (connectionStatus) {
            subscribedSuccess++
          } else {
            if (socketClients.has(newSenderInfo.publicKey))
              socketClients.delete(newSenderInfo.publicKey)
          }
          socketConnectionsTracker.delete(newSenderInfo.publicKey)
        }
        remainingActiveList = remainingActiveList.filter(
          (node) => node.publicKey !== newSenderInfo.publicKey
        )
      }
    }
  }
  if (queueForSelectingNewDataSenders.size > 0) {
    const newPublicKeys = Object.keys(queueForSelectingNewDataSenders)
    queueForSelectingNewDataSenders.clear()
    selectNewDataSendersByConsensusRadius(newPublicKeys)
  } else {
    selectingNewDataSender = false
  }
}

async function getConsensusRadius() {
  const activeList = NodeList.getActiveList()
  let randomNode = activeList[Math.floor(Math.random() * activeList.length)]
  Logger.mainLogger.debug(
    `Checking network configs from random node ${randomNode.ip}:${randomNode.port}`
  )
  let response: any = await P2P.getJson(
    `http://${randomNode.ip}:${randomNode.port}/netconfig`
  )
  if (response && response.config) {
    const nodesPerConsensusGroup =
      response.config.sharding.nodesPerConsensusGroup
    const consensusRadius = Math.floor((nodesPerConsensusGroup - 1) / 2)
    Logger.mainLogger.debug('consensusRadius', consensusRadius)
    console.log('consensusRadius', consensusRadius)
    return consensusRadius
  }
  return activeList.length
}

export async function createDataTransferConnection(
  newSenderInfo: NodeList.ConsensusNodeInfo
) {
  initSocketClient(newSenderInfo)
  let count = 0
  while (!socketClients.has(newSenderInfo.publicKey) && count <= 50) {
    await Utils.sleep(200)
    count++
    if (count === 50) {
      // This means the socket connection to the node is not successful.
      return false
    }
  }
  await Utils.sleep(200) // Wait 1s before sending data request to be sure if the connection is refused
  if (socketConnectionsTracker.get(newSenderInfo.publicKey) === 'disconnected')
    return false
  const newSender: DataSender = {
    nodeInfo: newSenderInfo,
    types: [
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
    ],
    contactTimeout: createContactTimeout(
      newSenderInfo.publicKey,
      'This timeout is created during newSender selection',
      2 * currentCycleDuration
    ),
    replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
  }

  // Add new dataSender to dataSenders
  addDataSenders(newSender)
  Logger.mainLogger.debug(
    `replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`
  )

  // Send dataRequest to new dataSender
  const dataRequest = {
    dataRequestCycle: createDataRequest<Cycle>(
      P2PTypes.SnapshotTypes.TypeNames.CYCLE,
      currentCycleCounter,
      State.getNodeInfo().publicKey
    ),
    dataRequestStateMetaData:
      createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
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

export async function subscribeMoreConsensorsByConsensusRadius() {
  const calculatedConsensusRadius = await getConsensusRadius()
  let consensusRadius = calculatedConsensusRadius
  if (consensusRadius > 2) consensusRadius-- // Change default to 3 for now assuming nodesPerConsensusGroup 10
  const activeList = NodeList.getActiveList()
  if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
  const totalNumberOfNodesToSubscribe = Math.ceil(
    activeList.length / consensusRadius
  )
  Logger.mainLogger.debug(
    'totalNumberOfNodesToSubscribe',
    totalNumberOfNodesToSubscribe
  )
  console.log('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  for (let i = 0; i < activeList.length; i += consensusRadius) {
    let subsetList = activeList.slice(i, i + consensusRadius)
    if (config.VERBOSE)
      console.log('Round', i, 'subsetList', subsetList, socketClients.keys())
    let noNodeFromThisSubset = true
    for (let node of Object.values(subsetList)) {
      if (socketClients.has(node.publicKey)) {
        if (config.VERBOSE) console.log('The node is found in this subset')
        noNodeFromThisSubset = false
      }
    }

    if (!noNodeFromThisSubset) {
      Logger.mainLogger.debug(
        'There is already node from this subset or node to rotate is not from this subset!'
      )
      console.log(
        'There is already node from this subset or node to rotate is not from this subset!'
      )
      continue
    }
    // Pick a new dataSender
    let newSenderInfo =
      subsetList[Math.floor(Math.random() * subsetList.length)]
    let connectionStatus = false
    let retry = 0
    while (true && retry < consensusRadius) {
      // Retry 5 times to get the new Sender
      if (!socketClients.has(newSenderInfo.publicKey)) {
        connectionStatus = await createDataTransferConnection(newSenderInfo)
        if (connectionStatus) {
          // if (noNodeFromThisSubset) await Utils.sleep(30000) // Start another node with 30s difference
          break
        } else {
          if (socketClients.has(newSenderInfo.publicKey))
            socketClients.delete(newSenderInfo.publicKey)
          subsetList = subsetList.filter(
            (node) => node.publicKey !== newSenderInfo.publicKey
          )
        }
        socketConnectionsTracker.delete(newSenderInfo.publicKey)
      }
      if (subsetList.length > 0) {
        newSenderInfo =
          subsetList[Math.floor(Math.random() * subsetList.length)]
      } else {
        break
      }
      retry++
    }
  }
  // Temp hack to pick half of the nodes not to miss data at all
  if (calculatedConsensusRadius === 2) {
    // const subsetList = activeList.slice(0, Math.floor(activeList.length / 2))
    // for (let node of Object.values(subsetList)) {
    //   if (!socketClients.has(node.publicKey)) {
    //     let connectionStatus = await createDataTransferConnection(node)
    //     if (connectionStatus) await Utils.sleep(10000) // sleep for 10
    //   }
    // }
    let extraConsensorsToSubscribe = Math.floor((activeList.length / 4) * 3)
    if (socketClients.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= socketClients.size
      Logger.mainLogger.debug(
        'extraConsensorsToSubscribe',
        extraConsensorsToSubscribe
      )
      const retryTimes = 2
      let subscribedSuccess = 0
      let retry = 0

      let remainingActiveList = [...activeList]
      if (subscribedSuccess < extraConsensorsToSubscribe) {
        for (const key of socketClients.keys()) {
          remainingActiveList = remainingActiveList.filter(
            (node) => node.publicKey !== key
          )
        }
        if (config.VERBOSE)
          Logger.mainLogger.debug(
            'remainingActiveList',
            remainingActiveList.length,
            socketClients.keys()
          )
      }

      while (subscribedSuccess < extraConsensorsToSubscribe) {
        if (retry === retryTimes) {
          break
        }
        if (remainingActiveList.length === 0) {
          remainingActiveList = [...activeList]
          if (subscribedSuccess < extraConsensorsToSubscribe) {
            for (const key of socketClients.keys()) {
              remainingActiveList = remainingActiveList.filter(
                (node) => node.publicKey !== key
              )
            }
          }
          if (remainingActiveList.length === 0) {
            break
          }
          retry++
        }
        let newSenderInfo =
          remainingActiveList[
            Math.floor(Math.random() * remainingActiveList.length)
          ]
        Logger.mainLogger.debug(
          'newSenderInfo',
          newSenderInfo,
          remainingActiveList
        )
        if (!socketClients.has(newSenderInfo.publicKey)) {
          let connectionStatus = await createDataTransferConnection(
            newSenderInfo
          )
          if (connectionStatus) {
            subscribedSuccess++
          } else {
            if (socketClients.has(newSenderInfo.publicKey))
              socketClients.delete(newSenderInfo.publicKey)
          }
          socketConnectionsTracker.delete(newSenderInfo.publicKey)
        }
        remainingActiveList = remainingActiveList.filter(
          (node) => node.publicKey !== newSenderInfo.publicKey
        )
      }
    }
  }
  Logger.mainLogger.debug(
    'Subscribed socketClients',
    socketClients.size,
    dataSenders.size
  )
}

export function sendDataRequest(sender: DataSender, dataRequest: any) {
  const taggedDataRequest = Crypto.tag(dataRequest, sender.nodeInfo.publicKey)
  Logger.mainLogger.info('Sending tagged data request to consensor.', sender)
  if (socketClients.has(sender.nodeInfo.publicKey))
    emitter.emit('selectNewDataSender', sender.nodeInfo, taggedDataRequest)
}

export async function subscribeMoreConsensors(numbersToSubscribe: number) {
  Logger.mainLogger.info(
    `Subscribing ${numbersToSubscribe} more consensors for tagged data request`
  )
  console.log('numbersToSubscribe', numbersToSubscribe)
  for (let i = 0; i < numbersToSubscribe; i++) {
    await Utils.sleep(60000)
    // Pick a new dataSender
    const activeList = NodeList.getActiveList()
    let newSenderInfo =
      activeList[Math.floor(Math.random() * activeList.length)]
    if (multipleDataSenders) {
      let retry = 0
      while (true && retry < 5) {
        // Retry 5 times to get the new Sender
        if (!socketClients.has(newSenderInfo.publicKey)) {
          break
        }
        newSenderInfo =
          activeList[Math.floor(Math.random() * activeList.length)]
        retry++
      }
    }
    Logger.mainLogger.debug('New data sender is selected', newSenderInfo)
    if (!newSenderInfo) {
      Logger.mainLogger.error('Unable to select a new data sender.')
      continue
    }
    if (
      multipleDataSenders &&
      socketClients.size > consensorsCountToSubscribe
    ) {
      Logger.mainLogger.debug(
        `There are already ${socketClients.size} nodes that the archiver has picked.`
      )
      console.log(
        `There are already ${socketClients.size} nodes that the archiver has picked.`
      )
      break
    }
    initSocketClient(newSenderInfo)
    const newSender: DataSender = {
      nodeInfo: newSenderInfo,
      types: [
        P2PTypes.SnapshotTypes.TypeNames.CYCLE,
        P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA,
      ],
      contactTimeout: createContactTimeout(
        newSenderInfo.publicKey,
        'This timeout is created during newSender selection',
        2 * currentCycleDuration
      ),
      replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
    }

    // Add new dataSender to dataSenders
    addDataSenders(newSender)
    Logger.mainLogger.debug(
      `replaceDataSender: added new sender ${newSenderInfo.publicKey} to dataSenders`
    )

    // Send dataRequest to new dataSender
    const dataRequest = {
      dataRequestCycle: createDataRequest<Cycle>(
        P2PTypes.SnapshotTypes.TypeNames.CYCLE,
        currentCycleCounter,
        State.getNodeInfo().publicKey
      ),
      dataRequestStateMetaData:
        createDataRequest<P2PTypes.SnapshotTypes.StateMetaData>(
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
    let isJoined
    if (checkFromConsensor)
      isJoined = await checkJoinStatusFromConsensor(nodeList)
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
  let request = P2P.createArchiverJoinRequest()
  let shuffledNodes = [...nodeList]
  Utils.shuffleArray(shuffledNodes)

  // Wait until a Q1 then send join request to active nodes
  let untilQ1 = startQ1 - Date.now()
  while (untilQ1 < 0) {
    untilQ1 += latestCycle.duration * 1000
  }

  Logger.mainLogger.debug(
    `Waiting ${untilQ1 + 500} ms for Q1 before sending join...`
  )
  await Utils.sleep(untilQ1 + 500) // Not too early

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
  Logger.mainLogger.debug(
    `Sending join request to ${selectedNodes.map((n) => `${n.ip}:${n.port}`)}`
  )
  for (const node of selectedNodes) {
    let response = await P2P.postJson(
      `http://${node.ip}:${node.port}/joinarchiver`,
      joinRequest
    )
    Logger.mainLogger.debug('Join request response:', response)
  }
}

export function sendLeaveRequest(
  nodeInfo: NodeList.ConsensusNodeInfo,
  cycle: Cycles.Cycle
) {
  let leaveRequest = P2P.createArchiverLeaveRequest()
  Logger.mainLogger.debug('Emitting submitLeaveRequest event')
  emitter.emit('submitLeaveRequest', nodeInfo, leaveRequest)
  return true
}

export async function getCycleDuration() {
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)[0]
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`
  )
  if (response && response.cycleInfo) {
    return response.cycleInfo[0].duration
  }
}

export async function getNewestCycleFromConsensors(
  activeNodes: NodeList.ConsensusNodeInfo[]
): Promise<Cycle> {
  function isSameCyceInfo(info1: any, info2: any) {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    return isDeepStrictEqual(cm1, cm2)
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(
      `http://${node.ip}:${node.port}/sync-newest-cycle`
    )
    if (response.newestCycle) return response.newestCycle
  }
  let newestCycle: any = await Utils.robustQuery(
    activeNodes,
    queryFn,
    isSameCyceInfo
  )
  return newestCycle[0]
}

export function checkJoinStatus(): Promise<boolean> {
  Logger.mainLogger.debug('Checking join status')
  const ourNodeInfo = State.getNodeInfo()
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)[0]

  return new Promise(async (resolve) => {
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

export function checkJoinStatusFromConsensor(
  nodeList: NodeList.ConsensusNodeInfo[]
): Promise<boolean> {
  Logger.mainLogger.debug('Checking join status from consenosr')
  const ourNodeInfo = State.getNodeInfo()

  return new Promise(async (resolve) => {
    const latestCycle = await getNewestCycleFromConsensors(nodeList)
    try {
      if (
        latestCycle &&
        latestCycle.joinedArchivers &&
        latestCycle.refreshedArchivers
      ) {
        let joinedArchivers = latestCycle.joinedArchivers
        let refreshedArchivers = latestCycle.refreshedArchivers
        Logger.mainLogger.debug('cycle counter', latestCycle.counter)
        Logger.mainLogger.debug('Joined archivers', joinedArchivers)

        let isJoind: any = [...joinedArchivers, ...refreshedArchivers].find(
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

async function sendDataQuery(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: any,
  validateFn: any
) {
  const taggedDataQuery = Crypto.tag(dataQuery, consensorNode.publicKey)
  let result = await queryDataFromNode(
    consensorNode,
    taggedDataQuery,
    validateFn
  )
  return result
}

async function processData(
  newData: DataResponse<P2PTypes.SnapshotTypes.ValidTypes> &
    Crypto.TaggedMessage
) {
  // Get sender entry
  const sender = dataSenders.get(newData.publicKey)

  // If no sender entry, remove publicKey from senders, END
  if (!sender) {
    Logger.mainLogger.error('No sender found for this data')
    return
  }

  if (
    multipleDataSenders &&
    !currentDataSenders.includes(sender.nodeInfo.publicKey)
  ) {
    Logger.mainLogger.error(
      `Sender ${sender.nodeInfo.publicKey} is not in the data sender list.`
    )
  }
  if (!multipleDataSenders && sender.nodeInfo.publicKey !== currentDataSender) {
    Logger.mainLogger.error(
      `Sender ${sender.nodeInfo.publicKey} is not current data sender.`
    )
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
        processCycles(newData.responses.CYCLE as Cycle[])
        // socketServer.emit('ARCHIVED_CYCLE', 'CYCLE')
        if (newData.responses.CYCLE.length > 0) {
          for (let cycle of newData.responses.CYCLE) {
            let archivedCycle: any = {}
            archivedCycle.cycleRecord = cycle
            archivedCycle.cycleMarker = cycle.marker
            Cycles.CycleChain.set(cycle.counter, cycle)
            await Storage.insertArchivedCycle(archivedCycle)
          }
        } else {
          Logger.mainLogger.error(
            'Received empty newData.responses.CYCLE',
            newData.responses
          )
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

export async function processStateMetaData(response: any) {
  Logger.mainLogger.error('response', response)
  let STATE_METADATA = response.STATE_METADATA
  if (!STATE_METADATA || STATE_METADATA.length === 0) {
    Logger.mainLogger.error(
      'Invalid STATE_METADATA provided to processStateMetaData function',
      STATE_METADATA
    )
    return
  }
  profilerInstance.profileSectionStart('state_metadata')
  for (let stateMetaData of STATE_METADATA) {
    let data, receipt, summary
    // [TODO] validate the state data by robust querying other nodes

    // store state hashes to archivedCycle
    for (const stateHashesForCycle of stateMetaData.stateHashes) {
      let parentCycle = Cycles.CycleChain.get(stateHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error(
          'Unable to find parent cycle for cycle',
          stateHashesForCycle.counter
        )
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
      let parentCycle = Cycles.CycleChain.get(receiptHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error(
          'Unable to find parent cycle for cycle',
          receiptHashesForCycle.counter
        )
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
        let failedPartitions = new Map()
        let coveredPartitions = new Map()
        let downloadedReceiptMaps = new Map()

        let shouldProcessReceipt = (cycle: number, partition: number) => {
          if (cycle === receiptHashesForCycle.counter - 1)
            if (
              failedPartitions.has(partition) ||
              !coveredPartitions.has(partition)
            )
              return true
          return false
        }
        const cycleActiveNodesSize =
          parentCycle.active +
          parentCycle.activated.length -
          parentCycle.removed.length
        while (!isDownloadSuccess && sleepCount < 20) {
          let randomConsensor = NodeList.getRandomActiveNode()[0]
          const queryRequest = createQueryRequest(
            'RECEIPT_MAP',
            receiptHashesForCycle.counter - 1,
            randomConsensor.publicKey
          )
          let { success, completed, failed, covered, blobs } =
            await sendDataQuery(
              randomConsensor,
              queryRequest,
              shouldProcessReceipt
            )
          if (success) {
            for (let partition of failed) {
              failedPartitions.set(partition, true)
            }
            for (let partition of completed) {
              if (failedPartitions.has(partition))
                failedPartitions.delete(partition)
            }
            for (let partition of covered) {
              coveredPartitions.set(partition, true)
            }
            for (let partition in blobs) {
              downloadedReceiptMaps.set(partition, blobs[partition])
            }
          }
          isDownloadSuccess =
            failedPartitions.size === 0 &&
            coveredPartitions.size === cycleActiveNodesSize
          if (isDownloadSuccess) {
            Logger.mainLogger.debug(
              'Data query for receipt map is completed for cycle',
              parentCycle.counter
            )
            Logger.mainLogger.debug(
              'Total downloaded receipts',
              downloadedReceiptMaps.size
            )
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
          if (!isDownloadSuccess && retry >= NodeList.getActiveList().length) {
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
          Logger.mainLogger.debug(
            `Downloading receipt map for cycle ${parentCycle.counter} has failed.`
          )
          Logger.mainLogger.debug(
            `There are ${failedPartitions.size} failed partitions in cycle ${parentCycle.counter}.`
          )
        }
      }
    }

    // store summary hashes to archivedCycle
    for (const summaryHashesForCycle of stateMetaData.summaryHashes) {
      let parentCycle = Cycles.CycleChain.get(summaryHashesForCycle.counter)
      if (!parentCycle) {
        Logger.mainLogger.error(
          'Unable to find parent cycle for cycle',
          summaryHashesForCycle.counter
        )
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
      //     if (!isDownloadSuccess && retry >= NodeList.getActiveList().length) {
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

export async function sendToExplorer(counter) {
  if (socketServer) {
    let completedArchivedCycle
    if (lastSentCycleCounterToExplorer === 0) {
      completedArchivedCycle = await Storage.queryAllArchivedCyclesBetween(
        lastSentCycleCounterToExplorer,
        counter
      )
      Logger.mainLogger.debug(
        'start, end',
        lastSentCycleCounterToExplorer,
        counter
      )
    } else {
      completedArchivedCycle = await Storage.queryAllArchivedCyclesBetween(
        counter,
        counter
      )
      Logger.mainLogger.debug('start, end', counter, counter)
    }
    let signedDataToSend = Crypto.sign({
      archivedCycles: completedArchivedCycle,
    })
    Logger.mainLogger.debug(
      'Sending completed archived_cycle to explorer',
      signedDataToSend
    )
    lastSentCycleCounterToExplorer = counter
    if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
  }
}

export async function fetchStateHashes(archivers: any) {
  function _isSameStateHashes(info1: any, info2: any) {
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
  const stateHashes: any = await Utils.robustQuery(
    archivers,
    queryFn,
    _isSameStateHashes
  )
  return stateHashes[0]
}

export async function fetchCycleRecords(
  activeArchivers: State.ArchiverNodeInfo[],
  start: number,
  end: number
): Promise<any> {
  function isSameCyceInfo(info1: any, info2: any) {
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

export async function getNewestCycleFromArchivers(
  activeArchivers: State.ArchiverNodeInfo[]
): Promise<any> {
  function isSameCyceInfo(info1: any, info2: any) {
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
  constructor() {
    this.final = {
      added: [],
      removed: [],
      updated: [],
    }
    this.addedIds = new Set()
    this.removedIds = new Set()
    this.seenUpdates = new Map()
  }

  addChange(change: Change) {
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

export function parseRecord(record: any): Change {
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

export function parse(record: any): Change {
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

    NodeList.addNodes(
      NodeList.Statuses.ACTIVE,
      change.added[0].cycleJoined,
      consensorInfos
    )
  }
  if (change.removed.length > 0) {
    NodeList.removeNodes(change.removed)
  }
}

export async function syncGenesisAccountsFromArchiver(
  activeArchivers: State.ArchiverNodeInfo[]
) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let complete = false
  let startAccount = 0
  let endAccount = startAccount + 10000
  let combineAccountsData = []
  let totalGenesisAccounts = 0
  // const totalExistingGenesisAccounts =
  //   await AccountDB.queryAccountCountBetweenCycles(0, 5);
  // if (totalExistingGenesisAccounts > 0) {
  //   // Let's assume it has synced data for now, update to sync account count between them
  //   return;
  // }
  let res: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/account?startCycle=0&endCycle=5`
  )
  if (res && res.totalAccounts) {
    totalGenesisAccounts = res.totalAccounts
    Logger.mainLogger.debug('TotalGenesis Accounts', totalGenesisAccounts)
  } else {
    Logger.mainLogger.error(
      'Genesis Total Accounts Query',
      'Invalid download response'
    )
    return
  }
  if (totalGenesisAccounts <= 0) return
  let page = 0
  while (!complete) {
    Logger.mainLogger.debug(
      `Downloading accounts from ${startAccount} to ${endAccount}`
    )
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/account?startCycle=0&endCycle=5&page=${page}`
    )
    if (response && response.accounts) {
      if (response.accounts.length < 10000) {
        complete = true
        Logger.mainLogger.debug('Download completed for accounts')
      }
      Logger.mainLogger.debug(`Downloaded accounts`, response.accounts.length)
      combineAccountsData = [...combineAccountsData, ...response.accounts]
    } else {
      Logger.mainLogger.debug(
        'Genesis Accounts Query',
        'Invalid download response'
      )
    }
    startAccount = endAccount
    endAccount += 10000
    page++
    // await sleep(1000);
  }
  await storeAccountData(combineAccountsData)
  Logger.mainLogger.debug('Sync genesis accounts completed!')
}

export async function syncGenesisAccountsFromConsensor(
  totalGenesisAccounts = 0,
  firstConsensor: NodeList.ConsensusNodeInfo
) {
  if (totalGenesisAccounts <= 0) return
  let complete = false
  let startAccount = 0
  // let combineAccountsData = [];
  let totalDownloadedAccounts = 0
  while (startAccount <= totalGenesisAccounts) {
    Logger.mainLogger.debug(`Downloading accounts from ${startAccount}`)
    let response: any = await P2P.getJson(
      `http://${firstConsensor.ip}:${firstConsensor.port}/genesis_accounts?start=${startAccount}`
    )
    if (response && response.accounts) {
      if (response.accounts.length < 1000) {
        complete = true
        Logger.mainLogger.debug('Download completed for accounts')
      }
      Logger.mainLogger.debug(`Downloaded accounts`, response.accounts.length)
      await storeAccountData(response.accounts)
      // combineAccountsData = [...combineAccountsData, ...response.accounts];
      totalDownloadedAccounts += response.accounts.length
    } else {
      Logger.mainLogger.debug(
        'Genesis Accounts Query',
        'Invalid download response'
      )
    }
    startAccount += 1000
    // await sleep(1000);
  }
  Logger.mainLogger.debug(`Total downloaded accounts`, totalDownloadedAccounts)
  // await storeAccountData(combineAccountsData);
  Logger.mainLogger.debug('Sync genesis accounts completed!')
}

export async function buildNodeListFromStoredCycle(
  lastStoredCycle: Cycles.Cycle
) {
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
      `Got ${
        squasher.final.updated.length
      } active nodes, need ${activeNodeCount(lastStoredCycle)}`
    )
    Logger.mainLogger.debug(
      `Got ${squasher.final.added.length} total nodes, need ${totalNodeCount(
        lastStoredCycle
      )}`
    )
    if (squasher.final.added.length < totalNodeCount(lastStoredCycle))
      Logger.mainLogger.debug(
        'Short on nodes. Need to get more cycles. Cycle:' +
          lastStoredCycle.counter
      )

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

export async function syncCyclesAndNodeList(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredCycleCount: number = 0
) {
  // Get the networks newest cycle as the anchor point for sync
  Logger.mainLogger.debug('Getting newest cycle...')
  const [cycleToSyncTo] = await getNewestCycleFromArchivers(activeArchivers)
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
      `Got ${
        squasher.final.updated.length
      } active nodes, need ${activeNodeCount(cycleToSyncTo)}`
    )
    Logger.mainLogger.debug(
      `Got ${squasher.final.added.length} total nodes, need ${totalNodeCount(
        cycleToSyncTo
      )}`
    )
    if (squasher.final.added.length < totalNodeCount(cycleToSyncTo))
      Logger.mainLogger.debug(
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
  Logger.mainLogger.debug('NodeList after sync', NodeList.getActiveList())

  for (let i = 0; i < CycleChain.length; i++) {
    let record = CycleChain[i]
    Cycles.CycleChain.set(record.counter, { ...record })
    if (config.experimentalSnapshot) {
      if (i === CycleChain.length - 1) storeCycleData(CycleChain)
    } else {
      Logger.mainLogger.debug(
        'Inserting archived cycle for counter',
        record.counter
      )
      const archivedCycle = createArchivedCycle(record)
      await Storage.insertArchivedCycle(archivedCycle)
    }
    Cycles.setCurrentCycleCounter(record.counter)
  }
  Logger.mainLogger.debug(
    'Cycle chain is synced. Size of CycleChain',
    Cycles.CycleChain.size
  )

  // Download old cycle Records
  let endCycle = CycleChain[0].counter - 1
  Logger.mainLogger.debug(
    'endCycle counter',
    endCycle,
    'lastStoredCycleCount',
    lastStoredCycleCount
  )
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
    const prevCycles = await fetchCycleRecords(
      activeArchivers,
      nextEnd,
      endCycle
    )

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
      if (config.experimentalSnapshot) {
        combineCycles.push(prevCycle)
      } else {
        Logger.mainLogger.debug(
          'Inserting archived cycle for counter',
          prevCycle.counter
        )
        const archivedCycle = createArchivedCycle(prevCycle)
        await Storage.insertArchivedCycle(archivedCycle)
      }
    }
    if (config.experimentalSnapshot) storeCycleData(combineCycles)
    endCycle = nextEnd - 1
  }

  return true
}

function createArchivedCycle(cycleRecord: Cycle) {
  let archivedCycle: any = {
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
  startCycle: number = 0
) {
  let complete = false
  let lastData = startCycle
  let collector: any = []
  let count = 0
  let maxCount = Math.ceil((cycleToSyncTo - startCycle) / 5)
  while (!complete && count < maxCount) {
    Logger.mainLogger.debug(
      `Downloading archive from cycle ${lastData} to cycle ${lastData + 5}`
    )
    let response: any = await P2P.getJson(
      `http://${archiver.ip}:${
        archiver.port
      }/full-archive?start=${lastData}&end=${lastData + 5}`
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

export async function syncReceipts(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredReceiptCount: number = 0
) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`
  )
  if (!response || response.totalReceipts < 0) {
    return false
  }
  const { totalCycles, totalReceipts } = response
  await downloadReceipts(totalReceipts, lastStoredReceiptCount, randomArchiver)
  Logger.mainLogger.debug('Sync receipts data completed!')
  return false
}

export const downloadReceipts = async (
  to: number,
  from: number = 0,
  archiver: State.ArchiverNodeInfo
) => {
  let complete = false
  let start = from
  let end = start + 1000
  while (!complete) {
    if (end >= to) {
      let res: any = await P2P.getJson(
        `http://${archiver.ip}:${archiver.port}/totalData`
      )
      if (res && res.totalReceipts > 0) {
        if (res.totalReceipts > to) to = res.totalReceipts
        Logger.mainLogger.debug('totalReceiptsToSync', to)
      }
    }
    Logger.mainLogger.debug(`Downloading receipts from ${start} to  ${end}`)
    let response: any = await P2P.getJson(
      `http://${archiver.ip}:${archiver.port}/receipt?start=${start}&end=${end}`
    )
    if (response && response.receipts) {
      const downloadedReceipts = response.receipts
      Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
      await storeReceiptData(downloadedReceipts)
      if (response.receipts.length < 1000) {
        let res: any = await P2P.getJson(
          `http://${archiver.ip}:${archiver.port}/totalData`
        )
        start += response.receipts.length
        end = start + 1000
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
    end += 1000
  }
}

export async function syncReceiptsByCycle(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredReceiptCycle: number = 0
) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`
  )
  if (!response || response.totalReceipts < 0) {
    return false
  }
  let { totalCycles, totalReceipts } = response
  let complete = false
  let startCycle = lastStoredReceiptCycle
  let endCycle = startCycle + 100
  let receiptsCountToSyncBetweenCycles = 0
  let savedReceiptsCountBetweenCycles = 0
  let totalSavedReceiptsCount = 0
  while (!complete) {
    if (endCycle > totalCycles) {
      endCycle = totalCycles
      totalSavedReceiptsCount = await ReceiptDB.queryReceiptCount()
    }
    if (totalSavedReceiptsCount >= totalReceipts) {
      let res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`
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
    if (startCycle > endCycle) {
      Logger.mainLogger.error(
        `Got some issues in syncing receipts. Receipts query startCycle ${startCycle} is greater than endCycle ${endCycle}`
      )
      break
    }
    Logger.mainLogger.debug(
      `Downloading receipts from cycle ${startCycle} to cycle ${endCycle}`
    )
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`
    )
    if (response && response.receipts > 0) {
      receiptsCountToSyncBetweenCycles = response.receipts
      let page = 1
      savedReceiptsCountBetweenCycles = 0
      while (
        savedReceiptsCountBetweenCycles < receiptsCountToSyncBetweenCycles
      ) {
        response = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&page=${page}`
        )
        if (response && response.receipts) {
          const downloadedReceipts = response.receipts
          Logger.mainLogger.debug(
            `Downloaded receipts`,
            downloadedReceipts.length
          )
          await storeReceiptData(downloadedReceipts)
          savedReceiptsCountBetweenCycles += downloadedReceipts.length
          if (
            savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles
          ) {
            response = await P2P.getJson(
              `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`
            )
            if (response && response.receipts)
              receiptsCountToSyncBetweenCycles = response.receipts
            if (
              receiptsCountToSyncBetweenCycles > savedReceiptsCountBetweenCycles
            ) {
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
          if (
            savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles
          ) {
            Logger.mainLogger.debug(
              'There are more cycles than it supposed to have'
            )
          }
          totalSavedReceiptsCount += downloadedReceipts.length
          page++
        } else {
          Logger.mainLogger.debug('Invalid download response')
          continue
        }
      }
      Logger.mainLogger.debug(
        `Download receipts completed for ${startCycle} - ${endCycle}`
      )
      startCycle = endCycle + 1
      endCycle += 100
    } else {
      receiptsCountToSyncBetweenCycles = response.receipts
      if (receiptsCountToSyncBetweenCycles === 0) {
        startCycle = endCycle + 1
        endCycle += 100
        continue
      }
      Logger.mainLogger.debug('Invalid download response')
      continue
    }
  }
  return false
}

export const syncCyclesAndReceiptsData = async (
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredCycleCount: number = 0,
  lastStoredReceiptCount: number = 0
) => {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let response: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`
  )
  if (!response || response.totalCycles < 0 || response.totalReceipts < 0) {
    return false
  }
  const { totalCycles, totalReceipts } = response
  Logger.mainLogger.debug(
    totalCycles,
    lastStoredCycleCount,
    totalReceipts,
    lastStoredReceiptCount
  )
  if (
    totalCycles === lastStoredCycleCount &&
    totalReceipts === lastStoredReceiptCount
  ) {
    Logger.mainLogger.debug(
      'The archiver has synced the lastest cycle and receipts data!'
    )
    return false
  }
  let totalReceiptsToSync = totalReceipts
  let totalCyclesToSync = totalCycles
  let completeForReceipt = false
  let completeForCycle = false
  let startReceipt = lastStoredReceiptCount
  let startCycle = lastStoredCycleCount
  let endReceipt = startReceipt + 1000
  let endCycle = startCycle + 1000

  while (!completeForReceipt || !completeForCycle) {
    if (endReceipt >= totalReceiptsToSync || endCycle >= totalCyclesToSync) {
      response = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/totalData`
      )
      if (response && response.totalReceipts && response.totalCycles) {
        if (response.totalReceipts !== totalReceiptsToSync) {
          completeForReceipt = false
          totalReceiptsToSync = response.totalReceipts
        }
        if (response.totalCycles !== totalCyclesToSync) {
          completeForCycle = false
          totalCyclesToSync = response.totalCycles
        }
        if (totalReceiptsToSync === startReceipt) {
          completeForReceipt = true
        }
        if (totalCyclesToSync === startCycle) {
          completeForCycle = true
        }
        Logger.mainLogger.debug(
          'totalReceiptsToSync',
          totalReceiptsToSync,
          'totalCyclesToSync',
          totalCyclesToSync
        )
      }
    }
    if (!completeForReceipt) {
      Logger.mainLogger.debug(
        `Downloading receipts from ${startReceipt} to ${endReceipt}`
      )
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?start=${startReceipt}&end=${endReceipt}`
      )
      if (res && res.receipts) {
        const downloadedReceipts = res.receipts
        Logger.mainLogger.debug(
          `Downloaded receipts`,
          downloadedReceipts.length
        )
        await storeReceiptData(downloadedReceipts)
        if (downloadedReceipts.length < 1000) {
          startReceipt += downloadedReceipts.length
          endReceipt = startReceipt + 1000
          continue
        }
      } else {
        Logger.mainLogger.debug('Invalid download response')
      }
      startReceipt = endReceipt
      endReceipt += 1000
    }
    if (!completeForCycle) {
      Logger.mainLogger.debug(
        `Downloading cycles from ${startCycle} to ${endCycle}`
      )
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo?start=${startCycle}&end=${endCycle}`
      )
      if (res && res.cycleInfo) {
        Logger.mainLogger.debug(`Downloaded cycles`, res.cycleInfo.length)
        const cycles = res.cycleInfo
        processCycles(cycles)
        await storeCycleData(cycles)
        if (res.cycleInfo.length < 1000) {
          startCycle += res.cycleInfo.length
          endCycle = startCycle + 1000
          continue
        }
      } else {
        Logger.mainLogger.debug('Cycle', 'Invalid download response')
      }
      startCycle = endCycle
      endCycle += 1000
    }
  }
  Logger.mainLogger.debug('Sync Cycle and Receipt data completed!')
  return false
}

// export const downloadAndSaveAccounts = async (
//   to: number,
//   from: number = 0,
//   archiver: State.ArchiverNodeInfo
// ) => {
//   let complete = false;
//   let start = from;
//   let end = start + 10;
//   while (!complete) {
//     if (end >= to) {
//       let res: any = await P2P.getJson(
//         `http://${archiver.ip}:${archiver.port
//         }/totalData`
//       )
//       if (res && res.totalAccounts >= 0
//       ) {
//         to = res.totalAccounts;
//         Logger.mainLogger.debug('totalAccountsToSync', to);
//       }
//     }
//     Logger.mainLogger.debug(`Downloading accounts from ${start} to  ${end}`);
//     const response = await axios.get(
//       `${ARCHIVER_URL}/full-archive?start=${start}&end=${end}`
//     );
//     if (response && response.data && response.data.archivedCycles) {
//       // collector = collector.concat(response.data.archivedCycles);
//       if (response.data.archivedCycles.length < 100) {
//         complete = true;
//         Logger.mainLogger.debug('Download completed');
//       }
//       const downloadedArchivedCycles = response.data.archivedCycles;
//       Logger.mainLogger.debug(
//         `Downloaded archived cycles`,
//         downloadedArchivedCycles.length
//       );
//       downloadedArchivedCycles.sort((a, b) =>
//         a.cycleRecord.counter > b.cycleRecord.counter ? 1 : -1
//       );
//       await insertArchivedCycleData(downloadedArchivedCycles);
//     } else {
//       Logger.mainLogger.debug('Invalid download response');
//     }
//     start = end;
//     end += 10;
//   }
// };

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

export async function compareWithOldReceiptsData(
  archiver: State.ArchiverNodeInfo,
  lastStoredReceiptCycle: number = 0
) {
  let endCycle = lastStoredReceiptCycle
  let startCycle = endCycle - 10 > 0 ? endCycle - 10 : 0
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`
  )
  let downloadedReceiptCountByCycles
  if (response && response.receipts) {
    downloadedReceiptCountByCycles = response.receipts
  } else {
    throw Error(
      `Can't fetch receipts data from cycle ${startCycle} to cycle ${endCycle}  from archiver ${archiver}`
    )
  }
  let oldReceiptCountByCycle = await ReceiptDB.queryReceiptCountByCycles(
    startCycle,
    endCycle
  )
  let success = false
  let matchedCycle = 0
  for (let i = 0; i < downloadedReceiptCountByCycles.length; i++) {
    const downloadedReceipt = downloadedReceiptCountByCycles[i]
    const oldReceipt = oldReceiptCountByCycle[i]
    Logger.mainLogger.debug(downloadedReceipt, oldReceipt)
    if (
      downloadedReceipt.cycle !== oldReceipt.cycle ||
      downloadedReceipt.receipts !== oldReceipt.receipts
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

export async function compareWithOldCyclesData(
  archiver: State.ArchiverNodeInfo,
  lastCycleCounter = 0
) {
  let downloadedCycles
  const response: any = await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/cycleinfo?start=${
      lastCycleCounter - 10
    }&end=${lastCycleCounter - 1}`
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
  let oldCycles = await CycleDB.queryCycleRecordsBetween(
    lastCycleCounter - 10,
    lastCycleCounter + 1
  )
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

export async function syncStateMetaData(
  activeArchivers: State.ArchiverNodeInfo[]
) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let allCycleRecords = await Storage.queryAllCycleRecords()
  let lastCycleCounter = allCycleRecords[0].counter
  let downloadedArchivedCycles = await downloadArchivedCycles(
    randomArchiver,
    lastCycleCounter
  )

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

  for (let i = 0; i < downloadedArchivedCycles.length; i++) {
    let marker = downloadedArchivedCycles[i].cycleRecord.marker
    let counter = downloadedArchivedCycles[i].cycleRecord.counter
    let downloadedArchivedCycle = downloadedArchivedCycles[i]

    if (!downloadedArchivedCycle) {
      Logger.mainLogger.debug(
        'Unable to download archivedCycle for counter',
        counter
      )
      continue
    }

    let isDataSynced = false
    let isReceiptSynced = false
    let isSummarySynced = false

    // Check and store data hashes
    if (downloadedArchivedCycle.data) {
      const downloadedNetworkDataHash = downloadedArchivedCycle.data.networkHash
      const calculatedDataHash = calculateNetworkHash(
        downloadedArchivedCycle.data.partitionHashes
      )
      if (downloadedNetworkDataHash === calculatedDataHash) {
        if (
          downloadedNetworkDataHash !==
          networkDataHashesFromRecords.get(counter)
        ) {
          Logger.mainLogger.debug(
            'Different with hash from downloaded Cycle Records',
            'state data',
            counter
          )
        }
        await Storage.updateArchivedCycle(
          marker,
          'data',
          downloadedArchivedCycle.data
        )
        isDataSynced = true
      } else {
        Logger.mainLogger.error(
          'Different network data hash for cycle',
          counter
        )
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have data field`
      )
    }

    // Check and store receipt hashes + receiptMap
    if (downloadedArchivedCycle.receipt) {
      // TODO: calcuate the network hash by hashing downloaded receipt Map instead of using downloadedNetworkReceiptHash
      const downloadedNetworkReceiptHash =
        downloadedArchivedCycle.receipt.networkHash
      const calculatedReceiptHash = calculateNetworkHash(
        downloadedArchivedCycle.receipt.partitionHashes
      )
      if (
        downloadedNetworkReceiptHash ===
          networkReceiptHashesFromRecords.get(counter) ||
        downloadedNetworkReceiptHash === calculatedReceiptHash
      ) {
        if (
          downloadedNetworkReceiptHash !==
          networkReceiptHashesFromRecords.get(counter)
        ) {
          Logger.mainLogger.debug(
            'Different with hash from downloaded Cycle Records',
            'receipt',
            counter
          )
        }
        await Storage.updateArchivedCycle(
          marker,
          'receipt',
          downloadedArchivedCycle.receipt
        )
        isReceiptSynced = true
      } else {
        Logger.mainLogger.error(
          'Different network receipt hash for cycle',
          counter
        )
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have receipt field`
      )
    }

    // Check and store summary hashes
    if (downloadedArchivedCycle.summary) {
      // TODO: calcuate the network hash by hashing downloaded summary Blobs instead of using downloadedNetworkSummaryHash
      const downloadedNetworkSummaryHash =
        downloadedArchivedCycle.summary.networkHash
      const calculatedSummaryHash = calculateNetworkHash(
        downloadedArchivedCycle.summary.partitionHashes
      )
      if (downloadedNetworkSummaryHash === calculatedSummaryHash) {
        if (
          downloadedNetworkSummaryHash !==
          networkSummaryHashesFromRecords.get(counter)
        ) {
          Logger.mainLogger.debug(
            'Different with hash from downloaded Cycle Records',
            'summary',
            counter
          )
        }
        await Storage.updateArchivedCycle(
          marker,
          'summary',
          downloadedArchivedCycle.summary
        )
        isSummarySynced = true
      } else {
        Logger.mainLogger.error(
          'Different network summary hash for cycle',
          counter
        )
      }
    } else {
      Logger.mainLogger.error(
        `ArchivedCycle ${downloadedArchivedCycle.cycleRecord.counter}, ${downloadedArchivedCycle.cycleMarker} does not have summary field`
      )
    }
    if (isDataSynced && isReceiptSynced && isSummarySynced) {
      Logger.mainLogger.debug(
        `Successfully synced statemetadata for counter ${counter}`
      )
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

export type QueryDataResponse =
  | ReceiptMapQueryResponse
  | StatsClumpQueryResponse

async function queryDataFromNode(
  consensorNode: NodeList.ConsensusNodeInfo,
  dataQuery: any,
  validateFn: any
) {
  let request = {
    ...dataQuery,
    nodeInfo: State.getNodeInfo(),
  }
  let result: any = { success: false, completed: [] }
  try {
    let response = (await P2P.postJson(
      `http://${consensorNode.ip}:${consensorNode.port}/querydata`,
      request
    )) as QueryDataResponse
    if (response && request.type === 'RECEIPT_MAP') {
      let receiptMapData = response.data as ReceiptMapQueryResponse['data']
      // for (let counter in response.data) {
      result = await validateAndStoreReceiptMaps(receiptMapData, validateFn)
      // }
    } else if (response && request.type === 'SUMMARY_BLOB') {
      // for (let counter in response.data) {
      let summaryBlobData = response.data as StatsClumpQueryResponse['data']
      result = await validateAndStoreSummaryBlobs(
        Object.values(summaryBlobData),
        validateFn
      )
      // }
    }
    return result
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      `Unable to query complete querying ${request.type} from node`,
      consensorNode
    )
    return result
  }
}

async function validateAndStoreReceiptMaps(
  receiptMapResultsForCycles: {
    [key: number]: StateManager.StateManagerTypes.ReceiptMapResult[]
  },
  validateFn: any
) {
  let completed: number[] = []
  let failed: number[] = []
  let coveredPartitions: number[] = []
  let receiptMaps: any = {}
  for (let counter in receiptMapResultsForCycles) {
    let receiptMapResults: StateManager.StateManagerTypes.ReceiptMapResult[] =
      receiptMapResultsForCycles[counter]
    for (let receiptMap of receiptMapResults) {
      let { cycle, partition } = receiptMap
      if (validateFn) {
        let shouldProcess = validateFn(cycle, partition)
        if (!shouldProcess) {
          continue
        }
      }
      coveredPartitions.push(partition)
      let reciptMapHash = await Storage.queryReceiptMapHash(
        parseInt(counter),
        partition
      )
      if (!reciptMapHash) {
        Logger.mainLogger.error(
          `Unable to find receipt hash for counter ${counter}, partition ${partition}`
        )
        continue
      }
      let calculatedReceiptMapHash = Crypto.hashObj(receiptMap)
      if (calculatedReceiptMapHash === reciptMapHash) {
        await Storage.updateReceiptMap(receiptMap)
        completed.push(partition)
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
  validateFn: any
) {
  let completed: number[] = []
  let failed: number[] = []
  let coveredPartitions: number[] = []
  let blobs: any = {}

  for (let statsClump of statsClumpForCycles) {
    let { cycle, dataStats, txStats, covered } = statsClump
    if (!covered) continue
    for (let partition of covered) {
      if (validateFn) {
        let shouldProcess = validateFn(cycle, partition)
        if (!shouldProcess) {
          continue
        }
      }
      coveredPartitions.push(partition)
      let summaryBlob
      let dataBlob = dataStats.find((d) => d.partition === partition)
      let txBlob = txStats.find((t) => t.partition === partition)
      let summaryHash = await Storage.querySummaryHash(cycle, partition)
      if (!summaryHash) {
        continue
      }
      let summaryObj = {
        dataStats: dataBlob ? dataBlob.opaqueBlob : {},
        txStats: txBlob ? txBlob.opaqueBlob : {},
      }
      let calculatedSummaryHash = Crypto.hashObj(summaryObj)
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
  async (newSenderInfo: NodeList.ConsensusNodeInfo, taggedDataRequest: any) => {
    //let request = {
    //...dataRequest,
    //nodeInfo: State.getNodeInfo()
    //}
    if (socketClients.has(newSenderInfo.publicKey)) {
      let response = await P2P.postJson(
        `http://${newSenderInfo.ip}:${newSenderInfo.port}/requestdata`,
        taggedDataRequest
      )
      Logger.mainLogger.debug('/requestdata response', response)
    } else {
    }
  }
)

emitter.on(
  'submitJoinRequest',
  async (newSenderInfo: NodeList.ConsensusNodeInfo, joinRequest: any) => {
    let request = {
      ...joinRequest,
      nodeInfo: State.getNodeInfo(),
    }
    let response = await P2P.postJson(
      `http://${newSenderInfo.ip}:${newSenderInfo.port}/joinarchiver`,
      request
    )
    Logger.mainLogger.debug('Join request response:', response)
  }
)

emitter.on(
  'submitLeaveRequest',
  async (consensorInfo: NodeList.ConsensusNodeInfo, leaveRequest: any) => {
    let request = {
      ...leaveRequest,
      nodeInfo: State.getNodeInfo(),
    }
    Logger.mainLogger.debug('Sending leave request to: ', consensorInfo.port)
    let response = await P2P.postJson(
      `http://${consensorInfo.ip}:${consensorInfo.port}/leavingarchivers`,
      request
    )
    Logger.mainLogger.debug('Leave request response:', response)
  }
)
