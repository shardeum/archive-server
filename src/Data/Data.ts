import { EventEmitter } from 'events'
import * as Crypto from '../Crypto'
import * as NodeList from '../NodeList'
import * as Cycles from './Cycles'
import {
  currentCycleCounter,
  currentCycleDuration,
  Cycle,
  lastProcessedMetaData,
  processCycles,
  validateCycle,
  fetchCycleRecords,
  getNewestCycleFromArchivers,
} from './Cycles'
import { ChangeSquasher, parse, totalNodeCount, activeNodeCount, applyNodeListChange } from './CycleParser'
import * as State from '../State'
import * as P2P from '../P2P'
import * as Utils from '../Utils'
import { isDeepStrictEqual } from 'util'
import { config } from '../Config'
import { P2P as P2PTypes } from '@shardus/types'
import * as Logger from '../Logger'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import { profilerInstance } from '../profiler/profiler'
import { storeReceiptData, storeCycleData, storeAccountData, storingAccountData } from './Collector'
import * as CycleDB from '../dbstore/cycles'
import * as ReceiptDB from '../dbstore/receipts'
import * as StateMetaData from '../archivedCycle/StateMetaData'

// Socket modules
export let socketServer: SocketIO.Server
let ioclient: SocketIOClientStatic = require('socket.io-client')
export let socketClients: Map<string, SocketIOClientStatic['Socket']> = new Map()
// let socketConnectionsTracker: Map<string, string> = new Map()
export let combineAccountsData = {
  accounts: [],
  receipts: [],
}
let forwardGenesisAccounts = true
let selectByConsensuRadius = true
let selectingNewDataSender = false
export let queueForSelectingNewDataSenders: Map<string, string> = new Map()
let receivedCycleTracker = {}

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
    if (sender.replaceTimeout) {
      clearTimeout(sender.replaceTimeout)
      sender.replaceTimeout = null
    }
    // Delete sender from dataSenders
    dataSenders.delete(publicKey)
    await sendDataRequest(sender.nodeInfo, DataRequestTypes.UNSUBSCRIBE)
  }
  const socketClient = socketClients.get(publicKey)
  if (socketClient) {
    socketClient.emit('UNSUBSCRIBE', config.ARCHIVER_PUBLIC_KEY)
    socketClient.disconnect()
    socketClients.delete(publicKey)
  }
  nestedCountersInstance.countEvent('archiver', 'remove_data_sender')
  Logger.mainLogger.debug('Subscribed dataSenders', socketClients.size, dataSenders.size)
  if (config.VERBOSE)
    Logger.mainLogger.debug('Subscribed dataSenders', socketClients.keys(), dataSenders.keys())
}

export function initSocketClient(node: NodeList.ConsensusNodeInfo) {
  if (config.VERBOSE) Logger.mainLogger.debug('Node Info to socket connect', node)
  const socketClient = ioclient.connect(`http://${node.ip}:${node.port}`)

  socketClient.on('connect', () => {
    Logger.mainLogger.debug(`Connection to consensus node ${node.ip}:${node.port} is made`)
    // Send ehlo event right after connect:
    socketClient.emit('ARCHIVER_PUBLIC_KEY', config.ARCHIVER_PUBLIC_KEY)
    socketClients.set(node.publicKey, socketClient)
    // socketConnectionsTracker.set(node.publicKey, 'connected')
    if (config.VERBOSE) Logger.mainLogger.debug('Connected node', node)
    if (config.VERBOSE) Logger.mainLogger.debug('Init socketClients', socketClients.size, dataSenders.size)
  })

  socketClient.once('disconnect', async () => {
    Logger.mainLogger.debug(`Connection request is refused by the consensor node ${node.ip}:${node.port}`)
    // socketConnectionsTracker.set(node.publicKey, 'disconnected')
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
        const sender = dataSenders.get(newData.publicKey)
        // If no sender entry, remove publicKey from senders, END
        if (!sender) {
          Logger.mainLogger.error('This sender is not in the subscribed nodes list')
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
          console.log('RECEIPT RECEIPT', sender.nodeInfo.publicKey, sender.nodeInfo.ip, sender.nodeInfo.port)

        if (newData.responses && newData.responses.RECEIPT) {
          if (config.VERBOSE)
            Logger.mainLogger.debug(
              'RECEIPT RECEIPT',
              sender.nodeInfo.publicKey,
              sender.nodeInfo.ip,
              sender.nodeInfo.port,
              newData.responses.RECEIPT.length
            )
          // clearFalseNodes(sender.nodeInfo.publicKey)
          storeReceiptData(newData.responses.RECEIPT, sender.nodeInfo.ip + ':' + sender.nodeInfo.port)
        }
        if (newData.responses && newData.responses.CYCLE) {
          for (const cycle of newData.responses.CYCLE) {
            // Logger.mainLogger.debug('Cycle received', cycle.counter)
            let cycleToSave = [] as Cycle[]
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
              // Logger.mainLogger.debug('Cycle received', cycle.counter, receivedCycleTracker)
              let maxEqual = 3 // Setting as 3 for now
              if (cycle.active < 10) maxEqual = 1
              for (let value of Object.values(receivedCycleTracker[cycle.counter])) {
                if (value['saved']) break
                if (value['receivedTimes'] >= maxEqual) {
                  cycleToSave.push(cycle)
                  value['saved'] = true
                }
              }
            } else {
              const byCycleMarker = {}
              byCycleMarker[cycle.marker] = {
                cycleInfo: cycle,
                receivedTimes: 1,
                saved: false,
              }
              receivedCycleTracker[cycle.counter] = byCycleMarker
              // Logger.mainLogger.debug('Cycle received', cycle.counter, receivedCycleTracker)
              let maxEqual = 3 // Setting as 3 for now
              if (cycle.active < 10) maxEqual = 1
              for (let value of Object.values(receivedCycleTracker[cycle.counter])) {
                if (value['saved']) break
                if (value['receivedTimes'] >= maxEqual) {
                  cycleToSave.push(cycle)
                  value['saved'] = true
                }
              }
            }
            if (cycleToSave.length > 0) {
              // Logger.mainLogger.debug('Cycle To Save', cycle.counter, receivedCycleTracker)
              processCycles(cycleToSave as Cycle[])
              storeCycleData(cycleToSave)
            }
          }
          if (Object.keys(receivedCycleTracker).length > 10) {
            for (const counter of Object.keys(receivedCycleTracker)) {
              if (parseInt(counter) < currentCycleCounter - 5) {
                let totalTimes = 0
                for (const key of Object.keys(receivedCycleTracker[counter])) {
                  totalTimes += receivedCycleTracker[counter][key]['receivedTimes']
                }
                Logger.mainLogger.debug(`Received ${totalTimes} times for cycle counter ${counter}`)
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

export const emitter = new EventEmitter()

export async function replaceDataSender(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  nestedCountersInstance.countEvent('archiver', 'replace_data_sender')
  if (NodeList.getActiveList().length < 2) {
    Logger.mainLogger.debug('There is only one active node in the network. Unable to replace data sender')
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
  Logger.mainLogger.debug(
    'queueForSelectingNewDataSenders',
    socketClients.has(publicKey),
    dataSenders.has(publicKey),
    selectingNewDataSender,
    queueForSelectingNewDataSenders.size
  )

  if (!socketClients.has(publicKey) || !dataSenders.has(publicKey)) {
    unsubscribeDataSender(publicKey)
    return
  } // Extend the contactTimeout a bit longer for now to make sure the archiver has already got a new replacer node
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
      5 * 60 * 1000
    )
  }
  if (selectingNewDataSender) {
    queueForSelectingNewDataSenders.set(publicKey, publicKey)
  } else {
    selectingNewDataSender = true
    await selectNewDataSendersByConsensusRadius([publicKey])
  }
}

export async function subscribeNodeForDataTransfer() {
  if (config.experimentalSnapshot) {
    if (selectByConsensuRadius) await subscribeMoreConsensorsByConsensusRadius()
    else await subscribeRandomNodeForDataTransfer()
  } else {
    await StateMetaData.subscribeRandomNodeForDataTransfer()
  }
}

export async function subscribeRandomNodeForDataTransfer() {
  let retry = 0
  let nodeSubscribedFail = true
  // Set randomly select a consensor as dataSender
  while (nodeSubscribedFail && retry < 10) {
    let randomConsensor = NodeList.getRandomActiveNodes()[0]
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

export function createReplaceTimeout(publicKey: NodeList.ConsensusNodeInfo['publicKey']) {
  const ms = config.DATASENDER_TIMEOUT || 1000 * 60 * 5
  return setTimeout(() => {
    nestedCountersInstance.countEvent('archiver', 'replace_timeout')
    Logger.mainLogger.debug('ROTATING sender due to ROTATION timeout')
    replaceDataSender(publicKey)
  }, ms)
}

export function addDataSender(sender: DataSender) {
  dataSenders.set(sender.nodeInfo.publicKey, sender)
}

async function selectNewDataSendersByConsensusRadius(publicKeys: NodeList.ConsensusNodeInfo['publicKey'][]) {
  if (config.VERBOSE) Logger.mainLogger.debug('selectNewDataSendersByConsensusRadius publicKeys', publicKeys)
  if (publicKeys.length === 0) {
    selectingNewDataSender = false
    return
  }
  const calculatedConsensusRadius = await getConsensusRadius()
  let consensusRadius = calculatedConsensusRadius
  if (consensusRadius > 2) consensusRadius-- // Change default to 3 for now assuming nodesPerConsensusGroup 10
  let activeList = [...NodeList.activeListByIdSorted]
  if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
  let totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
  Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  for (const publicKey of publicKeys) {
    let nodeIsUnsubscribed = false
    let nodeIsInTheActiveList = false
    activeList = [...NodeList.activeListByIdSorted]
    if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
    totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
    Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
    for (let i = 0; i < activeList.length; i += consensusRadius) {
      const subsetList = activeList.slice(i, i + consensusRadius)
      if (config.VERBOSE)
        Logger.mainLogger.debug('Round', i, publicKey, 'subsetList', subsetList, dataSenders.keys())
      let nodeToRotateIsFromThisSubset = false
      let noNodeFromThisSubset = true
      let extraSubscribedNodesCountFromThisSubset = 0
      for (let node of Object.values(subsetList)) {
        if (dataSenders.has(node.publicKey)) {
          if (config.VERBOSE) Logger.mainLogger.debug('This node from the subset is in the subscribed list!')
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
        continue
      }
      if (extraSubscribedNodesCountFromThisSubset >= 1) {
        Logger.mainLogger.debug(
          `There are already ${extraSubscribedNodesCountFromThisSubset} nodes that the archiver has picked from this nodes subset.`
        )
        if (config.VERBOSE)
          console.log(
            `There are already ${extraSubscribedNodesCountFromThisSubset} nodes that the archiver has picked from this nodes subset.`
          )
        if (!nodeIsUnsubscribed && nodeToRotateIsFromThisSubset) {
          unsubscribeDataSender(publicKey)
          nodeIsUnsubscribed = true
        }
        continue
      }
      let newSubsetList = subsetList.filter((node) => node.publicKey !== publicKey)
      if (newSubsetList.length === 0) {
        if (subsetList[0].publicKey !== publicKey) {
          // This isn't supposed to happen and just to log if it happens
          Logger.mainLogger.error(`The node publicKey ${publicKey} is in the subset list!`)
        }
        continue
      }
      // Pick a new dataSender
      let newSenderInfo = newSubsetList[Math.floor(Math.random() * newSubsetList.length)]
      let connectionStatus = false
      let retry = 0
      while (true && retry < consensusRadius) {
        if (!dataSenders.has(newSenderInfo.publicKey) && publicKey !== newSenderInfo.publicKey) {
          connectionStatus = await createDataTransferConnection(newSenderInfo)
          if (connectionStatus) {
            if (!nodeIsUnsubscribed && nodeToRotateIsFromThisSubset) {
              unsubscribeDataSender(publicKey)
              nodeIsUnsubscribed = true
            }
            // if (noNodeFromThisSubset) await Utils.sleep(30000) // Start another node with 30s difference
            break
          } else {
            newSubsetList = newSubsetList.filter((node) => node.publicKey !== newSenderInfo.publicKey)
          }
        }
        if (newSubsetList.length > 0) {
          newSenderInfo = newSubsetList[Math.floor(Math.random() * newSubsetList.length)]
        } else {
          newSenderInfo = activeList[Math.floor(Math.random() * activeList.length)]
        }
        retry++
      }
    }
    if (!nodeIsUnsubscribed) {
      Logger.mainLogger.debug(`This publicKey is not in the active list!`)
      unsubscribeDataSender(publicKey)
      nodeIsUnsubscribed = true
    }
  }
  // Temp hack to pick half of the nodes not to miss data at all
  if (calculatedConsensusRadius === 2) {
    activeList = [...NodeList.activeListByIdSorted]
    if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
    totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
    Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
    let extraConsensorsToSubscribe = Math.floor((activeList.length / 4) * 3)
    if (dataSenders.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= dataSenders.size
      Logger.mainLogger.debug('extraConsensorsToSubscribe', extraConsensorsToSubscribe)
      await subscribeExtraConsensors(extraConsensorsToSubscribe)
    }
  }
  // Temp hack to pick more nodes when consensusRadius is high
  // Pick one node out of 10 nodes
  if (Math.floor(activeList.length / 5) > totalNumberOfNodesToSubscribe) {
    let extraConsensorsToSubscribe = Math.floor(activeList.length / 5)
    if (dataSenders.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= dataSenders.size
      Logger.mainLogger.debug('extraConsensorsToSubscribe', extraConsensorsToSubscribe)
      await subscribeExtraConsensors(extraConsensorsToSubscribe)
    }
  }
  if (queueForSelectingNewDataSenders.size > 0) {
    if (config.VERBOSE)
      Logger.mainLogger.debug('queueForSelectingNewDataSenders', queueForSelectingNewDataSenders)
    let newPublicKeys = []
    for (let [key, value] of queueForSelectingNewDataSenders) {
      newPublicKeys.push(key)
    }
    for (let key of newPublicKeys) {
      queueForSelectingNewDataSenders.delete(key)
    }
    if (config.VERBOSE) Logger.mainLogger.debug(newPublicKeys)
    if (newPublicKeys.length > 0) await selectNewDataSendersByConsensusRadius(newPublicKeys)
  } else {
    selectingNewDataSender = false
  }
}

async function getConsensusRadius() {
  const activeList = NodeList.getActiveList()
  let randomNode = activeList[Math.floor(Math.random() * activeList.length)]
  Logger.mainLogger.debug(`Checking network configs from random node ${randomNode.ip}:${randomNode.port}`)
  let response: any = await P2P.getJson(`http://${randomNode.ip}:${randomNode.port}/netconfig`)
  if (response && response.config) {
    const nodesPerConsensusGroup = response.config.sharding.nodesPerConsensusGroup
    const consensusRadius = Math.floor((nodesPerConsensusGroup - 1) / 2)
    Logger.mainLogger.debug('consensusRadius', consensusRadius)
    if (config.VERBOSE) console.log('consensusRadius', consensusRadius)
    return consensusRadius
  }
  return activeList.length
}

async function verifyNode(newSenderInfo: NodeList.ConsensusNodeInfo) {
  let status = false
  Logger.mainLogger.debug(`Checking node info ${newSenderInfo.ip}:${newSenderInfo.port}`)
  let response: any = await P2P.getJson(`http://${newSenderInfo.ip}:${newSenderInfo.port}/nodeInfo`)
  if (response && response.nodeInfo) {
    const nodeInfo = response.nodeInfo
    if (
      nodeInfo.id === newSenderInfo.id &&
      nodeInfo.publicKey === newSenderInfo.publicKey &&
      nodeInfo.status === NodeList.Statuses.ACTIVE
    )
      return (status = true)
  }
  Logger.mainLogger.error(`Node ${newSenderInfo.ip}:${newSenderInfo.port} status is not valid!`)
  return status
}

export async function createDataTransferConnection(newSenderInfo: NodeList.ConsensusNodeInfo) {
  // Verify node before subscribing for data transfer
  const status = await verifyNode(newSenderInfo)
  if (!status) return false
  // Subscribe this node for dataRequest
  const response = await sendDataRequest(newSenderInfo, DataRequestTypes.SUBSCRIBE)
  if (response) {
    // Add new dataSender to dataSenders
    const newSender: DataSender = {
      nodeInfo: newSenderInfo,
      types: [P2PTypes.SnapshotTypes.TypeNames.CYCLE, P2PTypes.SnapshotTypes.TypeNames.STATE_METADATA],
      contactTimeout: createContactTimeout(
        newSenderInfo.publicKey,
        'This timeout is created during newSender selection'
      ),
      replaceTimeout: createReplaceTimeout(newSenderInfo.publicKey),
    }
    addDataSender(newSender)
    Logger.mainLogger.debug(`added new sender ${newSenderInfo.publicKey} to dataSenders`)
    initSocketClient(newSenderInfo)
    // let count = 0
    // while (!socketClients.has(newSenderInfo.publicKey) && count <= 50) {
    //   await Utils.sleep(100)
    //   count++
    //   if (count === 50) {
    //     // This means the socket connection to the node is not successful.
    //     return false
    //   }
    // }
  }
  return response
}

export async function subscribeMoreConsensorsByConsensusRadius() {
  if (selectingNewDataSender) return
  const calculatedConsensusRadius = await getConsensusRadius()
  let consensusRadius = calculatedConsensusRadius
  if (consensusRadius > 2) consensusRadius-- // Change default to 3 for now assuming nodesPerConsensusGroup 10
  const activeList = [...NodeList.activeListByIdSorted]
  if (config.VERBOSE) console.log('activeList', activeList.length, activeList)
  const totalNumberOfNodesToSubscribe = Math.ceil(activeList.length / consensusRadius)
  Logger.mainLogger.debug('totalNumberOfNodesToSubscribe', totalNumberOfNodesToSubscribe)
  for (let i = 0; i < activeList.length; i += consensusRadius) {
    let subsetList = activeList.slice(i, i + consensusRadius)
    if (config.VERBOSE) Logger.mainLogger.debug('Round', i, 'subsetList', subsetList, dataSenders.keys())
    let noNodeFromThisSubset = true
    for (let node of Object.values(subsetList)) {
      if (dataSenders.has(node.publicKey)) {
        if (config.VERBOSE) Logger.mainLogger.debug('The node is found in this subset')
        noNodeFromThisSubset = false
      }
    }

    if (!noNodeFromThisSubset) {
      Logger.mainLogger.debug(
        'There is already node from this subset or node to rotate is not from this subset!'
      )
      continue
    }
    // Pick a new dataSender
    let newSenderInfo = subsetList[Math.floor(Math.random() * subsetList.length)]
    let connectionStatus = false
    let retry = 0
    while (true && retry < consensusRadius) {
      if (!dataSenders.has(newSenderInfo.publicKey)) {
        connectionStatus = await createDataTransferConnection(newSenderInfo)
        if (connectionStatus) {
          break
        } else {
          subsetList = subsetList.filter((node) => node.publicKey !== newSenderInfo.publicKey)
        }
      }
      if (subsetList.length > 0) {
        newSenderInfo = subsetList[Math.floor(Math.random() * subsetList.length)]
      } else {
        break
      }
      retry++
    }
  }
  // Temp hack to pick half of the nodes not to miss data at all
  if (calculatedConsensusRadius === 2) {
    let extraConsensorsToSubscribe = Math.floor((activeList.length / 4) * 3)
    if (dataSenders.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= dataSenders.size
      Logger.mainLogger.debug('extraConsensorsToSubscribe', extraConsensorsToSubscribe)
      await subscribeExtraConsensors(extraConsensorsToSubscribe)
    }
  }
  // Temp hack to pick more nodes when consensusRadius is high
  // Pick one node out of 10 nodes
  if (Math.floor(activeList.length / 5) > totalNumberOfNodesToSubscribe) {
    let extraConsensorsToSubscribe = Math.floor(activeList.length / 5)
    if (dataSenders.size < extraConsensorsToSubscribe) {
      extraConsensorsToSubscribe -= dataSenders.size
      Logger.mainLogger.debug('extraConsensorsToSubscribe', extraConsensorsToSubscribe)
      await subscribeExtraConsensors(extraConsensorsToSubscribe)
    }
  }
  Logger.mainLogger.debug('Subscribed dataSenders', socketClients.size, dataSenders.size)
}

export async function subscribeExtraConsensors(extraConsensorsToSubscribe) {
  const retryTimes = 2
  let subscribedSuccess = 0
  let retry = 0
  const activeList = [...NodeList.activeListByIdSorted]

  let remainingActiveList = [...activeList]
  if (subscribedSuccess < extraConsensorsToSubscribe) {
    for (const key of dataSenders.keys()) {
      remainingActiveList = remainingActiveList.filter((node) => node.publicKey !== key)
    }
    if (config.VERBOSE)
      Logger.mainLogger.debug('remainingActiveList', remainingActiveList.length, dataSenders.keys())
  }

  while (subscribedSuccess < extraConsensorsToSubscribe) {
    if (retry === retryTimes) {
      break
    }
    if (remainingActiveList.length === 0) {
      remainingActiveList = [...activeList]
      if (subscribedSuccess < extraConsensorsToSubscribe) {
        for (const key of dataSenders.keys()) {
          remainingActiveList = remainingActiveList.filter((node) => node.publicKey !== key)
        }
      }
      if (remainingActiveList.length === 0) {
        break
      }
      retry++
    }
    let newSenderInfo = remainingActiveList[Math.floor(Math.random() * remainingActiveList.length)]
    // Logger.mainLogger.debug('newSenderInfo', newSenderInfo, remainingActiveList)
    if (!dataSenders.has(newSenderInfo.publicKey)) {
      let connectionStatus = await createDataTransferConnection(newSenderInfo)
      if (connectionStatus) {
        subscribedSuccess++
      }
    }
    remainingActiveList = remainingActiveList.filter((node) => node.publicKey !== newSenderInfo.publicKey)
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
  Logger.mainLogger.info('Sending tagged data request to consensor.', nodeInfo.ip + ':' + nodeInfo.port)
  let reply = false
  let response = await P2P.postJson(`http://${nodeInfo.ip}:${nodeInfo.port}/requestdata`, taggedDataRequest)
  Logger.mainLogger.debug('/requestdata response', response)
  if (response && response.success) reply = response.success
  return reply
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
  let request = P2P.createArchiverJoinRequest()
  let shuffledNodes = [...nodeList]
  Utils.shuffleArray(shuffledNodes)

  // Wait until a Q1 then send join request to active nodes
  let untilQ1 = startQ1 - Date.now()
  while (untilQ1 < 0) {
    untilQ1 += latestCycle.duration * 1000
  }

  Logger.mainLogger.debug(`Waiting ${untilQ1 + 500} ms for Q1 before sending join...`)
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
  Logger.mainLogger.debug(`Sending join request to ${selectedNodes.map((n) => `${n.ip}:${n.port}`)}`)
  for (const node of selectedNodes) {
    let response = await P2P.postJson(`http://${node.ip}:${node.port}/joinarchiver`, joinRequest)
    Logger.mainLogger.debug('Join request response:', response)
  }
}

export async function sendLeaveRequest(nodeInfo: NodeList.ConsensusNodeInfo) {
  let leaveRequest = P2P.createArchiverLeaveRequest()
  Logger.mainLogger.debug('Sending leave request to: ', nodeInfo.port)
  let response = await P2P.postJson(`http://${nodeInfo.ip}:${nodeInfo.port}/leavingarchivers`, leaveRequest)
  Logger.mainLogger.debug('Leave request response:', response)
  return true
}

export async function getCycleDuration() {
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)[0]
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/cycleinfo/1`)
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
    const response: any = await P2P.getJson(`http://${node.ip}:${node.port}/sync-newest-cycle`)
    if (response.newestCycle) return response.newestCycle
  }
  let newestCycle: any = await Utils.robustQuery(activeNodes, queryFn, isSameCyceInfo)
  return newestCycle[0]
}

export function checkJoinStatus(): Promise<boolean> {
  Logger.mainLogger.debug('Checking join status')
  const ourNodeInfo = State.getNodeInfo()
  const randomArchiver = Utils.getRandomItemFromArr(State.activeArchivers)[0]

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

export async function syncGenesisAccountsFromArchiver(activeArchivers: State.ArchiverNodeInfo[]) {
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
    Logger.mainLogger.error('Genesis Total Accounts Query', 'Invalid download response')
    return
  }
  if (totalGenesisAccounts <= 0) return
  let page = 0
  while (!complete) {
    Logger.mainLogger.debug(`Downloading accounts from ${startAccount} to ${endAccount}`)
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/account?startCycle=0&endCycle=5&page=${page}`
    )
    if (response && response.accounts) {
      if (response.accounts.length < 10000) {
        complete = true
        Logger.mainLogger.debug('Download completed for accounts')
      }
      Logger.mainLogger.debug(`Downloaded accounts`, response.accounts.length)
      await storeAccountData({ accounts: response.accounts })
    } else {
      Logger.mainLogger.debug('Genesis Accounts Query', 'Invalid download response')
    }
    startAccount = endAccount
    endAccount += 10000
    page++
    // await sleep(1000);
  }
  Logger.mainLogger.debug('Sync genesis accounts completed!')
}

export async function syncGenesisTransactionsFromArchiver(activeArchivers: State.ArchiverNodeInfo[]) {
  const [randomArchiver] = Utils.getRandomItemFromArr(activeArchivers)
  let complete = false
  let startTransaction = 0
  let endTransaction = startTransaction + 10000
  let totalGenesisTransactions = 0

  let res: any = await P2P.getJson(
    `http://${randomArchiver.ip}:${randomArchiver.port}/transaction?startCycle=0&endCycle=5`
  )
  if (res && res.totalTransactions) {
    totalGenesisTransactions = res.totalTransactions
    Logger.mainLogger.debug('TotalGenesis Transactions', totalGenesisTransactions)
  } else {
    Logger.mainLogger.error('Genesis Total Transaction Query', 'Invalid download response')
    return
  }
  if (totalGenesisTransactions <= 0) return
  let page = 0
  while (!complete) {
    Logger.mainLogger.debug(`Downloading transactions from ${startTransaction} to ${endTransaction}`)
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/transaction?startCycle=0&endCycle=5&page=${page}`
    )
    if (response && response.transactions) {
      if (response.transactions.length < 10000) {
        complete = true
        Logger.mainLogger.debug('Download completed for transactions')
      }
      Logger.mainLogger.debug(`Downloaded transactions`, response.transactions.length)
      await storeAccountData({ receipts: response.transactions })
    } else {
      Logger.mainLogger.debug('Genesis Transactions Query', 'Invalid download response')
    }
    startTransaction = endTransaction
    endTransaction += 10000
    page++
    // await sleep(1000);
  }
  Logger.mainLogger.debug('Sync genesis transactions completed!')
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
      // TODO - update to include receipts data also
      await storeAccountData({ accounts: response.accounts })
      // combineAccountsData = [...combineAccountsData, ...response.accounts];
      totalDownloadedAccounts += response.accounts.length
    } else {
      Logger.mainLogger.debug('Genesis Accounts Query', 'Invalid download response')
    }
    startAccount += 1000
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
    let nextEnd: number = endCycle - 10000 // Downloading max 1000 cycles each time
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

export async function syncReceipts(
  activeArchivers: State.ArchiverNodeInfo[],
  lastStoredReceiptCount: number = 0
) {
  const randomArchiver = Utils.getRandomItemFromArr(activeArchivers)[0]
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
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
  let end = start + 1000
  while (!complete) {
    if (end >= to) {
      let res: any = await P2P.getJson(`http://${archiver.ip}:${archiver.port}/totalData`)
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
        let res: any = await P2P.getJson(`http://${archiver.ip}:${archiver.port}/totalData`)
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
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
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
      let res: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
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
    Logger.mainLogger.debug(`Downloading receipts from cycle ${startCycle} to cycle ${endCycle}`)
    let response: any = await P2P.getJson(
      `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`
    )
    if (response && response.receipts > 0) {
      receiptsCountToSyncBetweenCycles = response.receipts
      let page = 1
      savedReceiptsCountBetweenCycles = 0
      while (savedReceiptsCountBetweenCycles < receiptsCountToSyncBetweenCycles) {
        response = await P2P.getJson(
          `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&page=${page}`
        )
        if (response && response.receipts) {
          const downloadedReceipts = response.receipts
          Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
          await storeReceiptData(downloadedReceipts)
          savedReceiptsCountBetweenCycles += downloadedReceipts.length
          if (savedReceiptsCountBetweenCycles > receiptsCountToSyncBetweenCycles) {
            response = await P2P.getJson(
              `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=count`
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
            Logger.mainLogger.debug('There are more cycles than it supposed to have')
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
  let response: any = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
  if (!response || response.totalCycles < 0 || response.totalReceipts < 0) {
    return false
  }
  const { totalCycles, totalReceipts } = response
  Logger.mainLogger.debug('totalCycles', totalCycles, 'lastStoredCycleCount', lastStoredCycleCount)
  Logger.mainLogger.debug('totalReceipts', totalReceipts, 'lastStoredReceiptCount', lastStoredReceiptCount)
  if (totalCycles === lastStoredCycleCount && totalReceipts === lastStoredReceiptCount) {
    Logger.mainLogger.debug('The archiver has synced the lastest cycle and receipts data!')
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

  if (totalCycles === lastStoredCycleCount) completeForCycle = true
  if (totalReceipts === lastStoredReceiptCount) completeForReceipt = true

  while (!completeForReceipt || !completeForCycle) {
    if (endReceipt >= totalReceiptsToSync || endCycle >= totalCyclesToSync) {
      response = await P2P.getJson(`http://${randomArchiver.ip}:${randomArchiver.port}/totalData`)
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
      Logger.mainLogger.debug(`Downloading receipts from ${startReceipt} to ${endReceipt}`)
      const res: any = await P2P.getJson(
        `http://${randomArchiver.ip}:${randomArchiver.port}/receipt?start=${startReceipt}&end=${endReceipt}`
      )
      if (res && res.receipts) {
        const downloadedReceipts = res.receipts
        Logger.mainLogger.debug(`Downloaded receipts`, downloadedReceipts.length)
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
      Logger.mainLogger.debug(`Downloading cycles from ${startCycle} to ${endCycle}`)
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
    `http://${archiver.ip}:${archiver.port}/cycleinfo?start=${lastCycleCounter - 10}&end=${lastCycleCounter - 1
    }`
  )
  if (response && response.cycleInfo) {
    downloadedCycles = response.cycleInfo
  } else {
    throw Error(
      `Can't fetch data from cycle ${lastCycleCounter - 10} to cycle ${lastCycleCounter - 1
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
