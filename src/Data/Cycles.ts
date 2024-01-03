import * as NodeList from '../NodeList'
import * as Crypto from '../Crypto'
import * as State from '../State'
import * as Logger from '../Logger'
import { P2P } from '@shardus/types'
import { getJson } from '../P2P'
import { profilerInstance } from '../profiler/profiler'
import { nestedCountersInstance } from '../profiler/nestedCounters'
import {
  clearDataSenders,
  dataSenders,
  subscribeConsensorsByConsensusRadius,
  unsubscribeDataSender,
} from './Data'
import * as Utils from '../Utils'
import { isDeepStrictEqual } from 'util'
import { config } from '../Config'
import fetch from 'node-fetch'
import { getAdjacentLeftAndRightArchivers, sendDataToAdjacentArchivers, DataType } from './GossipData'
import { storeCycleData } from './Collector'
import { clearServingValidatorsInterval, initServingValidatorsInterval } from './AccountDataProvider'
import { hexstring } from 'shardus-crypto-types'
import { handleLostArchivers } from '../LostArchivers'

export interface Cycle extends P2P.CycleCreatorTypes.CycleRecord {
  certificate: string
  marker: string
  currentTime?: number
}

export interface LostNode {
  counter: Cycle['counter']
  timestamp: number
  nodeInfo: NodeList.ConsensusNodeInfo
}

interface ArchiverCycleResponse {
  cycleInfo: Cycle[]
}

interface ConsensorCycleResponse {
  newestCycle: Cycle
}

export let currentCycleDuration = 0
let currentCycleCounter = -1
let currentCycleMarker = '0'.repeat(32)
export let lastProcessedMetaData = -1
export const CycleChain: Map<Cycle['counter'], Cycle> = new Map()
export const lostNodes: LostNode[] = []
export const removedNodes = []
export let cycleRecordWithShutDownMode = null as P2P.CycleCreatorTypes.CycleRecord | null
export let currentNetworkMode: P2P.ModesTypes.Record['mode'] = 'forming'

export async function processCycles(cycles: Cycle[]): Promise<void> {
  if (profilerInstance) profilerInstance.profileSectionStart('process_cycle', false)
  try {
    if (nestedCountersInstance) nestedCountersInstance.countEvent('cycle', 'process', 1)
    for (const cycle of cycles) {
      // Logger.mainLogger.debug('Current cycle counter', currentCycleCounter)
      // Skip if already processed [TODO] make this check more secure
      if (cycle.counter <= currentCycleCounter) continue
      Logger.mainLogger.debug(new Date(), 'New Cycle received', cycle.counter)

      // Update currentCycle state
      currentCycleDuration = cycle.duration * 1000
      currentCycleCounter = cycle.counter

      // Update NodeList from cycle info
      updateNodeList(cycle)
      changeNetworkMode(cycle.mode)
      handleLostArchivers(cycle)

      await storeCycleData([cycle])
      getAdjacentLeftAndRightArchivers()

      Logger.mainLogger.debug(`Processed cycle ${cycle.counter}`)

      sendDataToAdjacentArchivers(DataType.CYCLE, [cycle])
      // Check the archivers reputaion in every new cycle & record the status
      recordArchiversReputation()
      if (currentNetworkMode === 'shutdown') {
        Logger.mainLogger.debug(Date.now(), `❌ Shutdown Cycle Record received at Cycle #: ${cycle.counter}`)
        await Utils.sleep(currentCycleDuration)
        NodeList.clearNodeListCache()
        await clearDataSenders()
        setShutdownCycleRecord(cycle)
        NodeList.toggleFirstNode()
      }
    }
  } finally {
    if (profilerInstance) profilerInstance.profileSectionEnd('process_cycle', false)
  }
}

export function getCurrentCycleCounter(): number {
  return currentCycleCounter
}

export function getCurrentCycleMarker(): hexstring {
  return currentCycleMarker
}

export function getLostNodes(from: number, to: number): LostNode[] {
  return lostNodes.filter((node: LostNode) => {
    return node.counter >= from && node.counter <= to
  })
}

export function setCurrentCycleDuration(duration: number): void {
  currentCycleDuration = duration * 1000
}

export function setCurrentCycleCounter(value: number): void {
  currentCycleCounter = value
}

export function setCurrentCycleMarker(value: hexstring): void {
  currentCycleMarker = value
}

export function setLastProcessedMetaDataCounter(value: number): void {
  lastProcessedMetaData = value
}

export function changeNetworkMode(newMode: P2P.ModesTypes.Record['mode']): void {
  if (newMode === currentNetworkMode) return
  // If the network mode is changed from restore to processing, clear the serving validators interval
  if (currentNetworkMode === 'restore' && newMode === 'processing') clearServingValidatorsInterval()
  if ((currentNetworkMode === 'restart' || currentNetworkMode === 'recovery') && newMode === 'restore') {
    NodeList.changeNodeListInRestore()
    initServingValidatorsInterval()
  }
  if (cycleRecordWithShutDownMode && newMode !== 'shutdown') {
    cycleRecordWithShutDownMode = null
  }
  currentNetworkMode = newMode
}

export function computeCycleMarker(fields: Cycle): string {
  const cycleMarker = Crypto.hashObj(fields)
  return cycleMarker
}

// validation of cycle record against previous marker
export function validateCycle(prev: Cycle, next: P2P.CycleCreatorTypes.CycleRecord): boolean {
  const previousRecordWithoutMarker: Cycle = { ...prev }
  delete previousRecordWithoutMarker.marker
  const prevMarker = computeCycleMarker(previousRecordWithoutMarker)
  return next.previous === prevMarker
}

export function setShutdownCycleRecord(cycleRecord: P2P.CycleCreatorTypes.CycleRecord): void {
  cycleRecordWithShutDownMode = cycleRecord
}

interface P2PNode {
  publicKey: string
  externalIp: string
  externalPort: number
  internalIp: string
  internalPort: number
  address: string
  joinRequestTimestamp: number
  activeTimestamp: number
}

export interface JoinedConsensor extends P2PNode {
  id: string
  cycleJoined: string
}

function updateNodeList(cycle: Cycle): void {
  const {
    // lost,  (not used)
    joinedConsensors,
    activatedPublicKeys,
    removed,
    apoptosized,
    joinedArchivers,
    leavingArchivers,
    refreshedConsensors,
    refreshedArchivers,
    standbyAdd,
    standbyRemove,
  } = cycle

  const consensorInfos = joinedConsensors.map((jc) => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
    id: jc.id,
  }))

  const refreshedConsensorInfos = refreshedConsensors.map((jc) => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
    id: jc.id,
  }))

  NodeList.addNodes(NodeList.NodeStatus.SYNCING, cycle.marker, consensorInfos)

  NodeList.setStatus(NodeList.NodeStatus.ACTIVE, activatedPublicKeys)

  NodeList.refreshNodes(NodeList.NodeStatus.ACTIVE, cycle.marker, refreshedConsensorInfos)

  if (standbyAdd.length > 0) {
    const standbyNodeList: NodeList.ConsensusNodeInfo[] = standbyAdd.map((joinRequest) => ({
      publicKey: joinRequest.nodeInfo.publicKey,
      ip: joinRequest.nodeInfo.externalIp,
      port: joinRequest.nodeInfo.externalPort,
    }))
    NodeList.addNodes(NodeList.NodeStatus.STANDBY, cycle.marker, standbyNodeList)
  }

  if (standbyRemove.length > 0) {
    NodeList.removeStandbyNodes(standbyRemove)
  }

  const removedAndApopedNodes: NodeList.ConsensusNodeInfo[] = []

  const removedPks = removed.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      removedAndApopedNodes.push(nodeInfo)
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(removedPks)

  // TODO: add a more scalable lostNodes collector (maybe removed nodes collector)
  // add lost nodes to lostNodes collector
  // lost.forEach((id: string) => {
  //   const nodeInfo = NodeList.getNodeInfoById(id)
  //   lostNodes.push({
  //     counter: cycle.counter,
  //     timestamp: Date.now(),
  //     nodeInfo,
  //   })
  // })

  // The archiver doesn't need to consider lost nodes; They will be in `apop` or `refuted` list in next cycle
  // const lostPks = lost.reduce((keys: string[], id) => {
  //   const nodeInfo = NodeList.getNodeInfoById(id)
  //   if (nodeInfo) {
  //     keys.push(nodeInfo.publicKey)
  //   }
  //   return keys
  // }, [])
  // NodeList.removeNodes(lostPks)

  const apoptosizedPks = apoptosized.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      removedAndApopedNodes.push(nodeInfo)
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(apoptosizedPks)

  for (const joinedArchiver of joinedArchivers) {
    const foundArchiver = State.activeArchivers.find((a) => a.publicKey === joinedArchiver.publicKey)
    if (!foundArchiver) {
      State.activeArchivers.push(joinedArchiver)
      Utils.insertSorted(
        State.activeArchiversByPublicKeySorted,
        joinedArchiver,
        NodeList.byAscendingPublicKey
      )
      Logger.mainLogger.debug(
        'activeArchiversByPublicKeySorted',
        State.activeArchiversByPublicKeySorted.map((archiver) => archiver.publicKey)
      )
      Logger.mainLogger.debug('New archiver added to active list', joinedArchiver)
    }
    Logger.mainLogger.debug('active archiver list', State.activeArchivers)
  }

  for (const refreshedArchiver of refreshedArchivers) {
    const foundArchiver = State.activeArchivers.find((a) => a.publicKey === refreshedArchiver.publicKey)
    if (!foundArchiver) {
      State.activeArchivers.push(refreshedArchiver)
      Utils.insertSorted(
        State.activeArchiversByPublicKeySorted,
        refreshedArchiver,
        NodeList.byAscendingPublicKey
      )
      Logger.mainLogger.debug(
        'activeArchiversByPublicKeySorted',
        State.activeArchiversByPublicKeySorted.map((archiver) => archiver.publicKey)
      )
      Logger.mainLogger.debug('Refreshed archiver added to active list', refreshedArchiver)
    }
  }

  for (const leavingArchiver of leavingArchivers) {
    State.removeActiveArchiver(leavingArchiver.publicKey)
    State.archiversReputation.delete(leavingArchiver.publicKey)
  }

  const nodesToUnsubscribed = [...apoptosizedPks, ...removedPks]
  if (nodesToUnsubscribed.length > 0) {
    for (const key of nodesToUnsubscribed) {
      if (dataSenders.has(key)) unsubscribeDataSender(key)
    }
  }
  if (removedAndApopedNodes.length > 0) {
    removedNodes.push({ cycle: cycle.counter, nodes: removedAndApopedNodes })
    while (removedNodes.length > 10) {
      removedNodes.shift()
    }
  }
  // To pick nodes only when the archiver is active
  if (State.isActive) {
    subscribeConsensorsByConsensusRadius()
  }
}

function isSameCycleInfo(info1: Cycle, info2: Cycle): boolean {
  const cm1 = Utils.deepCopy(info1)
  const cm2 = Utils.deepCopy(info2)
  delete cm1.currentTime
  delete cm2.currentTime
  const equivalent = isDeepStrictEqual(cm1, cm2)
  return equivalent
}

export async function fetchCycleRecords(
  activeArchivers: State.ArchiverNodeInfo[],
  start: number,
  end: number
): Promise<Cycle[]> {
  const queryFn = async (archiver: State.ArchiverNodeInfo): Promise<Cycle[]> => {
    const response = (await getJson(
      `http://${archiver.ip}:${archiver.port}/cycleinfo?start=${start}&end=${end}`,
      20
    )) as ArchiverCycleResponse
    return response.cycleInfo
  }
  const { result } = await Utils.sequentialQuery(activeArchivers, queryFn)
  return result as Cycle[]
}

export async function getNewestCycleFromConsensors(
  activeNodes: NodeList.ConsensusNodeInfo[]
): Promise<Cycle> {
  const queryFn = async (node: NodeList.ConsensusNodeInfo): Promise<Cycle> => {
    const response = (await getJson(
      `http://${node.ip}:${node.port}/sync-newest-cycle`
    )) as ConsensorCycleResponse

    if (response.newestCycle) {
      return response.newestCycle as Cycle
    }

    return null
  }
  const newestCycle = await Utils.robustQuery(activeNodes, queryFn, isSameCycleInfo)
  return newestCycle.value
}

export async function getNewestCycleFromArchivers(activeArchivers: State.ArchiverNodeInfo[]): Promise<Cycle> {
  activeArchivers = activeArchivers.filter((archiver) => archiver.publicKey !== State.getNodeInfo().publicKey)

  const queryFn = async (archiver: State.ArchiverNodeInfo): Promise<Cycle> => {
    const response = (await getJson(
      `http://${archiver.ip}:${archiver.port}/cycleinfo/1`
    )) as ArchiverCycleResponse
    return response.cycleInfo[0]
  }
  const cycleInfo = await Utils.robustQuery(activeArchivers, queryFn, isSameCycleInfo)
  return cycleInfo.value[0]
}

export async function recordArchiversReputation(): Promise<void> {
  const activeArchivers = [...State.activeArchivers]

  const promises = activeArchivers.map((archiver) =>
    fetch(`http://${archiver.ip}:${archiver.port}/cycleinfo/1`, {
      method: 'get',
      headers: { 'Content-Type': 'application/json' },
      timeout: 5000,
    }).then((res) => res.json())
  )

  Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      for (const response of responses) {
        // eslint-disable-next-line security/detect-object-injection
        const archiver = activeArchivers[i]
        if (response.status === 'fulfilled') {
          const res = response.value
          if (res && res.cycleInfo && res.cycleInfo.length > 0) {
            const cycleRecord = res.cycleInfo[0]
            // Set the archiver's reputation to 'up' if it is still 10 cycles behind or has higher than our current cycle
            if (cycleRecord.counter - currentCycleCounter >= -10) {
              State.archiversReputation.set(archiver.publicKey, 'up')
            } else {
              Logger.mainLogger.debug(
                `Archiver  ${archiver.ip}:${archiver.port} has fallen behind the latest cycle`
              )
              State.archiversReputation.set(archiver.publicKey, 'down')
            }
          } else {
            Logger.mainLogger.debug(`Archiver is not responding correctly ${archiver.ip}:${archiver.port}`)
            State.archiversReputation.set(archiver.publicKey, 'down')
          }
        } else {
          Logger.mainLogger.debug(`Archiver is not responding ${archiver.ip}:${archiver.port}`)
          State.archiversReputation.set(archiver.publicKey, 'down')
        }
        i++
      }
    })
    .catch((error) => {
      // Handle any errors that occurred
      console.error(error)
    })
  if (config.VERBOSE) Logger.mainLogger.debug('Active archivers status', State.archiversReputation)
}
