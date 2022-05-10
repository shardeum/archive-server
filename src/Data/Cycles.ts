import * as NodeList from '../NodeList'
import * as Crypto from '../Crypto'
import * as State from '../State'
import * as Logger from '../Logger'
import { P2P } from '@shardus/types'
import { profilerInstance } from '../profiler/profiler'
import { nestedCountersInstance } from '../profiler/nestedCounters'

export interface Cycle extends P2P.CycleCreatorTypes.CycleRecord {
  certificate: string
  marker: string
}

export interface LostNode {
  counter: Cycle['counter']
  timestamp: number
  nodeInfo: NodeList.ConsensusNodeInfo
}

export let currentCycleDuration = 0
export let currentCycleCounter = -1
export let lastProcessedMetaData = -1
export let CycleChain: Map<Cycle['counter'], any> = new Map()
export let lostNodes: LostNode[] = []

export function processCycles(cycles: Cycle[]) {
  profilerInstance.profileSectionStart('process_cycle', false)
  nestedCountersInstance.countEvent('cycle', 'process', 1)
  for (const cycle of cycles) {
    Logger.mainLogger.debug(new Date(), 'New Cycle received', cycle.counter)
    Logger.mainLogger.debug('Current cycle counter', currentCycleCounter)
    // Skip if already processed [TODO] make this check more secure
    if (cycle.counter <= currentCycleCounter) continue

    // Update NodeList from cycle info
    updateNodeList(cycle)

    // Update currentCycle state
    currentCycleDuration = cycle.duration * 1000
    currentCycleCounter = cycle.counter

    Logger.mainLogger.debug(`Processed cycle ${cycle.counter}`)
  }
  profilerInstance.profileSectionEnd('process_cycle', false)
}

export function getCurrentCycleCounter() {
  return currentCycleCounter
}

export function getLostNodes(from: number, to: number) {
  return lostNodes.filter((node: LostNode) => {
    return node.counter >= from && node.counter <= to
  })
}

export function setCurrentCycleCounter(value: number) {
  currentCycleCounter = value
}

export function setLastProcessedMetaDataCounter(value: number) {
  lastProcessedMetaData = value
}

export function computeCycleMarker(fields: any) {
  const cycleMarker = Crypto.hashObj(fields)
  return cycleMarker
}

// validation of cycle record against previous marker
export function validateCycle(prev: Cycle, next: Cycle): boolean {
  let previousRecordWithoutMarker: any = { ...prev }
  delete previousRecordWithoutMarker.marker
  const prevMarker = computeCycleMarker(previousRecordWithoutMarker)
  if (next.previous !== prevMarker) return false
  return true
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

function updateNodeList(cycle: Cycle) {
  const {
    joinedConsensors,
    activatedPublicKeys,
    removed,
    lost,
    apoptosized,
    joinedArchivers,
    leavingArchivers,
  } = cycle

  const consensorInfos = joinedConsensors.map((jc) => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
    id: jc.id,
  }))

  const refreshedConsensorInfos = joinedConsensors.map((jc) => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
    id: jc.id,
  }))

  NodeList.addNodes(NodeList.Statuses.SYNCING, cycle.marker, consensorInfos)

  NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activatedPublicKeys)

  NodeList.refreshNodes(NodeList.Statuses.ACTIVE, cycle.marker, refreshedConsensorInfos)

  const removedPks = removed.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(removedPks)

  // add lost nodes to lostNodes collector
  lost.forEach((id: string) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    lostNodes.push({
      counter: cycle.counter,
      timestamp: Date.now(),
      nodeInfo,
    })
  })

  const lostPks = lost.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(lostPks)

  const apoptosizedPks = apoptosized.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(apoptosizedPks)

  for (let joinedArchiver of joinedArchivers) {
    let foundArchiver = State.activeArchivers.find(
      (a) => a.publicKey === joinedArchiver.publicKey
    )
    if (!foundArchiver) {
      State.activeArchivers.push(joinedArchiver)
      Logger.mainLogger.debug(
        'New archiver added to active list',
        joinedArchiver
      )
    }
    Logger.mainLogger.debug('active archiver list', State.activeArchivers)
  }

  for (let leavingArchiver of leavingArchivers) {
    State.removeActiveArchiver(leavingArchiver.publicKey)
  }
}
