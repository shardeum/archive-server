import * as Storage from '../Storage'
import * as NodeList from '../NodeList'
import * as Crypto from '../Crypto'
import { safeParse } from '../Utils'
import { countReset } from 'console'

export interface Cycle {
  counter: number
  certificate: string
  previous: string
  marker: string
  start: number
  duration: number
  active: number
  desired: number
  expired: number
  syncing: number
  joined: string
  joinedArchivers: string
  joinedConsensors: string
  refreshedArchivers: string
  refreshedConsensors: string
  activated: string
  activatedPublicKeys: string
  removed: string
  returned: string
  lost: string
  refuted: string
  apoptosized: string
  networkDataHash: string
  networkReceiptHash: string
  networkSummaryHash: string
}

export let currentCycleDuration = 0
export let currentCycleCounter = -1
export let CycleChain: Map<Cycle["counter"], any> = new Map()

export function processCycles(cycles: Cycle[]) {
  for (const cycle of cycles) {
    console.log('New Cycle received', cycle.counter)
    // Skip if already processed [TODO] make this check more secure
    if (cycle.counter <= currentCycleCounter) continue

    // Update NodeList from cycle info
    updateNodeList(cycle)

    // Update currentCycle state
    currentCycleDuration = cycle.duration * 1000
    currentCycleCounter = cycle.counter

    console.log(`Processed cycle ${cycle.counter}`)
  }
}

export function getCurrentCycleCounter() {
  return currentCycleCounter
}

export function setCurrentCycleCounter(value: number) {
  currentCycleCounter = value
}

export function computeCycleMarker(fields: any) {
  const cycleMarker = Crypto.hashObj(fields)
  return cycleMarker
}

export function validateCycle(prev: Cycle, next: Cycle): boolean {
  // let previousRecordWithoutMarker: any = {...prev}
  // delete previousRecordWithoutMarker.marker

  // for (let key in previousRecordWithoutMarker) {
  //   try {
  //     previousRecordWithoutMarker[key] = JSON.parse(previousRecordWithoutMarker[key])
  //   } catch(e) {
  //     console.log(e)
  //     console.log('Unable to parse record field', key)
  //   }
  // }
  // const prevMarker = computeCycleMarker(previousRecordWithoutMarker)
  // if (next.previous !== prevMarker) return false
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
  // Add joined nodes
  const joinedConsensors = safeParse<JoinedConsensor[]>(
    [],
    cycle.joinedConsensors,
    `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`
  )

  const consensorInfos = joinedConsensors.map((jc) => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
    id: jc.id,
  }))

  NodeList.addNodes(NodeList.Statuses.SYNCING, cycle.marker, consensorInfos)

  // Update activated nodes
  const activatedPublicKeys = safeParse<string[]>(
    [],
    cycle.activatedPublicKeys,
    `Error processing cycle ${cycle.counter}: failed to parse activated`
  )
  NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activatedPublicKeys)

  // Remove removed nodes
  const removed = safeParse<string[]>(
    [],
    cycle.removed,
    `Error processing cycle ${cycle.counter}: failed to parse removed`
  )
  const removedPks = removed.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(removedPks)

  // Remove lost nodes
  const lost = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse lost`
  )
  const lostPks = lost.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(lostPks)

  // Remove apoptosized nodes
  const apoptosized = safeParse<string[]>(
    [],
    cycle.apoptosized,
    `Error processing cycle ${cycle.counter}: failed to parse apoptosized`
  )
  const apoptosizedPks = apoptosized.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(apoptosizedPks)
}
