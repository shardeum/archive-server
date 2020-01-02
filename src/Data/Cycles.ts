import * as Storage from '../Storage'
import * as NodeList from '../NodeList'
import { safeParse } from '../Utils'

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
  joined: string
  joinedArchivers: string
  joinedConsensors: string
  activated: string
  activatedPublicKeys: string
  removed: string
  returned: string
  lost: string
  refuted: string
  apoptosized: string
}

export let currentCycleDuration = 0
export let currentCycleCounter = 0

export function processCycles(cycles: Cycle[]) {
  for (const cycle of cycles) {
    // Skip if already processed [TODO] make this check more secure
    if (currentCycleCounter > 0 && cycle.counter <= currentCycleCounter) {
      continue
    }

    // Save the cycle to db
    Storage.storeCycle(cycle)

    // Update NodeList from cycle info
    updateNodeList(cycle)

    // Update currentCycle state
    currentCycleDuration = cycle.duration * 1000
    currentCycleCounter = cycle.counter

    console.log(`Processed cycle ${cycle.counter}`)
  }
}

function updateNodeList(cycle: Cycle) {
  // Add joined nodes
  const joinedConsensors = safeParse<NodeList.ConsensusNodeInfo[]>(
    [],
    cycle.joinedConsensors,
    `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`
  )
  NodeList.addNodes(
    NodeList.Statuses.SYNCING,
    cycle.marker,
    ...joinedConsensors
  )

  // Update activated nodes
  const activatedPublicKeys = safeParse<string[]>(
    [],
    cycle.activatedPublicKeys,
    `Error processing cycle ${cycle.counter}: failed to parse activated`
  )
  NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activatedPublicKeys)
  NodeList.addNodeId(...activatedPublicKeys)

  // Remove removed nodes
  const removed = safeParse<string[]>(
    [],
    cycle.removed,
    `Error processing cycle ${cycle.counter}: failed to parse removed`
  )
  NodeList.removeNodes(
    ...removed.map(id => NodeList.getNodeInfoById(id).publicKey)
  )

  // Remove lost nodes
  const lost = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse lost`
  )
  NodeList.removeNodes(
    ...lost.map(id => NodeList.getNodeInfoById(id).publicKey)
  )

  // Remove apoptosized nodes
  const apoptosized = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse apoptosized`
  )
  NodeList.removeNodes(
    ...apoptosized.map(id => NodeList.getNodeInfoById(id).publicKey)
  )
}
