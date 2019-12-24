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
  removed: string
  returned: string
  lost: string
  refuted: string
  apoptosized: string
}

export let currentCycleDuration = 0
export let currentCycleCounter = 0

export function processCycles(cycles: Cycle[]) {
  // Process 10 cycles max on each call
  let cycle: Cycle
  for (let i = 0; i < 10; i++) {
    // Save the cycle to db
    cycle = cycles[i]
    Storage.storeCycle(cycle)

    // Update NodeList from cycle info
    updateNodeList(cycle)

    // Update currentCycle state
    currentCycleDuration = cycle.duration
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
  NodeList.addNodes(NodeList.Statuses.SYNCING, ...joinedConsensors)

  // Update activated nodes
  const activated = safeParse<string[]>(
    [],
    cycle.activated,
    `Error processing cycle ${cycle.counter}: failed to parse activated`
  )
  NodeList.setStatus(NodeList.Statuses.ACTIVE, ...activated)

  // Remove removed nodes
  const removed = safeParse<string[]>(
    [],
    cycle.removed,
    `Error processing cycle ${cycle.counter}: failed to parse removed`
  )
  NodeList.removeNodes(...removed)

  // Remove lost nodes
  const lost = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse lost`
  )
  NodeList.removeNodes(...lost)

  // Remove apoptosized nodes
  const apoptosized = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse apoptosized`
  )
  NodeList.removeNodes(...apoptosized)
}
