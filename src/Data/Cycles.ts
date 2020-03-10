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

interface P2PNodeInfo {
  publicKey: string
  externalIp: string
  externalPort: number
  internalIp: string
  internalPort: number
  address: string
  joinRequestTimestamp: number
  activeTimestamp: number
}

function updateNodeList(cycle: Cycle) {
  // Add joined nodes
  const joinedConsensors = safeParse<P2PNodeInfo[]>(
    [],
    cycle.joinedConsensors,
    `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`
  )

  const consensorInfos = joinedConsensors.map(jc => ({
    ip: jc.externalIp,
    port: jc.externalPort,
    publicKey: jc.publicKey,
  }))

  NodeList.addNodes(NodeList.Statuses.SYNCING, cycle.marker, ...consensorInfos)

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
  const removedPks = removed.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(...removedPks)

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
  NodeList.removeNodes(...lostPks)

  // Remove apoptosized nodes
  const apoptosized = safeParse<string[]>(
    [],
    cycle.lost,
    `Error processing cycle ${cycle.counter}: failed to parse apoptosized`
  )
  const apoptosizedPks = apoptosized.reduce((keys: string[], id) => {
    const nodeInfo = NodeList.getNodeInfoById(id)
    if (nodeInfo) {
      keys.push(nodeInfo.publicKey)
    }
    return keys
  }, [])
  NodeList.removeNodes(...apoptosizedPks)
}
