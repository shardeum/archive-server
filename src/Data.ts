import * as Storage from './Storage'
import * as NodeList from './NodeList'
import { safeParse } from './Utils'

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

const cycleSenders: Map<
  NodeList.ConsensusNodeInfo['publicKey'],
  NodeList.ConsensusNodeInfo
> = new Map()

export function processNewCycle(cycle: Cycle) {
  // Save the cycle to db
  Storage.storeCycle(cycle)

  // Update NodeList from cycle info
  updateNodeList(cycle)

  // Update cycleSenders

  console.log(`Processed cycle ${cycle.counter}`)
}

function updateNodeList(cycle: Cycle) {
  //   Add joined nodes
  const joinedConsensors = safeParse<NodeList.ConsensusNodeInfo[]>(
    [],
    cycle.joinedConsensors,
    `Error processing cycle ${cycle.counter}: failed to parse joinedConsensors`
  )
  NodeList.addNodes(...joinedConsensors)

  //   Remove removed nodes
  const removed = safeParse<string[]>(
    [],
    cycle.removed,
    `Error processing cycle ${cycle.counter}: failed to parse removed`
  )
  NodeList.removeNodes(...removed)
}

export function addCycleSenders(...nodes: NodeList.ConsensusNodeInfo[]) {
  for (const node of nodes) {
    if (cycleSenders.has(node.publicKey) === false) {
      cycleSenders.set(node.publicKey, node)
    }
  }
}

export function removeCycleSenders(...publicKeys: string[]) {
  for (const key of publicKeys) {
    if (cycleSenders.has(key) === true) {
      cycleSenders.delete(key)
    }
  }
}
