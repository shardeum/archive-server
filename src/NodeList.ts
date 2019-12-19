import { config } from './Config'

// TYPES

export interface ConsensusNodeInfo {
  ip: string
  port: number
  publicKey: string
}

export interface SignedList {
  nodeList: ConsensusNodeInfo[]
}

// STATE

const list: ConsensusNodeInfo[] = []
const byPublicKey: { [publicKey: string]: ConsensusNodeInfo } = {}
const byIpPort: { [ipPort: string]: ConsensusNodeInfo } = {}

// METHODS

function getIpPort({ ip, port }: { ip: string; port: number }): string {
  return ip + ':' + port
}

export function isEmpty(): boolean {
  return list.length <= 0
}

export function addNodes(...nodes: ConsensusNodeInfo[]) {
  for (const node of nodes) {
    if (byPublicKey[node.publicKey] !== undefined) {
      console.warn(
        `addNodes failed: publicKey ${node.publicKey} already in nodelist`
      )
      return
    }

    const ipPort = getIpPort(node)

    if (byIpPort[ipPort] !== undefined) {
      console.warn(`addNodes failed: ipPort ${ipPort} already in nodelist`)
      return
    }

    list.push(node)
    byPublicKey[node.publicKey] = node
    byIpPort[ipPort] = node
  }
}

export function removeNodes(...publicKeys: string[]): string[] {
  // Efficiently remove nodes from nodelist
  const keysToDelete: Map<ConsensusNodeInfo['publicKey'], boolean> = new Map()

  for (const key of publicKeys) {
    if (byPublicKey[key] === undefined) {
      console.warn(`removeNodes: publicKey ${key} not in nodelist`)
      continue
    }
    keysToDelete.set(key, true)
    delete byIpPort[getIpPort(byPublicKey[key])]
    delete byPublicKey[key]
  }

  if (keysToDelete.size > 0) {
    let key
    for (let i = list.length - 1; i > -1; i--) {
      key = list[i].publicKey
      if (keysToDelete.has(key)) {
        list.splice(i, 1)
      }
    }
  }

  return [...keysToDelete.keys()]
}

export function getList() {
  return list
}

export function getNodeInfo(node: Partial<ConsensusNodeInfo>) {
  // Prefer publicKey
  if (node.publicKey) {
    return byPublicKey[node.publicKey]
  }
  // Then, ipPort
  else if (node.ip && node.port) {
    return byIpPort[getIpPort(node as ConsensusNodeInfo)]
  }
  // If nothing found, return undefined
  return undefined
}
