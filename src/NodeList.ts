import { config } from './Config'

// TYPES

export enum Statuses {
  ACTIVE = 'active',
  SYNCING = 'syncing',
}

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
const syncingList: Map<string, ConsensusNodeInfo> = new Map()
const activeList: Map<string, ConsensusNodeInfo> = new Map()
const byPublicKey: { [publicKey: string]: ConsensusNodeInfo } = {}
const byIpPort: { [ipPort: string]: ConsensusNodeInfo } = {}

// METHODS

function getIpPort({ ip, port }: { ip: string; port: number }): string {
  return ip + ':' + port
}

export function isEmpty(): boolean {
  return list.length <= 0
}

export function addNodes(status: Statuses, ...nodes: ConsensusNodeInfo[]) {
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
    if (status === Statuses.SYNCING) {
      syncingList.set(node.publicKey, node)
    } else if (status === Statuses.ACTIVE) {
      activeList.set(node.publicKey, node)
    }
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
        if (syncingList.has(key)) syncingList.delete(key)
        else if (activeList.has(key)) activeList.delete(key)
      }
    }
  }

  return [...keysToDelete.keys()]
}

export function setStatus(status: Statuses, ...publicKeys: string[]) {
  for (const key of publicKeys) {
    const node = byPublicKey[key]
    if (node === undefined) {
      console.warn(`setStatus: publicKey ${key} not in nodelist`)
      continue
    }
    if (status === Statuses.SYNCING) {
      if (activeList.has(key)) activeList.delete(key)
      if (syncingList.has(key)) continue
      syncingList.set(key, node)
    } else if (status === Statuses.ACTIVE) {
      if (syncingList.has(key)) syncingList.delete(key)
      if (activeList.has(key)) continue
      activeList.set(key, node)
    }
  }
}

export function getList() {
  return list
}

export function getActiveList() {
  return [...activeList.values()]
}

export function getSyncingList() {
  return [...syncingList.values()]
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
