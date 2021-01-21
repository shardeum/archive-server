import { config } from './Config'
import * as Crypto from './Crypto'
import * as State from './State'
import * as P2P from './P2P'
import * as Data from './Data/Data'
import * as Utils from './Utils'
import { isDeepStrictEqual } from 'util'
// TYPES

export enum Statuses {
  ACTIVE = 'active',
  SYNCING = 'syncing',
}

export interface ConsensusNodeInfo {
  ip: string
  port: number
  publicKey: string
  id?: string
}

export interface ConsensusNodeMetadata {
  cycleMarkerJoined: string
}

export interface SignedList {
  nodeList: ConsensusNodeInfo[]
}

// STATE

const list: ConsensusNodeInfo[] = []
const metadata: Map<string, ConsensusNodeMetadata> = new Map()
const syncingList: Map<string, ConsensusNodeInfo> = new Map()
export const activeList: Map<string, ConsensusNodeInfo> = new Map()
export const byPublicKey: { [publicKey: string]: ConsensusNodeInfo } = {}
const byIpPort: { [ipPort: string]: ConsensusNodeInfo } = {}
export const byId: { [id: string]: ConsensusNodeInfo } = {}
const publicKeyToId: { [publicKey: string]: string } = {}

// METHODS

function getIpPort(node: any) {
  if (node.ip && node.port) {
    return node.ip + ':' + node.port
  } else if (node.externalIp && node.externalPort) {
    return node.externalIp + ':' + node.externalPort
  }
  return ''
}

export function isEmpty(): boolean {
  return list.length <= 0
}

export function addNodes(
  status: Statuses,
  cycleMarkerJoined: string,
  nodes: ConsensusNodeInfo[] | Data.JoinedConsensor[]
) {
  console.log('Typeof Nodes to add', typeof nodes)
  console.log('Length of Nodes to add', nodes.length)
  console.log('Nodes to add', nodes)
  for (const node of nodes) {
    const ipPort = getIpPort(node)

    // If node not in lists, add it
    if (
      byPublicKey[node.publicKey] === undefined &&
      byIpPort[ipPort] === undefined
    ) {
      console.log('adding new node', node.publicKey)
      list.push(node)
      if (status === Statuses.SYNCING) {
        syncingList.set(node.publicKey, node)
      } else if (status === Statuses.ACTIVE) {
        activeList.set(node.publicKey, node)
      }

      byPublicKey[node.publicKey] = node
      byIpPort[ipPort] = node
    }

    // If an id is given, update its id
    if (node.id) {
      const entry = byPublicKey[node.publicKey]
      if (entry) {
        entry.id = node.id
        publicKeyToId[node.publicKey] = node.id
        byId[node.id] = node
      }
    }

    // Update its metadata
    metadata.set(node.publicKey, {
      cycleMarkerJoined,
    })
  }
}

export function removeNodes(publicKeys: string[]): string[] {
  console.log('Removing nodes', publicKeys)
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
    const id = publicKeyToId[key]
    delete byId[id]
    delete publicKeyToId[key]
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

export async function getActiveListFromArchivers(activeArchivers: State.ArchiverNodeInfo[]): Promise<ConsensusNodeInfo> {
  function isSameCyceInfo (info1: any, info2: any) {
    // console.log('info1', info1)
    // console.log('info2', info2)
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(
      `http://${node.ip}:${node.port}/nodelist`
    )
    if(response.nodeList) return response.nodeList.sort((a: any, b: any) => a.publicKey - b.publicKey)
  }
  let nodeList: any = await Utils.robustQuery(
    activeArchivers,
    queryFn,
    isSameCyceInfo
  )
  return nodeList[0]
}

export function getRandomNode(nodeList: ConsensusNodeInfo[]): ConsensusNodeInfo {
  const randomIndex = Math.floor(Math.random() * nodeList.length)
  const randomConsensor: ConsensusNodeInfo = nodeList[randomIndex]
  return randomConsensor
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

export function getId(publicKey: string) {
  return publicKeyToId[publicKey]
}

export function getNodeInfoById(id: string) {
  return byId[id]
}
