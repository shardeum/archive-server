import * as State from './State'
import * as P2P from './P2P'
import * as Utils from './Utils'
import { isDeepStrictEqual } from 'util'
import * as Logger from './Logger'
import * as Crypto from './Crypto'
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
  id?: string
}

export interface ConsensusNodeMetadata {
  cycleMarkerJoined: string
}

export interface SignedList {
  nodeList: ConsensusNodeInfo[]
}

export interface JoinedConsensor extends ConsensusNodeInfo {
  cycleJoined: string
  counterRefreshed: number
  id: string
}

const byAscendingNodeId = (a: ConsensusNodeInfo, b: ConsensusNodeInfo) => (a.id > b.id ? 1 : -1)

export const byAscendingPublicKey = (a: State.ArchiverNodeInfo, b: State.ArchiverNodeInfo) =>
  a.publicKey > b.publicKey ? 1 : -1

// STATE

const list: ConsensusNodeInfo[] = []
const metadata: Map<string, ConsensusNodeMetadata> = new Map()
const syncingList: Map<string, ConsensusNodeInfo> = new Map()
export const activeList: Map<string, ConsensusNodeInfo> = new Map()
export let activeListByIdSorted: ConsensusNodeInfo[] = []
export const byPublicKey: { [publicKey: string]: ConsensusNodeInfo } = {}
const byIpPort: { [ipPort: string]: ConsensusNodeInfo } = {}
export const byId: { [id: string]: ConsensusNodeInfo } = {}
const publicKeyToId: { [publicKey: string]: string } = {}

export type SignedNodeList = {
  nodeList: ConsensusNodeInfo[]
} & Crypto.types.SignedObject

export const cache: Map<string, SignedNodeList> = new Map()
export const cacheUpdatedTimes: Map<string, number> = new Map()
export const realUpdatedTimes: Map<string, number> = new Map()

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
  nodes: ConsensusNodeInfo[] | JoinedConsensor[]
) {
  Logger.mainLogger.debug('Typeof Nodes to add', typeof nodes)
  Logger.mainLogger.debug('Length of Nodes to add', nodes.length)
  Logger.mainLogger.debug('Nodes to add', nodes)
  for (const node of nodes) {
    const ipPort = getIpPort(node)

    // If node not in lists, add it
    if (byPublicKey[node.publicKey] === undefined && byIpPort[ipPort] === undefined) {
      Logger.mainLogger.debug('adding new node', node.publicKey)
      list.push(node)
      if (status === Statuses.SYNCING) {
        syncingList.set(node.publicKey, node)
      } else if (status === Statuses.ACTIVE) {
        activeList.set(node.publicKey, node)
        Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
        // Logger.mainLogger.debug(
        //   'activeListByIdSorted',
        //   activeListByIdSorted.map((node) => node.id)
        // )
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

  // Set updated time for cache
  if (nodes.length > 0) {
    realUpdatedTimes.set('/nodelist', Date.now())
  }
}
export function refreshNodes(
  status: Statuses,
  cycleMarkerJoined: string,
  nodes: ConsensusNodeInfo[] | JoinedConsensor[]
) {
  Logger.mainLogger.debug('Typeof Nodes to refresh', typeof nodes)
  Logger.mainLogger.debug('Length of Nodes to refresh', nodes.length)
  Logger.mainLogger.debug('Nodes to refresh', nodes)
  for (const node of nodes) {
    const ipPort = getIpPort(node)

    // If node not in lists, add it
    if (byPublicKey[node.publicKey] === undefined && byIpPort[ipPort] === undefined) {
      Logger.mainLogger.debug('adding new node during refresh', node.publicKey)
      list.push(node)
      if (status === Statuses.SYNCING) {
        syncingList.set(node.publicKey, node)
      } else if (status === Statuses.ACTIVE) {
        activeList.set(node.publicKey, node)
        Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
        // Logger.mainLogger.debug(
        //   'activeListByIdSorted',
        //   activeListByIdSorted.map((node) => node.id)
        // )
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
  }

  // Set updated time for cache
  if (nodes.length > 0) {
    realUpdatedTimes.set('/nodelist', Date.now())
  }
}

export function removeNodes(publicKeys: string[]): string[] {
  if (publicKeys.length > 0) Logger.mainLogger.debug('Removing nodes', publicKeys)
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
    activeListByIdSorted = activeListByIdSorted.filter((node) => node.id !== id)
    delete byId[id]
    delete publicKeyToId[key]
  }

  if (keysToDelete.size > 0) {
    let key: string
    for (let i = list.length - 1; i > -1; i--) {
      key = list[i].publicKey
      if (keysToDelete.has(key)) {
        list.splice(i, 1)
        if (syncingList.has(key)) syncingList.delete(key)
        else if (activeList.has(key)) activeList.delete(key)
      }
    }

    // Set updated time for cache
    realUpdatedTimes.set('/nodelist', Date.now())
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
      Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
      // Logger.mainLogger.debug(
      //   'activeListByIdSorted',
      //   activeListByIdSorted.map((node) => node.id)
      // )
    }
  }

  // Set updated time for cache
  if (publicKeys.length > 0) {
    realUpdatedTimes.set('/nodelist', Date.now())
  }
}

export function getList() {
  return list
}

export function getActiveList() {
  return [...activeList.values()]
}

export async function getActiveNodeListFromArchiver(
  archiver: State.ArchiverNodeInfo
): Promise<ConsensusNodeInfo[]> {
  let response: any = await P2P.getJson(`http://${archiver.ip}:${archiver.port}/nodelist`)
  Logger.mainLogger.debug('response', `http://${archiver.ip}:${archiver.port}/nodelist`, response)
  if (response && response.nodeList && response.nodeList.length > 0) {
    // TODO: validate the reponse is from archiver
    return response.nodeList
  } else Logger.mainLogger.debug(`Fail To get nodeList from the archiver ${archiver.ip}:${archiver.port}`)
  return []
}

// We can't get the same node list from all archivers; Each archiver would response with its max 30 random nodes
export async function getActiveListFromArchivers(
  activeArchivers: State.ArchiverNodeInfo[]
): Promise<ConsensusNodeInfo> {
  function isSameCyceInfo(info1: any, info2: any) {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: any) => {
    const response: any = await P2P.getJson(`http://${node.ip}:${node.port}/nodelist`)
    if (response.nodeList) return response.nodeList.sort((a: any, b: any) => a.publicKey - b.publicKey)
  }
  let nodeList = await Utils.robustQuery(activeArchivers, queryFn, isSameCyceInfo)
  return nodeList.value[0]
}

export function getRandomActiveNodes(node_count: number = 1): ConsensusNodeInfo[] {
  let nodeList = getActiveList()
  if (node_count <= 1 || node_count > nodeList.length)
    return Utils.getRandomItemFromArr(nodeList, config.N_NODE_REJECT_PERCENT)

  return Utils.getRandomItemFromArr(nodeList, config.N_NODE_REJECT_PERCENT, node_count)
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
