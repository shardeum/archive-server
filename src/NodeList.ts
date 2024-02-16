import * as State from './State'
import * as P2P from './P2P'
import * as Utils from './Utils'
import { isDeepStrictEqual } from 'util'
import * as Logger from './Logger'
import { config } from './Config'
import * as Crypto from './Crypto'
import { P2P as P2PTypes } from '@shardus/types'
import { SignedObject } from '@shardus/crypto-utils'
// TYPES

export enum NodeStatus {
  STANDBY = 'standby',
  ACTIVE = 'active',
  SYNCING = 'syncing',
}

export interface ConsensusNodeInfo {
  ip: string
  port: number
  publicKey: string
  id?: string
  externalIp?: string
  externalPort?: number
}

export interface ConsensusNodeListResponse {
  nodeList: ConsensusNodeInfo[]
}

export interface ConsensusNodeListResponse {
  nodeList: ConsensusNodeInfo[]
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

const byAscendingNodeId = (a: ConsensusNodeInfo, b: ConsensusNodeInfo): number => (a.id > b.id ? 1 : -1)

export const byAscendingPublicKey = (a: State.ArchiverNodeInfo, b: State.ArchiverNodeInfo): number =>
  a.publicKey > b.publicKey ? 1 : -1

export const fromP2PTypesJoinedConsensor = (
  joinedConsensor: P2PTypes.JoinTypes.JoinedConsensor
): JoinedConsensor => ({
  ip: joinedConsensor.internalIp,
  port: joinedConsensor.internalPort,
  publicKey: joinedConsensor.publicKey,
  id: joinedConsensor.id,
  externalIp: joinedConsensor.externalIp,
  externalPort: joinedConsensor.externalPort,
  cycleJoined: joinedConsensor.cycleJoined,
  counterRefreshed: joinedConsensor.counterRefreshed,
})

export const fromP2PTypesNode = (node: P2PTypes.NodeListTypes.Node): JoinedConsensor => ({
  ip: node.internalIp,
  port: node.internalPort,
  publicKey: node.publicKey,
  id: node.id,
  externalIp: node.externalIp,
  externalPort: node.externalPort,
  cycleJoined: node.cycleJoined,
  counterRefreshed: node.counterRefreshed,
})

// STATE

const list: ConsensusNodeInfo[] = []
const standbyList: Map<string, ConsensusNodeInfo> = new Map()
const syncingList: Map<string, ConsensusNodeInfo> = new Map()
const activeList: Map<string, ConsensusNodeInfo> = new Map()
export let activeListByIdSorted: ConsensusNodeInfo[] = []
export let byPublicKey: { [publicKey: string]: ConsensusNodeInfo } = {}
let byIpPort: { [ipPort: string]: ConsensusNodeInfo } = {}
export let byId: { [id: string]: ConsensusNodeInfo } = {}
let publicKeyToId: { [publicKey: string]: string } = {}
export let foundFirstNode = false

export type SignedNodeList = {
  nodeList: ConsensusNodeInfo[]
} & SignedObject

export const activeNodescache: Map<string, SignedNodeList> = new Map()
export const fullNodesCache: Map<string, SignedNodeList> = new Map()
export const cacheUpdatedTimes: Map<string, number> = new Map()
export const realUpdatedTimes: Map<string, number> = new Map()

// METHODS

function getIpPort(node: Node): string {
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

type Node = ConsensusNodeInfo | JoinedConsensor

export function addNodes(status: NodeStatus, nodes: Node[]): void {
  if (nodes.length === 0) return
  Logger.mainLogger.debug(`Adding ${status} nodes to the list`, nodes.length, nodes)
  for (const node of nodes) {
    const ipPort = getIpPort(node)

    // If node not in lists, add it
    // eslint-disable-next-line security/detect-object-injection
    if (byPublicKey[node.publicKey] === undefined && byIpPort[ipPort] === undefined) {
      list.push(node)
      const key = node.publicKey
      switch (status) {
        case NodeStatus.SYNCING:
          if (standbyList.has(key)) standbyList.delete(key)
          if (activeList.has(key)) {
            activeList.delete(key)
            activeListByIdSorted = activeListByIdSorted.filter((node) => node.publicKey === key)
          }
          if (syncingList.has(key)) break
          syncingList.set(node.publicKey, node)
          break
        case NodeStatus.ACTIVE:
          if (standbyList.has(key)) standbyList.delete(key)
          if (syncingList.has(key)) syncingList.delete(key)
          if (activeList.has(key)) break
          activeList.set(node.publicKey, node)
          Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
          // Logger.mainLogger.debug(
          //   'activeListByIdSorted',
          //   activeListByIdSorted.map((node) => node.id)
          // )
          break
      }
      /* eslint-disable security/detect-object-injection */
      byPublicKey[node.publicKey] = node
      byIpPort[ipPort] = node
      /* eslint-enable security/detect-object-injection */
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
}
export function refreshNodes(status: NodeStatus, nodes: ConsensusNodeInfo[] | JoinedConsensor[]): void {
  if (nodes.length === 0) return
  Logger.mainLogger.debug('Refreshing nodes', nodes.length, nodes)
  for (const node of nodes) {
    const ipPort = getIpPort(node)

    // If node not in lists, add it
    // eslint-disable-next-line security/detect-object-injection
    if (byPublicKey[node.publicKey] === undefined && byIpPort[ipPort] === undefined) {
      Logger.mainLogger.debug('adding new node during refresh', node.publicKey)
      list.push(node)
      switch (status) {
        case NodeStatus.SYNCING:
          syncingList.set(node.publicKey, node)
          break
        case NodeStatus.ACTIVE:
          activeList.set(node.publicKey, node)
          Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
          // Logger.mainLogger.debug(
          //   'activeListByIdSorted',
          //   activeListByIdSorted.map((node) => node.id)
          // )
          break
      }

      byPublicKey[node.publicKey] = node
      // eslint-disable-next-line security/detect-object-injection
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
}

export function removeNodes(publicKeys: string[]): void {
  if (publicKeys.length > 0) Logger.mainLogger.debug('Removing nodes', publicKeys)
  // Efficiently remove nodes from nodelist
  const keysToDelete: Map<ConsensusNodeInfo['publicKey'], boolean> = new Map()

  for (const key of publicKeys) {
    // eslint-disable-next-line security/detect-object-injection
    if (byPublicKey[key] === undefined) {
      console.warn(`removeNodes: publicKey ${key} not in nodelist`)
      continue
    }
    keysToDelete.set(key, true)
    /* eslint-disable security/detect-object-injection */
    delete byIpPort[getIpPort(byPublicKey[key])]
    delete byPublicKey[key]
    const id = publicKeyToId[key]
    activeListByIdSorted = activeListByIdSorted.filter((node) => node.id !== id)
    delete byId[id]
    delete publicKeyToId[key]
    /* eslint-enable security/detect-object-injection */
  }

  if (keysToDelete.size > 0) {
    let key: string
    for (let i = list.length - 1; i > -1; i--) {
      // eslint-disable-next-line security/detect-object-injection
      key = list[i].publicKey
      if (keysToDelete.has(key)) {
        list.splice(i, 1)
        if (syncingList.has(key)) syncingList.delete(key)
        else if (activeList.has(key)) activeList.delete(key)
        else if (standbyList.has(key)) standbyList.delete(key)
      }
    }
  }
}

export const addStandbyNodes = (nodes: ConsensusNodeInfo[]): void => {
  if (nodes.length === 0) return
  Logger.mainLogger.debug('Adding standby nodes to the list', nodes.length, nodes)
  for (const node of nodes) {
    if (standbyList.has(node.publicKey)) continue
    standbyList.set(node.publicKey, node)
  }
}

export const removeStandbyNodes = (publicKeys: string[]): void => {
  if (publicKeys.length > 0) Logger.mainLogger.debug('Removing standby nodes', publicKeys)
  for (const key of publicKeys) {
    if (standbyList.has(key)) standbyList.delete(key)
  }
}

export function setStatus(status: NodeStatus, publicKeys: string[]): void {
  if (publicKeys.length === 0) return
  Logger.mainLogger.debug(`Updating status ${status} for nodes`, publicKeys)
  for (const key of publicKeys) {
    // eslint-disable-next-line security/detect-object-injection
    const node = byPublicKey[key]
    if (node === undefined) {
      console.warn(`setStatus: publicKey ${key} not in nodelist`)
      continue
    }
    switch (status) {
      case NodeStatus.SYNCING:
        if (standbyList.has(key)) standbyList.delete(key)
        if (activeList.has(key)) {
          activeList.delete(key)
          activeListByIdSorted = activeListByIdSorted.filter((node) => node.publicKey === key)
        }
        if (syncingList.has(key)) break
        syncingList.set(key, node)
        break
      case NodeStatus.ACTIVE:
        if (standbyList.has(key)) standbyList.delete(key)
        if (syncingList.has(key)) syncingList.delete(key)
        if (activeList.has(key)) break
        activeList.set(key, node)
        Utils.insertSorted(activeListByIdSorted, node, byAscendingNodeId)
        // Logger.mainLogger.debug(
        //   'activeListByIdSorted',
        //   activeListByIdSorted.map((node) => node.id)
        // )
        break
      case NodeStatus.STANDBY:
        if (activeList.has(key)) {
          activeList.delete(key)
          activeListByIdSorted = activeListByIdSorted.filter((node) => node.publicKey === key)
        }
        if (syncingList.has(key)) syncingList.delete(key)
        if (standbyList.has(key)) break
        standbyList.set(key, node)
    }
  }
}

export function getList(): ConsensusNodeInfo[] {
  return list
}

export function getActiveList(): ConsensusNodeInfo[] {
  // return [...activeList.values()]
  return activeListByIdSorted
}

export function getActiveNodeCount(): number {
  return activeList.size
}

/**
 * Check the cache for the node list, if it's hot, return it. Otherwise,
 * rebuild the cache and return the node list.
 */
export const getCachedNodeList = (): SignedNodeList => {
  const cacheUpdatedTime = cacheUpdatedTimes.get('/nodelist')
  const realUpdatedTime = realUpdatedTimes.get('/nodelist')

  const bucketCacheKey = (index: number): string => `/nodelist/${index}`

  if (cacheUpdatedTime && realUpdatedTime && cacheUpdatedTime > realUpdatedTime) {
    // cache is hot, send cache

    const randomIndex = Math.floor(Math.random() * config.N_RANDOM_NODELIST_BUCKETS)
    const cachedNodeList = activeNodescache.get(bucketCacheKey(randomIndex))
    return cachedNodeList
  }

  // cache is cold, remake cache
  const nodeCount = Math.min(config.N_NODELIST, getActiveNodeCount())

  for (let index = 0; index < config.N_RANDOM_NODELIST_BUCKETS; index++) {
    // If we dont have any active nodes, send back the first node in our list
    const nodeList = nodeCount < 1 ? getList().slice(0, 1) : getRandomActiveNodes(nodeCount)
    const sortedNodeList = [...nodeList].sort(byAscendingNodeId)
    const signedSortedNodeList = Crypto.sign({
      nodeList: sortedNodeList,
    })

    // Update cache
    activeNodescache.set(bucketCacheKey(index), signedSortedNodeList)
  }

  // Update cache timestamps
  if (realUpdatedTimes.get('/nodelist') === undefined) {
    // This gets set when the list of nodes changes. For the first time, set to a large value
    realUpdatedTimes.set('/nodelist', Infinity)
  }
  cacheUpdatedTimes.set('/nodelist', Date.now())

  const nodeList = activeNodescache.get(bucketCacheKey(0))
  return nodeList
}

export const getCachedFullNodeList = (
  activeOnly: boolean,
  syncingOnly: boolean,
  standbyOnly: boolean
): SignedNodeList => {
  const cacheUpdatedTime = cacheUpdatedTimes.get('/full-nodelist')
  const realUpdatedTime = realUpdatedTimes.get('/full-nodelist')

  if (cacheUpdatedTime && realUpdatedTime && cacheUpdatedTime > realUpdatedTime) {
    // cache is hot, send cache
    if (activeOnly) return fullNodesCache.get('/full-active-nodelist')
    if (syncingOnly) return fullNodesCache.get('/full-syncing-nodelist')
    if (standbyOnly) return fullNodesCache.get('/full-standby-nodelist')
    return fullNodesCache.get('/full-active-syncing-nodelist')
  }

  // cache is cold, remake cache
  const activeNodeList = getActiveList()
  const syncingNodeList = getSyncingList()
  const standbyNodeList = getStandbyList()
  const activeSyncingNodeList = activeNodeList.concat(syncingNodeList) // active + syncing
  fullNodesCache.set('/full-active-nodelist', Crypto.sign({ nodeList: activeNodeList }))
  fullNodesCache.set('/full-syncing-nodelist', Crypto.sign({ nodeList: syncingNodeList }))
  fullNodesCache.set('/full-standby-nodelist', Crypto.sign({ nodeList: standbyNodeList }))
  fullNodesCache.set('/full-active-syncing-nodelist', Crypto.sign({ nodeList: activeSyncingNodeList }))

  // Update cache timestamps
  if (realUpdatedTimes.get('/full-nodelist') === undefined) {
    // This gets set when the list of nodes changes. For the first time, set to a large value
    realUpdatedTimes.set('/full-nodelist', Infinity)
  }
  cacheUpdatedTimes.set('/full-nodelist', Date.now())

  if (activeOnly) return fullNodesCache.get('/full-active-nodelist')
  if (syncingOnly) return fullNodesCache.get('/full-syncing-nodelist')
  if (standbyOnly) return fullNodesCache.get('/full-standby-nodelist')
  return fullNodesCache.get('/full-active-syncing-nodelist')
}

export async function getActiveNodeListFromArchiver(
  archiver: State.ArchiverNodeInfo
): Promise<ConsensusNodeInfo[]> {
  const response = (await P2P.getJson(
    `http://${archiver.ip}:${archiver.port}/nodelist`
  )) as ConsensusNodeListResponse
  Logger.mainLogger.debug('response', `http://${archiver.ip}:${archiver.port}/nodelist`, response)
  if (response && response.nodeList && response.nodeList.length > 0) {
    // TODO: validate the response is from archiver
    return response.nodeList
  } else Logger.mainLogger.debug(`Fail To get nodeList from the archiver ${archiver.ip}:${archiver.port}`)
  return []
}

// We can't get the same node list from all archivers; Each archiver would response with its max 30 random nodes
export async function getActiveListFromArchivers(
  activeArchivers: State.ArchiverNodeInfo[]
): Promise<ConsensusNodeInfo> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function isSameCycleInfo(info1: any, info2: any): boolean {
    const cm1 = Utils.deepCopy(info1)
    const cm2 = Utils.deepCopy(info2)
    delete cm1.currentTime
    delete cm2.currentTime
    const equivalent = isDeepStrictEqual(cm1, cm2)
    return equivalent
  }

  const queryFn = async (node: ConsensusNodeInfo): Promise<ConsensusNodeInfo[]> => {
    const response = (await P2P.getJson(
      `http://${node.ip}:${node.port}/nodelist`
    )) as ConsensusNodeListResponse

    if (response && response.nodeList) {
      return response.nodeList.sort((a: ConsensusNodeInfo, b: ConsensusNodeInfo) =>
        a.publicKey > b.publicKey ? 1 : -1
      )
    }
    return null
  }
  const nodeList = await Utils.robustQuery(activeArchivers, queryFn, isSameCycleInfo)
  if (nodeList && nodeList.count > 0) {
    return nodeList.value[0]
  }
  return null
}

export function getRandomActiveNodes(node_count = 1): ConsensusNodeInfo[] {
  const nodeList = getActiveList()
  if (node_count <= 1 || node_count > nodeList.length)
    return Utils.getRandomItemFromArr(nodeList, config.N_NODE_REJECT_PERCENT)

  return Utils.getRandomItemFromArr(nodeList, config.N_NODE_REJECT_PERCENT, node_count)
}

export function getSyncingList(): ConsensusNodeInfo[] {
  return [...syncingList.values()]
}

export function getStandbyList(): ConsensusNodeInfo[] {
  return [...standbyList.values()]
}

export function getNodeInfo(node: Partial<ConsensusNodeInfo>): ConsensusNodeInfo | undefined {
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

export function getId(publicKey: string): string | undefined {
  // eslint-disable-next-line security/detect-object-injection
  return publicKeyToId[publicKey]
}

export function getNodeInfoById(id: string): ConsensusNodeInfo | undefined {
  // eslint-disable-next-line security/detect-object-injection
  return byId[id]
}

export function changeNodeListInRestore(): void {
  if (activeList.size === 0) return
  // change the active status nodes to syncing status in all the nodelist
  const activatedPublicKeys = activeListByIdSorted.map((node) => node.publicKey)
  setStatus(NodeStatus.SYNCING, activatedPublicKeys)
}

/** Resets/Cleans all the NodeList associated Maps and Array variables/caches */
export function clearNodeListCache(): void {
  try {
    activeNodescache.clear()
    fullNodesCache.clear()
    activeList.clear()
    syncingList.clear()
    realUpdatedTimes.clear()
    cacheUpdatedTimes.clear()

    list.length = 0
    byId = {}
    byIpPort = {}
    byPublicKey = {}
    publicKeyToId = {}
    activeListByIdSorted = []
  } catch (e) {
    Logger.mainLogger.error('Error thrown in clearNodeListCache', e)
  }
}

export function toggleFirstNode(): void {
  foundFirstNode = !foundFirstNode
}
