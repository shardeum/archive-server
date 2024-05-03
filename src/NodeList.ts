import * as State from './State'
import * as P2P from './P2P'
import * as Utils from './Utils'
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

const standbyList: Map<string, ConsensusNodeInfo> = new Map()
const syncingList: Map<string, ConsensusNodeInfo> = new Map()
const activeList: Map<string, ConsensusNodeInfo> = new Map()

// Map to get node public key by node Id
const byId: Map<string, string> = new Map()

// Map to get node info by public key, stores all nodes
export const byPublicKey: Map<string, ConsensusNodeInfo> = new Map()

// Array of active nodes sorted by id
export let activeListByIdSorted: ConsensusNodeInfo[] = []

export let foundFirstNode = false

export type SignedNodeList = {
  nodeList: ConsensusNodeInfo[]
} & SignedObject

export const activeNodescache: Map<string, SignedNodeList> = new Map()
export const fullNodesCache: Map<string, SignedNodeList> = new Map()
export const cacheUpdatedTimes: Map<string, number> = new Map()
export const realUpdatedTimes: Map<string, number> = new Map()

// METHODS

export function isEmpty(): boolean {
  return byPublicKey.size === 0
}

type Node = ConsensusNodeInfo | JoinedConsensor

export function addNodes(status: NodeStatus, nodes: Node[]): void {
  if (nodes.length === 0) return
  Logger.mainLogger.debug(`Adding ${status} nodes to the list`, nodes.length, nodes)

  for (const node of nodes) {

    // If node not in lists, add it
    // eslint-disable-next-line security/detect-object-injection
    if (!byPublicKey.has(node.publicKey)) {
      const key = node.publicKey
      switch (status) {
        case NodeStatus.SYNCING:
          if (standbyList.has(key)) standbyList.delete(key)
          if (activeList.has(key)) {
            activeList.delete(key)
            activeListByIdSorted = activeListByIdSorted.filter((node) => node.publicKey !== key)
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
      byPublicKey.set(node.publicKey, node)
      /* eslint-enable security/detect-object-injection */
    }

    // If an id is given, update its id
    if (node.id) {
      const entry = byPublicKey.get(node.publicKey)
      if (entry) {
        entry.id = node.id
        byId.set(node.id, node.publicKey)
      }
    }
  }
}
export function refreshNodes(status: NodeStatus, nodes: ConsensusNodeInfo[] | JoinedConsensor[]): void {
  if (nodes.length === 0) return
  Logger.mainLogger.debug('Refreshing nodes', nodes.length, nodes)
  for (const node of nodes) {

    // If node not in lists, add it
    // eslint-disable-next-line security/detect-object-injection
    if (!byPublicKey.has(node.publicKey)) {
      Logger.mainLogger.debug('adding new node during refresh', node.publicKey)
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

      byPublicKey.set(node.publicKey, node)
      // eslint-disable-next-line security/detect-object-injection
    }

    // If an id is given, update its id
    if (node.id) {
      const entry = byPublicKey.get(node.publicKey)
      if (entry) {
        entry.id = node.id
        byId.set(node.id, node.publicKey)
      }
    }
  }
}

export function removeNodes(publicKeys: string[]): void {
  if (publicKeys.length > 0) Logger.mainLogger.debug('Removing nodes', publicKeys)
  // Efficiently remove nodes from nodelist

  for (const key of publicKeys) {
    // eslint-disable-next-line security/detect-object-injection
    if (!byPublicKey.has(key)) {
      console.warn(`removeNodes: publicKey ${key} not in nodelist`)
      continue
    }
    /* eslint-disable security/detect-object-injection */
    syncingList.delete(key)
    activeList.delete(key)
    standbyList.delete(key)
    const id = byPublicKey.get(key).id
    activeListByIdSorted = activeListByIdSorted.filter((node) => node.id !== byPublicKey.get(key).id)
    byId.delete(id)
    byPublicKey.delete(key)
    /* eslint-enable security/detect-object-injection */
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

export function setStatus(status: Exclude<NodeStatus, NodeStatus.STANDBY>, publicKeys: string[]): void {
  if (publicKeys.length === 0) return
  Logger.mainLogger.debug(`Updating status ${status} for nodes`, publicKeys)
  for (const key of publicKeys) {
    // eslint-disable-next-line security/detect-object-injection
    const node = byPublicKey.get(key)
    if (node === undefined) {
      console.warn(`setStatus: publicKey ${key} not in nodelist`)
      continue
    }
    switch (status) {
      case NodeStatus.SYNCING:
        if (standbyList.has(key)) standbyList.delete(key)
        if (activeList.has(key)) {
          activeList.delete(key)
          activeListByIdSorted = activeListByIdSorted.filter((node) => node.publicKey !== key)
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
    }
  }
}

export function getFirstNode(): ConsensusNodeInfo | undefined {
  return byPublicKey.values().next().value;
}

export function getActiveList(id_sorted = true): ConsensusNodeInfo[] {
  if (id_sorted) return activeListByIdSorted
  return [...activeList.values()]
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
    const nodeList = nodeCount < 1 ? [getFirstNode()] : getRandomActiveNodes(nodeCount)
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
    realUpdatedTimes.set('/nodelist', Date.now())
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
  const activeNodeList = getActiveList(false)
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
    realUpdatedTimes.set('/full-nodelist', Date.now())
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
  const response = (await P2P.getJson(`http://${archiver.ip}:${archiver.port}/nodelist`)) as SignedNodeList
  Logger.mainLogger.debug('response', `http://${archiver.ip}:${archiver.port}/nodelist`, response)

  if (response && response.nodeList && response.nodeList.length > 0) {
    const isResponseVerified = Crypto.verify(response)
    const isFromActiveArchiver = State.activeArchivers.some(
      (archiver) => archiver.publicKey === response.sign.owner
    )

    if (!isResponseVerified || !isFromActiveArchiver) {
      Logger.mainLogger.debug(`Fail to verify the response from the archiver ${archiver.ip}:${archiver.port}`)
      return []
    }
    return response.nodeList
  } else Logger.mainLogger.debug(`Fail To get nodeList from the archiver ${archiver.ip}:${archiver.port}`)
  return []
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
export function clearNodeLists(): void {
  try {
    activeNodescache.clear()
    fullNodesCache.clear()
    activeList.clear()
    syncingList.clear()
    realUpdatedTimes.clear()
    cacheUpdatedTimes.clear()

    byId.clear()
    byPublicKey.clear()
    activeListByIdSorted = []
  } catch (e) {
    Logger.mainLogger.error('Error thrown in clearNodeListCache', e)
  }
}

export function toggleFirstNode(): void {
  foundFirstNode = !foundFirstNode
  Logger.mainLogger.debug('foundFirstNode', foundFirstNode)
}
