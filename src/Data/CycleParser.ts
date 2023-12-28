import * as NodeList from '../NodeList'
import { P2P } from '@shardus/types'

export enum NodeStatus {
  ACTIVE = 'active',
  SYNCING = 'syncing',
  REMOVED = 'removed',
}

type OptionalExceptFor<T, TRequired extends keyof T> = Partial<T> & Pick<T, TRequired>

export interface Node extends NodeList.JoinedConsensor {
  curvePublicKey: string
  status: NodeStatus
}

export type Update = OptionalExceptFor<Node, 'id'>

export interface Change {
  added: NodeList.JoinedConsensor[] // order joinRequestTimestamp [OLD, ..., NEW]
  removed: Array<string> // order doesn't matter
  updated: Update[] // order doesn't matter
}

export function reversed<T>(thing: Iterable<T>): Iterable<T> {
  const arr = Array.isArray(thing) ? thing : Array.from(thing)
  let i = arr.length - 1

  const reverseIterator = {
    next: (): IteratorResult<T> => {
      const done = i < 0
      // eslint-disable-next-line security/detect-object-injection
      const value = done ? undefined : arr[i]
      i--
      return { value, done }
    },
  }
  return {
    [Symbol.iterator]: () => reverseIterator,
  }
}

export class ChangeSquasher {
  final: Change
  removedIds: Set<P2P.P2PTypes.NodeInfo['id']>
  seenUpdates: Map<Update['id'], Update>
  addedIds: Set<P2P.P2PTypes.NodeInfo['id']>
  constructor() {
    this.final = {
      added: [],
      removed: [],
      updated: [],
    }
    this.addedIds = new Set()
    this.removedIds = new Set()
    this.seenUpdates = new Map()
  }

  addChange(change: Change): void {
    for (const id of change.removed) {
      // Ignore if id is already removed
      if (this.removedIds.has(id)) continue
      // Mark this id as removed
      this.removedIds.add(id)
    }

    for (const update of change.updated) {
      // Ignore if update.id is already removed
      if (this.removedIds.has(update.id)) continue
      // Skip if it's already seen in the update
      if (this.seenUpdates.has(update.id)) continue
      // Mark this id as updated
      this.seenUpdates.set(update.id, update)
      // console.log('seenUpdates', this.seenUpdates, update)
    }

    for (const joinedConsensor of reversed(change.added)) {
      // Ignore if it's already been added
      if (this.addedIds.has(joinedConsensor.id)) continue

      // Ignore if joinedConsensor.id is already removed
      if (this.removedIds.has(joinedConsensor.id)) {
        continue
      }
      // Check if this id has updates
      const update = this.seenUpdates.get(joinedConsensor.id)
      if (update) {
        // If so, put them into final.updated
        this.final.updated.unshift(update)
        this.seenUpdates.delete(joinedConsensor.id)
      }
      // Add joinedConsensor to final.added
      this.final.added.unshift(joinedConsensor)
      // Mark this id as added
      this.addedIds.add(joinedConsensor.id)
    }
  }
}

export function parseRecord(record: P2P.CycleCreatorTypes.CycleRecord): Change {
  // For all nodes described by activated, make an update to change their status to active
  const activated = record.activated.map((id: string) => ({
    id,
    activeTimestamp: record.start,
    status: NodeStatus.ACTIVE,
  }))

  const refreshAdded: Change['added'] = []
  const refreshUpdated: Change['updated'] = []
  for (const refreshed of record.refreshedConsensors) {
    // const node = NodeList.nodes.get(refreshed.id)
    const node = NodeList.getNodeInfoById(refreshed.id) as NodeList.JoinedConsensor
    if (node) {
      // If it's in our node list, we update its counterRefreshed
      // (IMPORTANT: update counterRefreshed only if its greater than ours)
      if (record.counter > node.counterRefreshed) {
        refreshUpdated.push({
          id: refreshed.id,
          counterRefreshed: record.counter,
        })
      }
    } else {
      // If it's not in our node list, we add it...
      refreshAdded.push(NodeList.fromP2PTypesNode(refreshed))
      // and immediately update its status to ACTIVE
      // (IMPORTANT: update counterRefreshed to the records counter)
      refreshUpdated.push({
        id: refreshed.id,
        status: NodeStatus.ACTIVE,
        counterRefreshed: record.counter,
      })
    }
  }
  // Logger.mainLogger.debug('parseRecord', record.counter, {
  //   added: [...record.joinedConsensors],
  //   removed: [...record.apoptosized],
  //   updated: [...activated, ...refreshUpdated],
  // })

  const added = (record.joinedConsensors || []).map((joinedConsensor: P2P.JoinTypes.JoinedConsensor) =>
    NodeList.fromP2PTypesJoinedConsensor(joinedConsensor)
  )

  return {
    added: [...added],
    removed: [...record.apoptosized, ...record.removed, ...record.appRemoved],
    updated: [...activated, ...refreshUpdated],
  }
}

export function parse(record: P2P.CycleCreatorTypes.CycleRecord): Change {
  const changes = parseRecord(record)
  // const mergedChange = deepmerge.all<Change>(changes)
  // return mergedChange
  return changes
}

export function applyNodeListChange(change: Change): void {
  // console.log('change', change)
  if (change.added.length > 0) {
    const nodesBycycleJoined: { [cycleJoined: number]: NodeList.JoinedConsensor[] } = {}
    for (const node of change.added) {
      const joinedConsensor: NodeList.JoinedConsensor = node
      const consensorInfo: object = {
        ip: joinedConsensor.externalIp,
        port: joinedConsensor.externalPort,
        publicKey: joinedConsensor.publicKey,
        id: joinedConsensor.id,
      }
      if (!nodesBycycleJoined[joinedConsensor.cycleJoined]) {
        nodesBycycleJoined[joinedConsensor.cycleJoined] = [consensorInfo]
      } else nodesBycycleJoined[joinedConsensor.cycleJoined].push(consensorInfo)
    }

    for (const cycleJoined in nodesBycycleJoined) {
      // eslint-disable-next-line security/detect-object-injection
      NodeList.addNodes(NodeList.NodeStatus.SYNCING, cycleJoined, nodesBycycleJoined[cycleJoined])
    }
  }
  // This is not needed though since no removed nodes are ever added to this list
  // If we ever add removed nodes to this list, we need to update to removeNodes by publicKey instead of id
  // Commenting out for now
  // if (change.removed.length > 0) {
  //   NodeList.removeNodes(change.removed)
  // }
  if (change.updated.length > 0) {
    const activatedPublicKeys = change.updated.reduce((keys: string[], update: Update) => {
      const nodeInfo = NodeList.getNodeInfoById(update.id)
      if (nodeInfo) {
        keys.push(nodeInfo.publicKey)
      }
      return keys
    }, [])
    NodeList.setStatus(NodeList.NodeStatus.ACTIVE, activatedPublicKeys)
  }
}
export function activeNodeCount(cycle: P2P.CycleCreatorTypes.CycleRecord): number {
  return (
    cycle.active +
    cycle.activated.length +
    -cycle.apoptosized.length +
    -cycle.removed.length +
    -cycle.appRemoved.length +
    -cycle.lost.length
  )
}

export function totalNodeCount(cycle: P2P.CycleCreatorTypes.CycleRecord): number {
  return (
    cycle.syncing +
    cycle.joinedConsensors.length +
    cycle.active +
    //    cycle.activated.length -      // don't count activated because it was already counted in syncing
    -cycle.apoptosized.length +
    -cycle.removed.length +
    -cycle.appRemoved.length
    // -cycle.lost.length
  )
}
