import * as Logger from '../Logger'

/**
 * SyncV2 a p2p module that contains all of the functionality for the new
 * Node List Sync v2.
 */

import { ResultAsync } from 'neverthrow'
import { hexstring, P2P } from '@shardus/types'
import {
  getCurrentCycleDataFromNode,
  robustQueryForCycleRecordHash,
  robustQueryForValidatorListHash,
  getValidatorListFromNode,
} from './queries'
import { ArchiverNodeInfo } from '../State'
import { getActiveNodeListFromArchiver } from '../NodeList'
import * as NodeList from '../NodeList'
import { Cycle as DbCycle } from '../dbstore/cycles'
import { Cycle } from '../Data/Cycles'

/**
 * Given a list of archivers, queries each one until one returns an active node list.
 *
 * The endpoint queried does not return a *full* list of nodes. It's a partial
 * list that will be enough to use in robust queries.
 */
async function getActiveListFromSomeArchiver(
  archivers: ArchiverNodeInfo[]
): Promise<P2P.SyncTypes.ActiveNode[]> {
  for (const archiver of archivers) {
    try {
      const nodeList = await getActiveNodeListFromArchiver(archiver)
      if (nodeList) {
        return nodeList
      }
    } catch (e) {
      console.warn(`failed to get active node list from archiver ${archiver.ip}:${archiver.port}: ${e}`)
      continue
    }
  }

  // all archivers have failed at this point
  throw new Error('no archiver could return an active node list')
}

/**
 * Synchronizes the NodeList and gets the latest CycleRecord from other validators.
 */
export function syncV2(activeArchivers: ArchiverNodeInfo[]): ResultAsync<Cycle, Error> {
  return ResultAsync.fromPromise(getActiveListFromSomeArchiver(activeArchivers), (e: Error) => e).andThen(
    (nodeList) =>
      syncValidatorList(nodeList).andThen((validatorList) =>
        syncLatestCycleRecordAndMarker(nodeList).map(([cycle, cycleMarker]) => {
          Logger.mainLogger.debug('syncV2: validatorList', validatorList)

          // validatorList needs to be transformed into a ConsensusNodeInfo[]
          const consensusNodeList: NodeList.ConsensusNodeInfo[] = validatorList.map((node) => ({
            publicKey: node.publicKey,
            ip: node.externalIp,
            port: node.externalPort,
            id: node.id,
          }))

          NodeList.addNodes(NodeList.Statuses.ACTIVE, cycleMarker, consensusNodeList)

          // return a cycle that we'll store in the database
          return {
            ...cycle,
            marker: cycleMarker,
            certificate: '',
          }
        })
      )
  )
}

/**
 * This function synchronizes a validator list from `activeNodes`.
 *
 * @param {P2P.SyncTypes.ActiveNode[]} activeNodes - An array of active nodes to be queried.
 * The function first performs a robust query for the latest node list hash.
 * After obtaining the hash, it retrieves the full node list from one of the winning nodes.
 *
 * @returns {ResultAsync<P2P.NodeListTypes.Node[], Error>} - A ResultAsync object. On success, it will contain
 * an array of Node objects, and on error, it will contain an Error object. The function is asynchronous
 * and can be awaited.
 */
function syncValidatorList(
  activeNodes: P2P.SyncTypes.ActiveNode[]
): ResultAsync<P2P.NodeListTypes.Node[], Error> {
  // run a robust query for the lastest node list hash
  return robustQueryForValidatorListHash(activeNodes).andThen(({ value, winningNodes }) =>
    // get full node list from one of the winning nodes
    getValidatorListFromNode(winningNodes[0], value.nodeListHash)
  )
}

/**
 * Synchronizes the latest cycle record from a list of active nodes.
 *
 * @param {P2P.SyncTypes.ActiveNode[]} activeNodes - An array of active nodes to be queried.
 * The function first performs a robust query for the latest cycle record hash.
 * After obtaining the hash, it retrieves the current cycle data from one of the winning nodes.
 *
 * @returns {ResultAsync<P2P.CycleCreatorTypes.CycleRecord, Error>} - A ResultAsync object.
 * On success, it will contain a CycleRecord object, and on error, it will contain an Error object.
 * The function is asynchronous and can be awaited.
 */
function syncLatestCycleRecordAndMarker(
  activeNodes: P2P.SyncTypes.ActiveNode[]
): ResultAsync<[P2P.CycleCreatorTypes.CycleRecord, hexstring], Error> {
  // run a robust query for the latest cycle record hash
  return robustQueryForCycleRecordHash(activeNodes).andThen(({ value: cycleRecordHash, winningNodes }) =>
    // get current cycle record from node
    getCurrentCycleDataFromNode(winningNodes[0], cycleRecordHash).map(
      (cycle) => [cycle, cycleRecordHash] as [P2P.CycleCreatorTypes.CycleRecord, hexstring]
    )
  )
}
