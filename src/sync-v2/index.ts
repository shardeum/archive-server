/**
 * SyncV2 a p2p module that contains all of the functionality for the new
 * Node List Sync v2.
 */

import { okAsync, errAsync, ResultAsync } from 'neverthrow'
import { hexstring, P2P } from '@shardus/types'
import {
  getCurrentCycleDataFromNode,
  robustQueryForCycleRecordHash,
  robustQueryForValidatorListHash,
  getValidatorListFromNode,
  robustQueryForStandbyNodeListHash,
  getStandbyNodeListFromNode,
} from './queries'
import { ArchiverNodeInfo } from '../State'
import { getActiveNodeListFromArchiver } from '../NodeList'
import * as NodeList from '../NodeList'
import { Cycle } from '../Data/Cycles'
import { verifyCycleRecord, verifyStandbyList, verifyValidatorList } from './verify'
import * as Logger from '../Logger'

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
  return ResultAsync.fromPromise(
    getActiveListFromSomeArchiver(activeArchivers),
    (e: Error) => e
  ).andThen(
    (nodeList) =>
      syncValidatorList(nodeList).andThen(([validatorList, validatorListHash]) =>
        syncStandbyNodeList(nodeList).andThen(([standbyList, standbyListHash]) =>
          syncLatestCycleRecordAndMarker(nodeList).andThen(([cycle, cycleMarker]) => {
            Logger.mainLogger.debug('syncV2: validatorList', validatorList)

            // additional checks to make sure the list hashes in the cycle
            // matches the hash for the validator list retrieved earlier
            if (cycle.nodeListHash !== validatorListHash) {
              return errAsync(
                new Error(
                  `validator list hash from received cycle (${cycle.nodeListHash}) does not match the hash received from robust query (${validatorListHash})`
                )
              )
            }
            if (cycle.standbyNodeListHash !== standbyListHash) {
              return errAsync(
                new Error(
                  `standby list hash from received cycle (${cycle.nodeListHash}) does not match the hash received from robust query (${validatorListHash})`
                )
              )
            }

            // validatorList and standbyList need to be transformed into a ConsensusNodeInfo[]
            const consensusNodeList: NodeList.ConsensusNodeInfo[] = validatorList.map((node) => ({
              publicKey: node.publicKey,
              ip: node.externalIp,
              port: node.externalPort,
              id: node.id,
            }))
            const standbyNodeList: NodeList.ConsensusNodeInfo[] = standbyList.map((joinRequest) => ({
              publicKey: joinRequest.nodeInfo.publicKey,
              ip: joinRequest.nodeInfo.externalIp,
              port: joinRequest.nodeInfo.externalPort,
            }))
            NodeList.addNodes(NodeList.Statuses.SYNCING, cycleMarker, consensusNodeList)
            NodeList.addNodes(NodeList.Statuses.STANDBY, cycleMarker, standbyNodeList)

            // return a cycle that we'll store in the database
            return okAsync({
              ...cycle,
              marker: cycleMarker,
              certificate: ''
            })
          })))
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
): ResultAsync<[P2P.NodeListTypes.Node[], hexstring], Error> {
  // run a robust query for the lastest node list hash
  return robustQueryForValidatorListHash(activeNodes).andThen(({ value, winningNodes }) =>
    // get full node list from one of the winning nodes
    getValidatorListFromNode(winningNodes[0], value.nodeListHash).andThen((validatorList) =>
      verifyValidatorList(validatorList, value.nodeListHash).map(() =>
        [validatorList, value.nodeListHash] as [P2P.NodeListTypes.Node[], hexstring])))
}

/**
 * This function synchronizes a standby node list from `activeNodes`.
 *
 * @param {P2P.SyncTypes.ActiveNode[]} activeNodes - An array of active nodes to be queried.
 * The function first performs a robust query for the latest node list hash.
 * After obtaining the hash, it retrieves the full node list from one of the winning nodes.
 *
 * @returns {ResultAsync<P2P.NodeListTypes.Node[], Error>} - A ResultAsync object. On success, it will contain
 * an array of Node objects, and on error, it will contain an Error object. The function is asynchronous
 * and can be awaited.
 */
function syncStandbyNodeList(
  activeNodes: P2P.SyncTypes.ActiveNode[]
): ResultAsync<[P2P.JoinTypes.JoinRequest[], hexstring], Error> {
  // run a robust query for the lastest archiver list hash
  return robustQueryForStandbyNodeListHash(activeNodes).andThen(({ value, winningNodes }) =>
    // get full standby list from one of the winning nodes
    getStandbyNodeListFromNode(winningNodes[0], value.standbyNodeListHash).andThen((standbyList) =>
      verifyStandbyList(standbyList, value.standbyNodeListHash).map(() =>
        [standbyList, value.standbyNodeListHash] as [P2P.JoinTypes.JoinRequest[], hexstring])))
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
    getCurrentCycleDataFromNode(winningNodes[0], cycleRecordHash).andThen((cycleRecord) =>
      verifyCycleRecord(cycleRecord, cycleRecordHash).map(() =>
        [cycleRecord, cycleRecordHash] as [P2P.CycleCreatorTypes.CycleRecord, hexstring])))
}
