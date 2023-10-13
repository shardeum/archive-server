/**
 * `queries` submodule. Contains logic pertaining to anything that
 * requires an external node to be queried, including robust queries.
 *
 * Code is duplicated from shardus-global-server.
 */

import { hexstring, P2P } from '@shardus/types'
import { errAsync, ResultAsync } from 'neverthrow'
import { attempt, robustQuery } from '../Utils'
import { get } from '../P2P'

/** A successful RobustQueryResult whose value is an unwrapped `Ok` `Result`. */
type UnwrappedRobustResult<N, V> = {
  winningNodes: N[]
  value: V
}

// Convenience type aliases.
type ActiveNode = P2P.SyncTypes.ActiveNode
type Validator = P2P.NodeListTypes.Node
type Archiver = P2P.ArchiversTypes.JoinedArchiver
type CycleRecord = P2P.CycleCreatorTypes.CycleRecord

/** A `ResultAsync` that wraps an `UnwrappedRobustResult`. */
export type RobustQueryResultAsync<T> = ResultAsync<UnwrappedRobustResult<ActiveNode, T>, Error>

const MAX_RETRIES = 3
const REDUNDANCY = 3

async function throwableQueryFunction<T>(
  node: ActiveNode,
  endpointName: string,
  params: Record<string, string>
): Promise<T> {
  let url = `http://${node.ip}:${node.port}/${endpointName}`
  if (params) {
    const encodedParams = new URLSearchParams(params).toString()
    url += `?${encodedParams}`
  }

  const res = await get(url)

  if (res.ok) {
    return (await res.json()) as T
  } else {
    throw new Error(`get failed with status ${res.statusText}`)
  }
}

/**
 * Executes a robust query to a specified endpoint across multiple nodes, providing more fault tolerance.
 *
 * @param {ActiveNode[]} nodes - An array of active nodes to query.
 * @param {string} endpointName - The name of the endpoint to call on the active nodes.
 *
 * The function runs a robust query, logs the query, and ensures the result of the query is robust.
 * It makes an HTTP GET request to the specified endpoint for each node, and if the call fails,
 * an error message is returned.
 *
 * @returns {RobustQueryResultAsync<T>} - A ResultAsync object. On success, it contains a result object with
 * the winning nodes and the returned value. On failure, it contains an Error object. The function is asynchronous
 * and can be awaited.
 */
function makeRobustQueryCall<T>(nodes: ActiveNode[], endpointName: string): RobustQueryResultAsync<T> {
  // query function that makes the endpoint call as specified
  const queryFn = (node: ActiveNode): ResultAsync<T, Error> =>
    ResultAsync.fromPromise(
      throwableQueryFunction(node, endpointName, {}),
      (err) => new Error(`couldn't query ${endpointName}: ${err}`)
    )

  // run the robust query, wrapped in an async Result return the unwrapped result (with `map`) if successful
  const logPrefix = `syncv2-robust-query-${endpointName}`
  return ResultAsync.fromPromise(
    attempt(() => robustQuery(nodes, queryFn), {
      maxRetries: MAX_RETRIES,
      logPrefix,
    }),
    (err) => new Error(`robust query failed for ${endpointName}: ${err}`)
  ).andThen((robustResult) => {
    // ensure the result was robust as well.
    if (!(robustResult.count >= Math.min(REDUNDANCY, nodes.length))) {
      return errAsync(new Error(`result of ${endpointName} wasn't robust`))
    }
    return robustResult.value.map((value) => ({
      winningNodes: robustResult.nodes,
      value,
    }))
  })
}

/**
 * Performs a simple fetch operation from a given node to a specified endpoint. The operation is retried up to
 * MAX_RETRIES times in case of failure.
 *
 * @param {ActiveNode} node - An active node to fetch data from.
 * @param {string} endpointName - The name of the endpoint to fetch data from.
 *
 * The function attempts to make an HTTP GET request to the specified endpoint. If the call fails,
 * it retries the operation and returns an error message.
 *
 * @returns {ResultAsync<T, Error>} - A ResultAsync object. On success, it will contain the data fetched, and
 * on error, it will contain an Error object. The function is asynchronous and can be awaited.
 */
function attemptSimpleFetch<T>(
  node: ActiveNode,
  endpointName: string,
  params: Record<string, string>
): ResultAsync<T, Error> {
  return ResultAsync.fromPromise(
    attempt(async () => throwableQueryFunction(node, endpointName, params), {
      maxRetries: MAX_RETRIES,
      logPrefix: `syncv2-simple-fetch-${endpointName}`,
    }),
    (err) => new Error(`simple fetch failed for ${endpointName}: ${err}`)
  )
}

/** Executes a robust query to retrieve the cycle marker from the network. */
export function robustQueryForCycleRecordHash(nodes: ActiveNode[]): RobustQueryResultAsync<hexstring> {
  return makeRobustQueryCall(nodes, 'current-cycle-hash')
}

/** Executes a robust query to retrieve the validator list hash and next cycle timestamp from the network. */
export function robustQueryForValidatorListHash(
  nodes: ActiveNode[]
): RobustQueryResultAsync<{ nodeListHash: hexstring; nextCycleTimestamp: number }> {
  return makeRobustQueryCall(nodes, 'validator-list-hash')
}

/** Executes a robust query to retrieve the archiver list hash from the network. */
export function robustQueryForArchiverListHash(
  nodes: ActiveNode[]
): RobustQueryResultAsync<{ archiverListHash: hexstring }> {
  return makeRobustQueryCall(nodes, 'archiver-list-hash')
}

/** Retrives the entire last cycle from the node. */
export function getCurrentCycleDataFromNode(
  node: ActiveNode,
  expectedMarker: hexstring
): ResultAsync<CycleRecord, Error> {
  return attemptSimpleFetch(node, 'cycle-by-marker', {
    marker: expectedMarker,
  })
}

/** Gets the full validator list from the specified node. */
export function getValidatorListFromNode(
  node: ActiveNode,
  expectedHash: hexstring
): ResultAsync<Validator[], Error> {
  console.log(`getting validator list from ${node.ip}:${node.port} with hash ${expectedHash}`)
  return attemptSimpleFetch(node, 'validator-list', {
    hash: expectedHash,
  })
}

/** Gets the full node list from the specified archiver. */
export function getArchiverListFromNode(
  node: ActiveNode,
  expectedHash: hexstring
): ResultAsync<Archiver[], Error> {
  console.log(`getting archiver list from ${node.ip}:${node.port} with hash ${expectedHash}`)
  return attemptSimpleFetch(node, 'archiver-list', {
    hash: expectedHash,
  })
}
