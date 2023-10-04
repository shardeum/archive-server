/**
 * `verify` submodule. Defines functions used to verify objects against
 * given hashes.
 *
 * This module is functionally identical to the one in shardus-global-server/p2p/SyncV2/verify.ts.
 */

import { hashObj } from '@shardus/crypto-utils';
import { P2P, hexstring } from '@shardus/types'
import { err, ok, Result } from 'neverthrow'
import { Signature } from '../shardus-crypto-types'

type HashableObject = (object | string) & { sign?: Signature }

/**
 * Verifies if the hash of a given object matches the expected hash.
 *
 * This function hashes a 'HashableObject' and compares it to an expected hash value. If the hashes match, it returns a successful result with value true. If the hashes don't match, it returns an error with a detailed message describing the mismatch.
 *
 * @param object - The object to be hashed and verified.
 * @param expectedHash - The expected hash string to compare with the hash of the object.
 * @param [objectName='some object'] - An optional name for the object, used in the error message in case of a hash mismatch.
 *
 * @returns Returns a Result object. On successful hash verification, returns 'ok' with value true. On mismatch, returns 'err' with an Error object detailing the mismatch.
 */
function verify(
  object: HashableObject,
  expectedHash: hexstring,
  objectName = 'some object'
): Result<boolean, Error> {
  const newHash = hashObj(object)
  return newHash === expectedHash
    ? ok(true)
    : err(new Error(`hash mismatch for ${objectName}: expected ${expectedHash}, got ${newHash}`))
}

/** Verifies that the hash of the validator list matches the expected hash. */
export function verifyValidatorList(
  validatorList: P2P.NodeListTypes.Node[],
  expectedHash: hexstring
): Result<boolean, Error> {
  return verify(validatorList, expectedHash, 'validator list')
}

/** Verifies that the hash of the cycle record matches the expected hash. */
export function verifyCycleRecord(
  cycleRecord: P2P.CycleCreatorTypes.CycleRecord,
  expectedHash: hexstring
): Result<boolean, Error> {
  const actualHash = makeCycleMarker(cycleRecord)

  // verify that the hash of the CycleRecord matches the expected hash
  if (actualHash !== expectedHash)
    return err(new Error(`hash mismatch for cycle: expected ${expectedHash}, got ${actualHash}`))

  return ok(true)
}

/** 
  * Compute the cycle marker for a given cycle record. This function is (or should be)
  * identical to the one in shardus-global-server/p2p/CycleCreator.
  */
function makeCycleMarker(record: P2P.CycleCreatorTypes.CycleRecord): string {
  return hashObj(record)
}

