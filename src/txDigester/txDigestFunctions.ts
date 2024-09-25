import { config } from '../Config'
import * as processedTxs from '../dbstore/processedTxs'
import * as txDigest from './txDigests'
import * as Crypto from '../Crypto'

let lastProcessedTxDigest: txDigest.TransactionDigest = null

export interface txDigestObj {
  prevHash: string
  txIdsHash: string
}

export const getTxIds = async (startCycle: number, endCycle: number): Promise<string[]> => {
  const sortedTxIds = await processedTxs.querySortedTxsBetweenCycleRange(startCycle, endCycle)
  return sortedTxIds
}

export const getHash = async (cycle: number): Promise<string> => {
  if (cycle == -1) {
    return '0x0'
  }
  const txDigestHash = await txDigest.queryByEndCycle(cycle)
  if (!txDigestHash) {
    throw new Error(`Failed to fetch txDigestHash for cycle ${cycle}`)
  }

  return txDigestHash.hash
}

export const updateLastProcessedTxDigest = async (): Promise<void> => {
  lastProcessedTxDigest = await txDigest.getLastProcessedTxDigest()
}

export const getLastProcessedTxDigest = async (): Promise<txDigest.TransactionDigest> => {
  if (!lastProcessedTxDigest) {
    await updateLastProcessedTxDigest()
  }
  return lastProcessedTxDigest
}

export const processAndInsertTxDigests = async (
  lastCheckedCycle: number,
  latestCycleCounter: number
): Promise<void> => {
  console.log('Processing and inserting txDigests from cycle: ', lastCheckedCycle, ' to ', latestCycleCounter)
  const batchSize = config.txDigest.cycleDiff
  let currentCycle = lastCheckedCycle
  let endCycle = currentCycle + batchSize - 1

  while (endCycle <= latestCycleCounter) {
    console.log(`Processing txDigests from cycle ${currentCycle} to ${endCycle}`)

    // Fetch txDigests in the current batch
    const txIds = await getTxIds(currentCycle, endCycle)
    if (txIds == null) {
      console.error(`Failed to fetch txIds for cycle ${currentCycle} to ${endCycle}`)
      return
    }

    if(config.VERBOSE) {
      console.log(`TxIds from ${currentCycle} to ${endCycle} of length ${txIds.length}: `, txIds)
    }

    const prevHash = await getHash(currentCycle - 1)
    console.log(`prevHash for cycle ${currentCycle}: `, prevHash)

    const txObj: txDigestObj = {
      prevHash: prevHash,
      txIdsHash: Crypto.hashObj(txIds),
    }

    const txRangeHash = Crypto.hashObj(txObj)

    const txDigestObj: txDigest.TransactionDigest = {
      cycleStart: currentCycle,
      cycleEnd: endCycle,
      txCount: txIds.length,
      hash: txRangeHash,
    }

    try {
      txDigest.insertTransactionDigest(txDigestObj)
    } catch (e) {
      console.error('Failed to insert txDigestObj: ', txDigestObj)
      console.error(e)
      return
    }

    currentCycle = endCycle + 1
    endCycle = currentCycle + batchSize - 1
    lastProcessedTxDigest = txDigestObj
  }

  console.log('Updated lastProcessedTxDigest: ', lastProcessedTxDigest)
  console.log('Finished processing txDigests.')
}

export const getTxDigestsForACycleRange = async (
  cycleStart: number,
  cycleEnd: number
): Promise<txDigest.TransactionDigest[]> => {
  const txDigests: txDigest.TransactionDigest[] = await txDigest.queryByCycleRange(cycleStart, cycleEnd)
  return txDigests
}

export const getLatestTxDigests = async (
  count: number
): Promise<txDigest.TransactionDigest[]> => {
  const txDigests: txDigest.TransactionDigest[] = await txDigest.queryLatestTxDigests(count)
  return txDigests
}
