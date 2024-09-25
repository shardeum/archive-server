import * as db from '../dbstore/sqlite3storage'
import { digesterDatabase } from '.'
import { config } from '../Config'

/**
 * TransactionDigest is for storing transaction digests, which is the hash of prevHash
 * and list of transactions in a timestamp range
 */
export interface TransactionDigest {
  cycleStart: number
  cycleEnd: number
  txCount: number
  hash: string
}

export async function insertTransactionDigest(txDigest: TransactionDigest): Promise<void> {
  try {
    const fields = Object.keys(txDigest).join(', ')
    const placeholders = Object.keys(txDigest).fill('?').join(', ')
    const values = db.extractValues(txDigest)
    const sql =
      'INSERT INTO txDigests (' +
      fields +
      ') VALUES (' +
      placeholders +
      ') ON CONFLICT (cycleEnd) DO UPDATE SET ' +
      'cycleStart = excluded.cycleStart, ' +
      'txCount = excluded.txCount, ' +
      'hash = excluded.hash'

    await db.run(digesterDatabase, sql, values)
    if (config.VERBOSE) {
      console.log(
        `Successfully inserted txDigest for cycle records from ${txDigest.cycleStart} to ${txDigest.cycleEnd}`
      )
    }
  } catch (e) {
    console.error(e)
    throw new Error(
      `Unable to insert txDigest for cycle records from ${txDigest.cycleStart} to ${txDigest.cycleEnd}`
    )
  }
}

export async function getLastProcessedTxDigest(): Promise<TransactionDigest> {
  try {
    const sql = `SELECT * FROM txDigests ORDER BY cycleEnd DESC LIMIT 1`
    const lastProcessedDigest = (await db.get(digesterDatabase, sql)) as TransactionDigest
    if (config.VERBOSE) {
      console.log('LastProcessed Tx Digest', lastProcessedDigest)
    }
    return lastProcessedDigest
  } catch (e) {
    console.error(e)
    return null
  }
}

export async function queryByEndCycle(endCycle: number): Promise<TransactionDigest> {
  try {
    const sql = `SELECT * FROM txDigests WHERE cycleEnd=? LIMIT 1`
    const txDigest = (await db.get(digesterDatabase, sql, [endCycle])) as TransactionDigest
    if (config.VERBOSE) {
      console.log('Tx Digest by endCycle', txDigest)
    }
    return txDigest
  } catch (e) {
    console.error(e)
    return null
  }
}

export async function queryByCycleRange(startCycle: number, endCycle: number): Promise<TransactionDigest[]> {
  try {
    const sql = `SELECT * FROM txDigests WHERE cycleStart >= ? AND cycleEnd <= ? ORDER BY cycleEnd`
    const txDigests = (await db.all(digesterDatabase, sql, [startCycle, endCycle])) as TransactionDigest[]
    if (config.VERBOSE) {
      console.log('Tx Digest by cycle range', txDigests)
    }
    return txDigests || []
  } catch (e) {
    console.error('Error fetching txDigests from DB: ', e)
    return []
  }
}

export async function queryLatestTxDigests(count: number): Promise<TransactionDigest[]> {
  try {
    const sql = `SELECT * FROM txDigests ORDER BY cycleEnd DESC LIMIT ?`
    const txDigests = (await db.all(digesterDatabase, sql, [count])) as TransactionDigest[]
    if (config.VERBOSE) {
      console.log('Latest Tx Digests', txDigests)
    }
    return txDigests || []
  } catch (e) {
    console.error('Error fetching latest tx digests from DB: ', e)
    return []
  }
}
