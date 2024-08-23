import * as db from './sqlite3storage'
import { processedTxDatabase, extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'

/**
 * ProcessedTransaction stores transactions which have a receipt
 */
export interface ProcessedTransaction {
  txId: string
  cycle: number
  txTimestamp: number
  txApplyTimestamp: number
}

export async function insertProcessedTx(processedTx: ProcessedTransaction): Promise<void> {
  try {
    const fields = Object.keys(processedTx).join(', ')
    const placeholders = Object.keys(processedTx).fill('?').join(', ')
    const values = extractValues(processedTx)
    const sql = 'INSERT OR REPLACE INTO processedTxs (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(processedTxDatabase, sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted ProcessedTransaction', processedTx.txId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert ProcessedTransaction or it is already stored in to database',
      processedTx.txId
    )
  }
}

export async function bulkInsertProcessedTxs(processedTxs: ProcessedTransaction[]): Promise<void> {
  try {
    const fields = Object.keys(processedTxs[0]).join(', ')
    const placeholders = Object.keys(processedTxs[0]).fill('?').join(', ')
    const values = extractValuesFromArray(processedTxs)
    let sql = 'INSERT OR REPLACE INTO processedTxs (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < processedTxs.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(processedTxDatabase, sql, values)
    if (config.VERBOSE)
      Logger.mainLogger.debug('Successfully inserted ProcessedTransaction', processedTxs.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert ProcessedTransaction', processedTxs.length)
  }
}

export async function queryProcessedTxByTxId(txId: string): Promise<ProcessedTransaction> {
  try {
    const sql = `SELECT * FROM processedTxs WHERE txId=?`
    const processedTx = (await db.get(processedTxDatabase, sql, [txId])) as ProcessedTransaction
    if (config.VERBOSE) {
      Logger.mainLogger.debug('ProcessedTransaction txId', processedTx)
    }
    return processedTx
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryProcessedTxsByCycleNumber(cycleNumber: number): Promise<ProcessedTransaction[]> {
  try {
    const sql = `SELECT * FROM processedTxs WHERE cycle=?`
    const processedTxs = (await db.all(processedTxDatabase, sql, [cycleNumber])) as ProcessedTransaction[]
    if (config.VERBOSE) {
      Logger.mainLogger.debug(`ProcessedTransactions for cycle: ${cycleNumber} ${processedTxs.length}`)
    }
    return processedTxs
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function querySortedTxsBetweenCycleRange(
  startCycle: number,
  endCycle: number
): Promise<string[]> {
  try {
    const sql = `SELECT txId FROM processedTxs WHERE cycle BETWEEN ? AND ?`
    const txIds = (await db.all(processedTxDatabase, sql, [startCycle, endCycle])) as string[]
    if (config.VERBOSE) {
      Logger.mainLogger.debug(`txIds between ${startCycle} and ${endCycle} are ${txIds ? txIds.length : 0}`)
    }

    if (!txIds) {
      return []
    }

    txIds.sort()
    return txIds
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}
