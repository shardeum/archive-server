// import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { originalTxDataDatabase, extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'

export interface OriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: object // eslint-disable-line @typescript-eslint/no-explicit-any
  // sign: Signature
}

type DbOriginalTxData = OriginalTxData & {
  originalTxData: string
  // sign: string
}

export interface OriginalTxDataCount {
  cycle: number
  originalTxDataCount: number
}

type DbOriginalTxDataCount = OriginalTxDataCount & {
  'COUNT(*)': number
}

export async function insertOriginalTxData(OriginalTxData: OriginalTxData): Promise<void> {
  try {
    const fields = Object.keys(OriginalTxData).join(', ')
    const placeholders = Object.keys(OriginalTxData).fill('?').join(', ')
    const values = extractValues(OriginalTxData)
    const sql = 'INSERT OR REPLACE INTO originalTxsData (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(originalTxDataDatabase, sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted OriginalTxData', OriginalTxData.txId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert OriginalTxData or it is already stored in to database',
      OriginalTxData.txId
    )
  }
}

export async function bulkInsertOriginalTxsData(originalTxsData: OriginalTxData[]): Promise<void> {
  try {
    const fields = Object.keys(originalTxsData[0]).join(', ')
    const placeholders = Object.keys(originalTxsData[0]).fill('?').join(', ')
    const values = extractValuesFromArray(originalTxsData)
    let sql = 'INSERT OR REPLACE INTO originalTxsData (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < originalTxsData.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(originalTxDataDatabase, sql, values)
    if (config.VERBOSE)
      Logger.mainLogger.debug('Successfully inserted OriginalTxsData', originalTxsData.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert OriginalTxsData', originalTxsData.length)
  }
}

export async function queryOriginalTxDataCount(startCycle?: number, endCycle?: number): Promise<number> {
  let originalTxsData
  try {
    let sql = `SELECT COUNT(*) FROM originalTxsData`
    const values: number[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    originalTxsData = await db.get(originalTxDataDatabase, sql, values)
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData count', originalTxsData)
  }
  return originalTxsData['COUNT(*)'] || 0
}

export async function queryOriginalTxsData(
  skip = 0,
  limit = 10,
  startCycle?: number,
  endCycle?: number
): Promise<OriginalTxData[]> {
  let originalTxsData: DbOriginalTxData[] = []
  try {
    let sql = `SELECT * FROM originalTxsData`
    const sqlSuffix = ` ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    const values: number[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    sql += sqlSuffix
    originalTxsData = (await db.all(originalTxDataDatabase, sql, values)) as DbOriginalTxData[]
    originalTxsData.forEach((originalTxData: DbOriginalTxData) => {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
      // if (originalTxData.sign) originalTxData.sign = DeSerializeFromJsonString(originalTxData.sign)
    })
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData originalTxsData', originalTxsData)
  }
  return originalTxsData
}

export async function queryOriginalTxDataByTxId(txId: string, timestamp = 0): Promise<OriginalTxData> {
  try {
    const sql = `SELECT * FROM originalTxsData WHERE txId=?` + (timestamp ? ` AND timestamp=?` : '')
    const value = timestamp ? [txId, timestamp] : [txId]
    const originalTxData = (await db.get(originalTxDataDatabase, sql, value)) as DbOriginalTxData
    if (originalTxData) {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
      // if (originalTxData.sign) originalTxData.sign = DeSerializeFromJsonString(originalTxData.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('OriginalTxData txId', originalTxData)
    }
    return originalTxData as OriginalTxData
  } catch (e) {
    console.log(e)
  }
  return null
}

export async function queryOriginalTxDataCountByCycles(
  start: number,
  end: number
): Promise<OriginalTxDataCount[]> {
  const originalTxsDataCount: OriginalTxDataCount[] = []
  let dbOriginalTxsDataCount: DbOriginalTxDataCount[] = []
  try {
    const sql = `SELECT cycle, COUNT(*) FROM originalTxsData GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    dbOriginalTxsDataCount = (await db.all(originalTxDataDatabase, sql, [
      start,
      end,
    ])) as DbOriginalTxDataCount[]
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData count by cycle', dbOriginalTxsDataCount)
  }
  if (dbOriginalTxsDataCount.length > 0) {
    for (let i = 0; i < dbOriginalTxsDataCount.length; i++) {
      /* eslint-disable security/detect-object-injection */
      originalTxsDataCount.push({
        cycle: dbOriginalTxsDataCount[i].cycle,
        originalTxDataCount: dbOriginalTxsDataCount[i]['COUNT(*)'],
      })
      /* eslint-enable security/detect-object-injection */
    }
  }
  return originalTxsDataCount
}

export async function queryLatestOriginalTxs(count: number): Promise<OriginalTxData[]> {
  try {
    const sql = `SELECT * FROM originalTxsData ORDER BY cycle DESC, timestamp DESC LIMIT ${
      count ? count : 100
    }`
    const originalTxsData = (await db.all(originalTxDataDatabase, sql)) as DbOriginalTxData[]
    if (originalTxsData.length > 0) {
      originalTxsData.forEach((tx: DbOriginalTxData) => {
        if (tx.originalTxData) tx.originalTxData = DeSerializeFromJsonString(tx.originalTxData)
        // if (tx.sign) tx.sign = DeSerializeFromJsonString(tx.sign)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Latest Original-Tx: ', originalTxsData)
    }
    return originalTxsData
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}
