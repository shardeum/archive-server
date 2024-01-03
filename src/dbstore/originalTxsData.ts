import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'

export interface OriginalTxData {
  txId: string
  timestamp: number
  cycle: number
  originalTxData: any // eslint-disable-line @typescript-eslint/no-explicit-any
  sign: Signature | string
}

type DbOriginalTxData = OriginalTxData & {
  originalTxData: string
  sign: string
}

type OriginalTxDataCount = {
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
    await db.run(sql, values)
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
    await db.run(sql, values)
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
    originalTxsData = await db.get(sql, values)
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
  let dbOriginalTxsData: DbOriginalTxData[] = []
  const originalTxsData: OriginalTxData[] = []
  try {
    let sql = `SELECT * FROM originalTxsData`
    const sqlSuffix = ` ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    const values: number[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    sql += sqlSuffix
    dbOriginalTxsData = (await db.all(sql, values)) as DbOriginalTxData[]
    if (dbOriginalTxsData.length > 0) {
      for (let i = 0; i < dbOriginalTxsData.length; i++) {
        /* eslint-disable security/detect-object-injection */
        originalTxsData.push({
          txId: dbOriginalTxsData[i].txId,
          timestamp: dbOriginalTxsData[i].timestamp,
          cycle: dbOriginalTxsData[i].cycle,
          originalTxData: dbOriginalTxsData[i].originalTxData ? DeSerializeFromJsonString(dbOriginalTxsData[i].originalTxData) : null,
          sign: dbOriginalTxsData[i].sign ? DeSerializeFromJsonString(dbOriginalTxsData[i].sign) : null
        })
        /* eslint-enable security/detect-object-injection */
      }
    }

    } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData originalTxsData', originalTxsData)
  }
  return originalTxsData
}

export async function queryOriginalTxDataByTxId(txId: string): Promise<OriginalTxData> {
  try {
    const sql = `SELECT * FROM originalTxsData WHERE txId=?`
    const dbOriginalTxData: DbOriginalTxData = (await db.get(sql, [txId])) as DbOriginalTxData
    let originalTxData: OriginalTxData
    if (dbOriginalTxData) {
      originalTxData = {
        txId: dbOriginalTxData.txId,
        timestamp: dbOriginalTxData.timestamp,
        cycle: dbOriginalTxData.cycle,
        originalTxData: dbOriginalTxData.originalTxData ? DeSerializeFromJsonString(dbOriginalTxData.originalTxData) : null,
        sign: dbOriginalTxData.sign ? DeSerializeFromJsonString(dbOriginalTxData.sign) : null
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('OriginalTxData txId', originalTxData)
    }
    return originalTxData
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
    dbOriginalTxsDataCount = (await db.all(sql, [start, end])) as DbOriginalTxDataCount[]
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
    const dbOriginalTxsData: DbOriginalTxData[] = (await db.all(sql)) as DbOriginalTxData[]
    const originalTxsData: OriginalTxData[] = []
    if (dbOriginalTxsData.length > 0) {
      for (let i = 0; i < dbOriginalTxsData.length; i++) {
        /* eslint-disable security/detect-object-injection */
        originalTxsData.push({
          txId: dbOriginalTxsData[i].txId,
          timestamp: dbOriginalTxsData[i].timestamp,
          cycle: dbOriginalTxsData[i].cycle,
          originalTxData: dbOriginalTxsData[i].originalTxData ? DeSerializeFromJsonString(dbOriginalTxsData[i].originalTxData) : null,
          sign: dbOriginalTxsData[i].sign ? DeSerializeFromJsonString(dbOriginalTxsData[i].sign) : null
        })
        /* eslint-enable security/detect-object-injection */
      }
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
