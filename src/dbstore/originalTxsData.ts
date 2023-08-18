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
  originalTxData: any
  sign: Signature | string
}

type DbOriginalTxData = OriginalTxData & {
  originalTxData: string
  sign: string
}

export async function insertOriginalTxData(OriginalTxData: OriginalTxData) {
  try {
    const fields = Object.keys(OriginalTxData).join(', ')
    const placeholders = Object.keys(OriginalTxData).fill('?').join(', ')
    const values = extractValues(OriginalTxData)
    let sql = 'INSERT OR REPLACE INTO originalTxsData (' + fields + ') VALUES (' + placeholders + ')'
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

export async function bulkInsertOriginalTxsData(originalTxsData: OriginalTxData[]) {
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
    let values: any[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    originalTxsData = await db.get(sql, [])
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
  let originalTxsData
  try {
    let sql = `SELECT * FROM originalTxsData`
    let sqlSuffix = ` ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    let values: any[] = []
    if (startCycle && endCycle) {
      sql += ` WHERE cycle BETWEEN ? AND ?`
      values.push(startCycle, endCycle)
    }
    sql += sqlSuffix
    originalTxsData = await db.all(sql, values)
    originalTxsData.forEach((originalTxData: DbOriginalTxData) => {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
      if (originalTxData.sign) originalTxData.sign = DeSerializeFromJsonString(originalTxData.sign)
    })
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData originalTxsData', originalTxsData)
  }
  return originalTxsData
}

export async function queryOriginalTxDataByTxId(txId: string): Promise<OriginalTxData | null> {
  try {
    const sql = `SELECT * FROM originalTxsData WHERE txId=?`
    const originalTxData = (await db.get(sql, [txId])) as DbOriginalTxData
    if (originalTxData) {
      if (originalTxData.originalTxData)
        originalTxData.originalTxData = DeSerializeFromJsonString(originalTxData.originalTxData)
      if (originalTxData.sign) originalTxData.sign = DeSerializeFromJsonString(originalTxData.sign)
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

export async function queryOriginalTxDataCountByCycles(start: number, end: number) {
  let originalTxsData
  try {
    const sql = `SELECT cycle, COUNT(*) FROM originalTxsData GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    originalTxsData = await db.all(sql, [start, end])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('OriginalTxData count by cycle', originalTxsData)
  }
  if (originalTxsData.length > 0) {
    originalTxsData.forEach((OriginalTxData) => {
      OriginalTxData['originalTxsData'] = OriginalTxData['COUNT(*)']
      delete OriginalTxData['COUNT(*)']
    })
  }
  return originalTxsData
}

export async function queryLatestOriginalTxs(count: number) {
  try {
    const sql = `SELECT * FROM originalTxsData ORDER BY cycle DESC, timestamp DESC LIMIT ${count ? count : 100}`
    const originalTxs: any = await db.all(sql)
    if (originalTxs.length > 0) {
      originalTxs.forEach((tx: OriginalTxData) => {
        if (tx.originalTxData) tx.originalTxData = DeSerializeFromJsonString(tx.originalTxData)
        if (tx.sign) tx.sign = DeSerializeFromJsonString(tx.sign as string)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Latest Original-Tx: ', originalTxs)
    }
    return originalTxs
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}