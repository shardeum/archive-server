import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'

export interface Receipt {
  receiptId: string
  tx: unknown
  cycle: number
  timestamp: number
  result: unknown
  beforeStateAccounts: unknown[]
  accounts: unknown[]
  receipt: unknown
  sign: Signature
}

type DbReceipt = Receipt & {
  tx: string
  result: string
  beforeStateAccounts: string
  accounts: string
  receipt: string
  sign: string
}

type ReceiptCount = {
  cycle: number
  receiptCount: number
}

type DbReceiptCount = ReceiptCount & {
  'COUNT(*)': number
}

export async function insertReceipt(receipt: Receipt): Promise<void> {
  try {
    const fields = Object.keys(receipt).join(', ')
    const placeholders = Object.keys(receipt).fill('?').join(', ')
    const values = extractValues(receipt)
    const sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted Receipt', receipt.receiptId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert Receipt or it is already stored in to database',
      receipt.receiptId
    )
  }
}

export async function bulkInsertReceipts(receipts: Receipt[]): Promise<void> {
  try {
    const fields = Object.keys(receipts[0]).join(', ')
    const placeholders = Object.keys(receipts[0]).fill('?').join(', ')
    const values = extractValuesFromArray(receipts)
    let sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < receipts.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Receipts', receipts.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Receipts', receipts.length)
  }
}

export async function queryReceiptByReceiptId(receiptId: string): Promise<Receipt> {
  try {
    const sql = `SELECT * FROM receipts WHERE receiptId=?`
    const dbReceipt: DbReceipt = (await db.get(sql, [receiptId])) as DbReceipt
    const receipt: Receipt = null
    if (dbReceipt) {
      if (dbReceipt.receiptId) receipt.receiptId = dbReceipt.receiptId
      if (dbReceipt.tx) receipt.tx = DeSerializeFromJsonString(dbReceipt.tx)
      if (dbReceipt.cycle) receipt.cycle = dbReceipt.cycle
      if (dbReceipt.timestamp) receipt.timestamp = dbReceipt.timestamp
      if (dbReceipt.beforeStateAccounts)
        receipt.beforeStateAccounts = DeSerializeFromJsonString(dbReceipt.beforeStateAccounts)
      if (dbReceipt.accounts) receipt.accounts = DeSerializeFromJsonString(dbReceipt.accounts)
      if (dbReceipt.receipt) receipt.receipt = DeSerializeFromJsonString(dbReceipt.receipt)
      if (dbReceipt.result) receipt.result = DeSerializeFromJsonString(dbReceipt.result)
      if (dbReceipt.sign) receipt.sign = DeSerializeFromJsonString(dbReceipt.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt receiptId', receipt)
    }
    return receipt
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryLatestReceipts(count: number): Promise<Receipt[]> {
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle DESC, timestamp DESC LIMIT ${count ? count : 100}`
    const dbReceipts: DbReceipt[] = (await db.all(sql)) as DbReceipt[]
    const receipts: Receipt[] = []
    if (dbReceipts.length > 0) {
      for (let i = 0; i < dbReceipts.length; i++) {
        /* eslint-disable security/detect-object-injection */
        receipts.push({
          receiptId: dbReceipts[i].receiptId,
          tx: dbReceipts[i].tx ? DeSerializeFromJsonString(dbReceipts[i].tx) : null,
          cycle: dbReceipts[i].cycle,
          timestamp: dbReceipts[i].timestamp,
          beforeStateAccounts: dbReceipts[i].beforeStateAccounts
            ? DeSerializeFromJsonString(dbReceipts[i].beforeStateAccounts)
            : null,
          accounts: dbReceipts[i].accounts ? DeSerializeFromJsonString(dbReceipts[i].accounts) : null,
          receipt: dbReceipts[i].receipt ? DeSerializeFromJsonString(dbReceipts[i].receipt) : null,
          result: dbReceipts[i].result ? DeSerializeFromJsonString(dbReceipts[i].result) : null,
          sign: dbReceipts[i].sign ? DeSerializeFromJsonString(dbReceipts[i].sign) : null,
        })
        /* eslint-enable security/detect-object-injection */
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt latest', receipts)
    }
    return receipts
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryReceipts(skip = 0, limit = 10000): Promise<Receipt[]> {
  let dbReceipts: DbReceipt[]
  const receipts: Receipt[] = []
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    dbReceipts = (await db.all(sql)) as DbReceipt[]
    if (dbReceipts.length > 0) {
      for (let i = 0; i < dbReceipts.length; i++) {
        /* eslint-disable security/detect-object-injection */
        receipts.push({
          receiptId: dbReceipts[i].receiptId,
          tx: dbReceipts[i].tx ? DeSerializeFromJsonString(dbReceipts[i].tx) : null,
          cycle: dbReceipts[i].cycle,
          timestamp: dbReceipts[i].timestamp,
          beforeStateAccounts: dbReceipts[i].beforeStateAccounts
            ? DeSerializeFromJsonString(dbReceipts[i].beforeStateAccounts)
            : null,
          accounts: dbReceipts[i].accounts ? DeSerializeFromJsonString(dbReceipts[i].accounts) : null,
          receipt: dbReceipts[i].receipt ? DeSerializeFromJsonString(dbReceipts[i].receipt) : null,
          result: dbReceipts[i].result ? DeSerializeFromJsonString(dbReceipts[i].result) : null,
          sign: dbReceipts[i].sign ? DeSerializeFromJsonString(dbReceipts[i].sign) : null,
        })
        /* eslint-enable security/detect-object-injection */
      }
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt receipts', receipts ? receipts.length : receipts, 'skip', skip)
  }
  return receipts
}

export async function queryReceiptCount(): Promise<number> {
  let receipts
  try {
    const sql = `SELECT COUNT(*) FROM receipts`
    receipts = await db.get(sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count', receipts)
  }
  if (receipts) receipts = receipts['COUNT(*)']
  else receipts = 0
  return receipts
}

export async function queryReceiptCountByCycles(start: number, end: number): Promise<ReceiptCount[]> {
  let receiptsCount: ReceiptCount[]
  let dbReceiptsCount: DbReceiptCount[]
  try {
    const sql = `SELECT cycle, COUNT(*) FROM receipts GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    dbReceiptsCount = (await db.all(sql, [start, end])) as DbReceiptCount[]
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count by cycle', dbReceiptsCount)
  }
  if (dbReceiptsCount.length > 0) {
    receiptsCount = dbReceiptsCount.map((dbReceipt) => {
      return {
        cycle: dbReceipt.cycle,
        receiptCount: dbReceipt['COUNT(*)'],
      }
    })
  }
  return receiptsCount
}

export async function queryReceiptCountBetweenCycles(
  startCycleNumber: number,
  endCycleNumber: number
): Promise<number> {
  let receipts
  try {
    const sql = `SELECT COUNT(*) FROM receipts WHERE cycle BETWEEN ? AND ?`
    receipts = await db.get(sql, [startCycleNumber, endCycleNumber])
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count between cycles', receipts)
  }
  if (receipts) receipts = receipts['COUNT(*)']
  else receipts = 0
  return receipts
}

export async function queryReceiptsBetweenCycles(
  skip = 0,
  limit = 10000,
  startCycleNumber: number,
  endCycleNumber: number
): Promise<Receipt[]> {
  let dbReceipts: DbReceipt[]
  const receipts: Receipt[] = []
  try {
    const sql = `SELECT * FROM receipts WHERE cycle BETWEEN ? AND ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    dbReceipts = (await db.all(sql, [startCycleNumber, endCycleNumber])) as DbReceipt[]
    if (dbReceipts.length > 0) {
      for (let i = 0; i < dbReceipts.length; i++) {
        /* eslint-disable security/detect-object-injection */
        receipts.push({
          receiptId: dbReceipts[i].receiptId,
          tx: dbReceipts[i].tx ? DeSerializeFromJsonString(dbReceipts[i].tx) : null,
          cycle: dbReceipts[i].cycle,
          timestamp: dbReceipts[i].timestamp,
          beforeStateAccounts: dbReceipts[i].beforeStateAccounts
            ? DeSerializeFromJsonString(dbReceipts[i].beforeStateAccounts)
            : null,
          accounts: dbReceipts[i].accounts ? DeSerializeFromJsonString(dbReceipts[i].accounts) : null,
          receipt: dbReceipts[i].receipt ? DeSerializeFromJsonString(dbReceipts[i].receipt) : null,
          result: dbReceipts[i].result ? DeSerializeFromJsonString(dbReceipts[i].result) : null,
          sign: dbReceipts[i].sign ? DeSerializeFromJsonString(dbReceipts[i].sign) : null,
        })
        /* eslint-enable security/detect-object-injection */
      }
    }
  } catch (e) {
    console.log(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      'Receipt receipts between cycles',
      receipts ? receipts.length : receipts,
      'skip',
      skip
    )
  }
  return receipts
}
