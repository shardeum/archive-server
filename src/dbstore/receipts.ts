import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'
import { ArchiverReceipt } from '../Data/Collector'

export interface Receipt extends ArchiverReceipt {
  receiptId: string
  timestamp: number
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
    let receipt = (await db.get(sql, [receiptId])) as DbReceipt
    if (receipt) {
      if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
      if (receipt.beforeStateAccounts)
        receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
      if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
      if (receipt.appReceiptData) receipt.appReceiptData = DeSerializeFromJsonString(receipt.appReceiptData)
      if (receipt.appliedReceipt) receipt.appliedReceipt = DeSerializeFromJsonString(receipt.appliedReceipt)
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
    const receipts = (await db.all(sql)) as DbReceipt[]
    if (receipts.length > 0) {
      receipts.forEach((receipt: any) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.appReceiptData) receipt.appReceiptData = DeSerializeFromJsonString(receipt.appReceiptData)
        if (receipt.appliedReceipt) receipt.appliedReceipt = DeSerializeFromJsonString(receipt.appliedReceipt)
      })
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
  let receipts: Receipt[] = []
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = (await db.all(sql)) as DbReceipt[]
    if (receipts.length > 0) {
      receipts.forEach((receipt: any) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.appReceiptData) receipt.appReceiptData = DeSerializeFromJsonString(receipt.appReceiptData)
        if (receipt.appliedReceipt) receipt.appliedReceipt = DeSerializeFromJsonString(receipt.appliedReceipt)
      })
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
  let receipts: Receipt[] = []
  try {
    const sql = `SELECT * FROM receipts WHERE cycle BETWEEN ? AND ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = (await db.all(sql, [startCycleNumber, endCycleNumber])) as DbReceipt[]
    if (receipts.length > 0) {
      receipts.forEach((receipt: any) => {
        if (receipt.tx) receipt.tx = DeSerializeFromJsonString(receipt.tx)
        if (receipt.beforeStateAccounts)
          receipt.beforeStateAccounts = DeSerializeFromJsonString(receipt.beforeStateAccounts)
        if (receipt.accounts) receipt.accounts = DeSerializeFromJsonString(receipt.accounts)
        if (receipt.appReceiptData) receipt.appReceiptData = DeSerializeFromJsonString(receipt.appReceiptData)
        if (receipt.appliedReceipt) receipt.appliedReceipt = DeSerializeFromJsonString(receipt.appliedReceipt)
      })
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
