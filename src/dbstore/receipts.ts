import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'

export interface Receipt {
  receiptId: string
  tx: any
  cycle: number
  timestamp: number
  result: any
  accounts: any[]
  receipt: any
  sign: Signature
}

export async function insertReceipt(receipt: Receipt) {
  try {
    const fields = Object.keys(receipt).join(', ')
    const placeholders = Object.keys(receipt).fill('?').join(', ')
    const values = extractValues(receipt)
    let sql = 'INSERT OR REPLACE INTO receipts (' + fields + ') VALUES (' + placeholders + ')'
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

export async function bulkInsertReceipts(receipts: Receipt[]) {
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

export async function queryReceiptByReceiptId(receiptId: string) {
  try {
    const sql = `SELECT * FROM receipts WHERE receiptId=?`
    let receipt: any = await db.get(sql, [receiptId])
    if (receipt) {
      if (receipt.tx) receipt.tx = JSON.parse(receipt.tx)
      if (receipt.accounts) receipt.accounts = JSON.parse(receipt.accounts)
      if (receipt.receipt) receipt.receipt = JSON.parse(receipt.receipt)
      if (receipt.result) receipt.result = JSON.parse(receipt.result)
      if (receipt.sign) receipt.sign = JSON.parse(receipt.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt receiptId', receipt)
    }
    return receipt
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryLatestReceipts(count: number) {
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle DESC, timestamp DESC LIMIT ${count ? count : 100}`
    const receipts: any = await db.all(sql)
    if (receipts.length > 0) {
      receipts.map((receipt: any) => {
        if (receipt.tx) receipt.tx = JSON.parse(receipt.tx)
        if (receipt.accounts) receipt.accounts = JSON.parse(receipt.accounts)
        if (receipt.receipt) receipt.receipt = JSON.parse(receipt.receipt)
        if (receipt.result) receipt.result = JSON.parse(receipt.result)
        if (receipt.sign) receipt.sign = JSON.parse(receipt.sign)
        return receipt
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Receipt latest', receipts)
    }
    return receipts
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryReceipts(skip: number = 0, limit: number = 10000) {
  let receipts
  try {
    const sql = `SELECT * FROM receipts ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql)
    if (receipts.length > 0) {
      receipts.map((receipt: any) => {
        if (receipt.tx) receipt.tx = JSON.parse(receipt.tx)
        if (receipt.accounts) receipt.accounts = JSON.parse(receipt.accounts)
        if (receipt.receipt) receipt.receipt = JSON.parse(receipt.receipt)
        if (receipt.result) receipt.result = JSON.parse(receipt.result)
        if (receipt.sign) receipt.sign = JSON.parse(receipt.sign)
        return receipt
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

export async function queryReceiptCount() {
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

export async function queryReceiptCountByCycles(start: number, end: number) {
  let receipts
  try {
    const sql = `SELECT cycle, COUNT(*) FROM receipts GROUP BY cycle HAVING cycle BETWEEN ? AND ? ORDER BY cycle ASC`
    receipts = await db.all(sql, [start, end])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Receipt count by cycle', receipts)
  }
  if (receipts.length > 0) {
    receipts.forEach((receipt) => {
      receipt['receipts'] = receipt['COUNT(*)']
      delete receipt['COUNT(*)']
    })
  }
  return receipts
}

export async function queryReceiptCountBetweenCycles(startCycleNumber: number, endCycleNumber: number) {
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
  skip: number = 0,
  limit: number = 10000,
  startCycleNumber: number,
  endCycleNumber: number
) {
  let receipts
  try {
    const sql = `SELECT * FROM receipts WHERE cycle BETWEEN ? AND ? ORDER BY cycle ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    receipts = await db.all(sql, [startCycleNumber, endCycleNumber])
    if (receipts.length > 0) {
      receipts.map((receipt: any) => {
        if (receipt.tx) receipt.tx = JSON.parse(receipt.tx)
        if (receipt.accounts) receipt.accounts = JSON.parse(receipt.accounts)
        if (receipt.receipt) receipt.receipt = JSON.parse(receipt.receipt)
        if (receipt.result) receipt.result = JSON.parse(receipt.result)
        if (receipt.sign) receipt.sign = JSON.parse(receipt.sign)
        return receipt
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
