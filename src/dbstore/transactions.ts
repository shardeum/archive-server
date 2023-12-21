import { Signature } from 'shardus-crypto-types'
import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString } from '../utils/serialization'
import { OriginalTxData } from './originalTxsData'

export interface Transaction {
  txId: string
  accountId: string
  timestamp: number
  cycleNumber: number
  data: unknown
  originalTxData: OriginalTxData
}

type DbTransaction = Transaction & {
  data: string
  keys: string
  result: string
  originalTxData: string
  sign: string
}

export interface TxResult {
  txIdShort: string
  txResult: string
}

export interface TxRaw {
  tx: {
    raw: string
    timestamp: number
  }
}

export async function insertTransaction(transaction: Transaction): Promise<void> {
  try {
    const fields = Object.keys(transaction).join(', ')
    const placeholders = Object.keys(transaction).fill('?').join(', ')
    const values = extractValues(transaction)
    const sql = 'INSERT OR REPLACE INTO transactions (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted Transaction', transaction.txId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert Transaction or it is already stored in to database',
      transaction.txId
    )
  }
}

export async function bulkInsertTransactions(transactions: Transaction[]): Promise<void> {
  try {
    const fields = Object.keys(transactions[0]).join(', ')
    const placeholders = Object.keys(transactions[0]).fill('?').join(', ')
    const values = extractValuesFromArray(transactions)
    let sql = 'INSERT OR REPLACE INTO transactions (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < transactions.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Transactions', transactions.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Transactions', transactions.length)
  }
}

export async function queryTransactionByTxId(txId: string): Promise<Transaction> {
  try {
    const sql = `SELECT * FROM transactions WHERE txId=?`
    const transaction = (await db.get(sql, [txId])) as DbTransaction // TODO: confirm structure of object from db
    if (transaction) {
      if (transaction.data) transaction.data = DeSerializeFromJsonString(transaction.data)
      if (transaction.result) transaction.result = DeSerializeFromJsonString(transaction.result)
      if (transaction.originalTxData)
        transaction.originalTxData = DeSerializeFromJsonString(transaction.originalTxData)
      if (transaction.sign) transaction.sign = DeSerializeFromJsonString(transaction.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Transaction txId', transaction)
    }
    return transaction
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryTransactionByAccountId(accountId: string): Promise<Transaction> {
  try {
    const sql = `SELECT * FROM transactions WHERE accountId=?`
    const transaction = (await db.get(sql, [accountId])) as DbTransaction // TODO: confirm structure of object from db
    if (transaction) {
      if (transaction.data) transaction.data = DeSerializeFromJsonString(transaction.data)
      if (transaction.result) transaction.result = DeSerializeFromJsonString(transaction.result)
      if (transaction.originalTxData)
        transaction.originalTxData = DeSerializeFromJsonString(transaction.originalTxData)
      if (transaction.sign) transaction.sign = DeSerializeFromJsonString(transaction.sign)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Transaction accountId', transaction)
    }
    return transaction
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryLatestTransactions(count: number): Promise<Transaction[]> {
  try {
    const sql = `SELECT * FROM transactions ORDER BY cycleNumber DESC, timestamp DESC LIMIT ${
      count ? count : 100
    }`
    const transactions = (await db.all(sql)) as DbTransaction[] // TODO: confirm structure of object from db
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction) => {
        if (transaction.data) transaction.data = DeSerializeFromJsonString(transaction.data)
        if (transaction.result) transaction.result = DeSerializeFromJsonString(transaction.result)
        if (transaction.originalTxData)
          transaction.originalTxData = DeSerializeFromJsonString(transaction.originalTxData)
        if (transaction.sign) transaction.sign = DeSerializeFromJsonString(transaction.sign)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Transaction latest', transactions)
    }
    return transactions
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryTransactions(skip = 0, limit = 10000): Promise<Transaction[]> {
  let transactions
  try {
    const sql = `SELECT * FROM transactions ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    transactions = (await db.all(sql)) as DbTransaction[] // TODO: confirm structure of object from db
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction) => {
        if (transaction.data) transaction.data = DeSerializeFromJsonString(transaction.data)
        if (transaction.result) transaction.result = DeSerializeFromJsonString(transaction.result)
        if (transaction.originalTxData)
          transaction.originalTxData = DeSerializeFromJsonString(transaction.originalTxData)
        if (transaction.sign) transaction.sign = DeSerializeFromJsonString(transaction.sign)
      })
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      'Transaction transactions',
      transactions ? transactions.length : transactions,
      'skip',
      skip
    )
  }
  return transactions
}

export async function queryTransactionCount(): Promise<number> {
  let transactions
  try {
    const sql = `SELECT COUNT(*) FROM transactions`
    transactions = await db.get(sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Transaction count', transactions)
  }
  if (transactions) transactions = transactions['COUNT(*)']
  else transactions = 0
  return transactions
}

export async function queryTransactionCountBetweenCycles(
  startCycleNumber: number,
  endCycleNumber: number
): Promise<number> {
  let transactions
  try {
    const sql = `SELECT COUNT(*) FROM transactions WHERE cycleNumber BETWEEN ? AND ?`
    transactions = await db.get(sql, [startCycleNumber, endCycleNumber])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Transaction count between cycles', transactions)
  }
  if (transactions) transactions = transactions['COUNT(*)']
  else transactions = 0
  return transactions
}

export async function queryTransactionsBetweenCycles(
  skip = 0,
  limit = 10000,
  startCycleNumber: number,
  endCycleNumber: number
): Promise<Transaction[]> {
  let transactions
  try {
    const sql = `SELECT * FROM transactions WHERE cycleNumber BETWEEN ? AND ? ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    transactions = (await db.all(sql, [startCycleNumber, endCycleNumber])) as DbTransaction[] // TODO: confirm structure of object from db
    if (transactions.length > 0) {
      transactions.forEach((transaction: DbTransaction) => {
        if (transaction.data) transaction.data = DeSerializeFromJsonString(transaction.data)
        if (transaction.result) transaction.result = DeSerializeFromJsonString(transaction.result)
        if (transaction.originalTxData)
          transaction.originalTxData = DeSerializeFromJsonString(transaction.originalTxData)
        if (transaction.sign) transaction.sign = DeSerializeFromJsonString(transaction.sign)
      })
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      'Transaction transactions between cycles',
      transactions ? transactions.length : transactions,
      'skip',
      skip
    )
  }
  return transactions
}
