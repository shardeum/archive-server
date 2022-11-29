import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString, SerializeToJsonString } from '../utils/serialization'

export type AccountCopy = {
  accountId: string
  data: any
  timestamp: number
  hash: string
  cycleNumber: number
  isGlobal?: boolean
}

export async function insertAccount(account: AccountCopy) {
  try {
    const fields = Object.keys(account).join(', ')
    const placeholders = Object.keys(account).fill('?').join(', ')
    const values = extractValues(account)
    let sql = 'INSERT OR REPLACE INTO accounts (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully inserted Account', account.accountId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert Account or it is already stored in to database',
      account.accountId
    )
  }
}

export async function bulkInsertAccounts(accounts: AccountCopy[]) {
  try {
    const fields = Object.keys(accounts[0]).join(', ')
    const placeholders = Object.keys(accounts[0]).fill('?').join(', ')
    const values = extractValuesFromArray(accounts)
    let sql = 'INSERT OR REPLACE INTO accounts (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < accounts.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Accounts', accounts.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Accounts', accounts.length)
  }
}

export async function updateAccount(accountId: string, account: AccountCopy) {
  try {
    const sql = `UPDATE accounts SET cycleNumber = $cycleNumber, timestamp = $timestamp, data = $data, hash = $hash WHERE accountId = $accountId `
    await db.run(sql, {
      $cycleNumber: account.cycleNumber,
      $timestamp: account.timestamp,
      $data: account.data && SerializeToJsonString(account.data),
      $hash: account.hash,
      $accountId: account.accountId,
    })
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Successfully updated Account', account.accountId)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to update Account', account)
  }
}

export async function queryAccountByAccountId(accountId: string) {
  try {
    const sql = `SELECT * FROM accounts WHERE accountId=?`
    let account: any = await db.get(sql, [accountId])
    if (account) if (account && account.data) account.data = DeSerializeFromJsonString(account.data)
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Account accountId', account)
    }
    return account
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryLatestAccounts(count: number) {
  try {
    const sql = `SELECT * FROM accounts ORDER BY cycleNumber DESC, timestamp DESC LIMIT ${
      count ? count : 100
    }`
    const accounts: any = await db.all(sql)
    if (accounts.length > 0) {
      accounts.forEach((account: any) => {
        if (account && account.data) account.data = DeSerializeFromJsonString(account.data)
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Account latest', accounts)
    }
    return accounts
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryAccounts(skip: number = 0, limit: number = 10000) {
  let accounts
  try {
    const sql = `SELECT * FROM accounts ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    accounts = await db.all(sql)
    if (accounts.length > 0) {
      accounts.forEach((account: any) => {
        if (account && account.data) account.data = DeSerializeFromJsonString(account.data)
      })
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Account accounts', accounts ? accounts.length : accounts, 'skip', skip)
  }
  return accounts
}

export async function queryAccountCount() {
  let accounts
  try {
    const sql = `SELECT COUNT(*) FROM accounts`
    accounts = await db.get(sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Account count', accounts)
  }
  if (accounts) accounts = accounts['COUNT(*)']
  else accounts = 0
  return accounts
}

export async function queryAccountCountBetweenCycles(startCycleNumber: number, endCycleNumber: number) {
  let accounts
  try {
    const sql = `SELECT COUNT(*) FROM accounts WHERE cycleNumber BETWEEN ? AND ?`
    accounts = await db.get(sql, [startCycleNumber, endCycleNumber])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Account count between cycles', accounts)
  }
  if (accounts) accounts = accounts['COUNT(*)']
  else accounts = 0
  return accounts
}

export async function queryAccountsBetweenCycles(
  skip = 0,
  limit = 10000,
  startCycleNumber: number,
  endCycleNumber: number
) {
  let accounts
  try {
    const sql = `SELECT * FROM accounts WHERE cycleNumber BETWEEN ? AND ? ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    accounts = await db.all(sql, [startCycleNumber, endCycleNumber])
    if (accounts.length > 0) {
      accounts.forEach((account: any) => {
        if (account && account.data) account.data = DeSerializeFromJsonString(account.data)
      })
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug(
      'Account accounts between cycles',
      accounts ? accounts.length : accounts,
      'skip',
      skip
    )
  }
  return accounts
}
