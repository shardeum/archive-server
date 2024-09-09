import * as db from './sqlite3storage'
import { accountDatabase } from '.'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString, SerializeToJsonString } from '../utils/serialization'

/** Same as type AccountsCopy in the shardus core */
export type AccountsCopy = {
  accountId: string
  data: any // eslint-disable-line @typescript-eslint/no-explicit-any
  timestamp: number
  hash: string
  cycleNumber: number
  isGlobal: boolean
}

type DbAccountCopy = AccountsCopy & {
  data: string
}

export async function insertAccount(account: AccountsCopy): Promise<void> {
  try {
    const fields = Object.keys(account).join(', ')
    const placeholders = Object.keys(account).fill('?').join(', ')
    const values = db.extractValues(account)
    const sql = 'INSERT OR REPLACE INTO accounts (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(accountDatabase, sql, values)
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

export async function bulkInsertAccounts(accounts: AccountsCopy[]): Promise<void> {
  try {
    const fields = Object.keys(accounts[0]).join(', ')
    const placeholders = Object.keys(accounts[0]).fill('?').join(', ')
    const values = db.extractValuesFromArray(accounts)
    let sql = 'INSERT OR REPLACE INTO accounts (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < accounts.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(accountDatabase, sql, values)
    if (config.VERBOSE) Logger.mainLogger.debug('Successfully inserted Accounts', accounts.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Accounts', accounts.length)
  }
}

export async function updateAccount(account: AccountsCopy): Promise<void> {
  try {
    const sql = `UPDATE accounts SET cycleNumber = $cycleNumber, timestamp = $timestamp, data = $data, hash = $hash WHERE accountId = $accountId `
    await db.run(accountDatabase, sql, {
      $cycleNumber: account.cycleNumber,
      $timestamp: account.timestamp,
      $data: SerializeToJsonString(account.data),
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

export async function queryAccountByAccountId(accountId: string): Promise<AccountsCopy | null> {
  try {
    const sql = `SELECT * FROM accounts WHERE accountId=?`
    const dbAccount = (await db.get(accountDatabase, sql, [accountId])) as DbAccountCopy
    let account: AccountsCopy
    if (dbAccount) account = { ...dbAccount, data: DeSerializeFromJsonString(dbAccount.data) }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Account accountId', account)
    }
    return account
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryLatestAccounts(count: number): Promise<AccountsCopy[] | null> {
  try {
    const sql = `SELECT * FROM accounts ORDER BY cycleNumber DESC, timestamp DESC LIMIT ${
      count ? count : 100
    }`
    const dbAccounts = (await db.all(accountDatabase, sql)) as DbAccountCopy[]
    const accounts: AccountsCopy[] = []
    if (dbAccounts.length > 0) {
      for (const dbAccount of dbAccounts) {
        accounts.push({ ...dbAccount, data: DeSerializeFromJsonString(dbAccount.data) })
      }
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Account latest', accounts)
    }
    return accounts
  } catch (e) {
    Logger.mainLogger.error(e)
    return null
  }
}

export async function queryAccounts(skip = 0, limit = 10000): Promise<AccountsCopy[]> {
  let dbAccounts: DbAccountCopy[]
  const accounts: AccountsCopy[] = []
  try {
    const sql = `SELECT * FROM accounts ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    dbAccounts = (await db.all(accountDatabase, sql)) as DbAccountCopy[]
    if (dbAccounts.length > 0) {
      for (const dbAccount of dbAccounts) {
        accounts.push({ ...dbAccount, data: DeSerializeFromJsonString(dbAccount.data) })
      }
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Account accounts', accounts ? accounts.length : accounts, 'skip', skip)
  }
  return accounts
}

export async function queryAccountCount(): Promise<number> {
  let accounts
  try {
    const sql = `SELECT COUNT(timestamp) FROM accounts`
    accounts = await db.get(accountDatabase, sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Account count', accounts)
  }
  if (accounts) accounts = accounts['COUNT(timestamp)']
  else accounts = 0
  return accounts
}

export async function queryAccountCountBetweenCycles(
  startCycleNumber: number,
  endCycleNumber: number
): Promise<number> {
  let accounts
  try {
    const sql = `SELECT COUNT(*) FROM accounts WHERE cycleNumber BETWEEN ? AND ?`
    accounts = await db.get(accountDatabase, sql, [startCycleNumber, endCycleNumber])
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
): Promise<AccountsCopy[]> {
  let dbAccounts: DbAccountCopy[]
  const accounts: AccountsCopy[] = []
  try {
    const sql = `SELECT * FROM accounts WHERE cycleNumber BETWEEN ? AND ? ORDER BY cycleNumber ASC, timestamp ASC LIMIT ${limit} OFFSET ${skip}`
    dbAccounts = (await db.all(accountDatabase, sql, [startCycleNumber, endCycleNumber])) as DbAccountCopy[]
    if (dbAccounts.length > 0) {
      for (const dbAccount of dbAccounts) {
        accounts.push({ ...dbAccount, data: DeSerializeFromJsonString(dbAccount.data) })
      }
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

export async function fetchAccountsBySqlQuery(sql: string, value: string[]): Promise<AccountsCopy[]> {
  const accounts: AccountsCopy[] = []
  try {
    const dbAccounts = (await db.all(accountDatabase, sql, value)) as DbAccountCopy[]
    if (dbAccounts.length > 0) {
      for (const dbAccount of dbAccounts) {
        accounts.push({ ...dbAccount, data: DeSerializeFromJsonString(dbAccount.data) })
      }
    }
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('fetchAccountsBySqlQuery', accounts ? accounts.length : accounts)
  }
  return accounts
}
