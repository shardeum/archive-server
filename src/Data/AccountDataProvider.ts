import { Signature } from '@shardus/crypto-utils'
import * as Crypto from '../Crypto'
import * as Account from '../dbstore/accounts'
import * as Logger from '../Logger'
import { config } from '../Config'
import * as Utils from '../Utils'

interface WrappedData {
  /** Account ID */
  accountId: string
  /** hash of the data blob */
  stateId: string
  /** data blob opaqe */
  data: unknown
  /** Timestamp */
  timestamp: number

  /** optional data related to sync process */
  syncData?: any
}

type WrappedStateArray = WrappedData[]

type GetAccountDataByRangeSmart = {
  wrappedAccounts: WrappedStateArray
  lastUpdateNeeded: boolean
  wrappedAccounts2: WrappedStateArray
  highestTs: number
  delta: number
}
type WrappedAccounts = {
  wrappedAccounts: WrappedStateArray
}
export interface AccountDataRequestSchema {
  accountStart: string
  accountEnd: string
  tsStart: number
  maxRecords: number
  offset: number
  accountOffset: string
  sign: Signature
}

export interface AccountDataByListRequestSchema {
  accountIds: string[]
  sign: Signature
}

// This has to align with the queue sit time in the validator
const QUEUE_SIT_TIME = 6 * 1000 // 6 seconds

export const validateAccountDataRequest = (
  payload: AccountDataRequestSchema
): { success: boolean; error?: string } => {
  let err = Utils.validateTypes(payload, {
    accountStart: 's',
    accountEnd: 's',
    tsStart: 'n',
    maxRecords: 'n',
    offset: 'n',
    accountOffset: 's',
    sign: 'o',
  })
  if (err) {
    return { success: false, error: err }
  }
  const { accountStart, accountEnd, tsStart, maxRecords, offset, accountOffset } = payload
  if (accountStart.length !== 64 || accountEnd.length !== 64 || accountStart > accountEnd) {
    return { success: false, error: 'Invalid account range' }
  }
  if (Number.isNaN(tsStart) || tsStart < 0 || tsStart > Date.now()) {
    return { success: false, error: 'Invalid start timestamp' }
  }
  if (Number.isNaN(maxRecords) || maxRecords < 1) {
    return { success: false, error: 'Invalid max records' }
  }
  if (Number.isNaN(offset) || offset < 0) {
    return { success: false, error: 'Invalid offset' }
  }
  if (accountOffset && accountOffset.length !== 64) {
    return { success: false, error: 'Invalid account offset' }
  }
  // TODO: We could need to add the data request sender is a validator present in the network
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}

export const validateAccountDataByListRequest = (
  payload: AccountDataByListRequestSchema
): { success: boolean; error?: string } => {
  let err = Utils.validateTypes(payload, {
    accountIds: 'a',
    sign: 'o',
  })
  if (err) {
    return { success: false, error: err }
  }
  const { accountIds } = payload
  // TODO: Add max limit check for accountIds list query
  if (accountIds.length !== 0 || accountIds.some((accountId) => accountId.length !== 64)) {
    return { success: false, error: 'Invalid account ids' }
  }
  // TODO: We could need to add the data request sender is a validator present in the network
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}
/**
 *
 * This function is contructed to provide data in similar way as the `getAccountDataByRangeSmart` function in the validator
 * @param payload
 * @returns
 */
export const provideAccountDataRequest = async (
  payload: AccountDataRequestSchema
): Promise<GetAccountDataByRangeSmart> => {
  let wrappedAccounts: WrappedStateArray = []
  let wrappedAccounts2: WrappedStateArray = [] // We might not need to provide data to this
  let lastUpdateNeeded = false
  let highestTs = 0
  let delta = 0
  const { accountStart, accountEnd, tsStart, maxRecords, offset, accountOffset } = payload
  const tsEnd = Date.now()
  // Query from Shardeum->queryAccountsEntryByRanges3 fn
  // const query = `SELECT * FROM accountsEntry
  // WHERE (timestamp, accountId) >= (${tsStart}, "${accountOffset}")
  // AND timestamp < ${tsEnd}
  // AND accountId <= "${accountEnd}" AND accountId >= "${accountStart}"
  // ORDER BY timestamp, accountId  LIMIT ${maxRecords}`

  let sqlPrefix = `SELECT * FROM accounts WHERE `
  let queryString = `accountId BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC, accountId ASC LIMIT ${maxRecords}`
  let offsetCondition = ` OFFSET ${offset}`
  let sql = sqlPrefix
  let values = []
  if (accountOffset) {
    sql += `${sqlPrefix}accountId >= ? AND `
    values.push(accountOffset)
  }
  sql += queryString
  values.push(accountStart, accountEnd, tsStart, tsEnd)
  if (!accountOffset) sql += offsetCondition

  let accounts = await Account.queryAccountsByRanges(sql, values)
  for (const account of accounts) {
    wrappedAccounts.push({
      accountId: account.accountId,
      stateId: account.hash,
      data: account.data,
      timestamp: account.timestamp,
    })
  }
  if (wrappedAccounts.length === 0) {
    lastUpdateNeeded = true
  } else {
    // see if our newest record is new enough
    highestTs = 0
    for (const account of wrappedAccounts) {
      if (account.timestamp > highestTs) {
        highestTs = account.timestamp
      }
    }
    delta = tsEnd - highestTs
    Logger.mainLogger.debug('Account Data received', JSON.stringify(payload))
    Logger.mainLogger.debug(
      'delta ' + delta,
      'tsEnd ' + tsEnd,
      'highestTs ' + highestTs,
      delta < QUEUE_SIT_TIME * 2
    )
    if (delta < QUEUE_SIT_TIME * 2) {
      const tsStart2 = highestTs
      const tsEnd2 = Date.now()
      sql = sqlPrefix + queryString + offsetCondition
      values = [accountStart, accountEnd, tsStart2, tsEnd2]
      accounts = await Account.queryAccountsByRanges(sql, values)
      for (const account of accounts) {
        wrappedAccounts2.push({
          accountId: account.accountId,
          stateId: account.hash,
          data: account.data,
          timestamp: account.timestamp,
        })
      }
      lastUpdateNeeded = true
    }
  }
  return { wrappedAccounts, lastUpdateNeeded, wrappedAccounts2, highestTs, delta }
}

export const provideAccountDataByListRequest = async (
  payload: AccountDataByListRequestSchema
): Promise<WrappedAccounts> => {
  const { accountIds } = payload
  let wrappedAccounts: WrappedStateArray = []
  const sql = `SELECT * FROM accounts WHERE accountId IN (?)`
  const accounts = await Account.queryAccountsByRanges(sql, accountIds)
  for (const account of accounts) {
    wrappedAccounts.push({
      accountId: account.accountId,
      stateId: account.hash,
      data: account.data,
      timestamp: account.timestamp,
    })
  }
  return { wrappedAccounts }
}
