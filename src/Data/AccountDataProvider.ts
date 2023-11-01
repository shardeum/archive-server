import { Signature } from '@shardus/crypto-utils'
import * as Crypto from '../Crypto'
import * as Account from '../dbstore/accounts'
import * as Logger from '../Logger'
import { config } from '../Config'

// Move account data request schema and validation function to another place
export interface AccountDataRequestSchema {
  accountStart: string
  accountEnd: string
  tsStart: number
  maxRecords: number
  offset: number
  accountOffset: string
  sign: Signature
}

// This has to align with the queue sit time in the validator
const QUEUE_SIT_TIME = 6 * 1000 // 6 seconds

export const validateAccountDataRequest = (
  payload: AccountDataRequestSchema
): { success: boolean; error?: string } => {
  const { accountStart, accountEnd, tsStart, maxRecords, offset, accountOffset } = payload
  // Make sure accountStart, accountEnd exist and valid, it should be a 64 character hex string and accountStart should be less than accountEnd
  if (
    !accountStart ||
    !accountEnd ||
    accountStart > accountEnd ||
    accountStart.length !== 64 ||
    accountEnd.length !== 64
  ) {
    return { success: false, error: 'Invalid account range' }
  }
  // Make sure tsStart exist and valid, it should be a number starting from 0 and less than current timestamp
  if (
    tsStart === null ||
    tsStart === undefined ||
    Number.isNaN(tsStart) ||
    tsStart < 0 ||
    tsStart > Date.now()
  ) {
    return { success: false, error: 'Invalid start timestamp' }
  }
  // Make sure maxRecords is exist and valid, it should be a number greater than 0
  if (!maxRecords || Number.isNaN(maxRecords) || maxRecords < 1) {
    return { success: false, error: 'Invalid max records' }
  }
  // Make sure offset is exist and valid, it should be a number greater than 0
  if (!offset || Number.isNaN(offset) || offset < 0) {
    return { success: false, error: 'Invalid offset' }
  }
  // Make sure accountOffset is exist and valid, it can be null or a 64 character hex string
  if (accountOffset && accountOffset.length !== 64) {
    return { success: false, error: 'Invalid account offset' }
  }
  // TODO: We could need to add the data request sender is a validator present in the network
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}

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

type GetAccountData3Resp = { data: GetAccountDataByRangeSmart; errors?: string[] }

/**
 *
 * This function is contructed to provide data in similar way as the `getAccountDataByRangeSmart` function in the validator
 * @param payload
 * @returns
 */
export const provideAccountDataRequest = async (payload: AccountDataRequestSchema) => {
  let wrappedAccounts: WrappedStateArray = []
  let wrappedAccounts2: WrappedStateArray = [] // We might not need to provide data to this
  let lastUpdateNeeded = false
  let highestTs = 0
  let delta = 0
  const { accountStart, accountEnd, tsStart, maxRecords, offset, accountOffset } = payload
  const tsEnd = Date.now()
  let sql = `SELECT * FROM accounts WHERE`
  const values = []
  if (accountOffset != null && accountOffset.length > 0) {
    // Query from Shardeum, queryAccountsEntryByRanges3 fn
    // const query = `SELECT * FROM accountsEntry
    // WHERE (timestamp, accountId) >= (${tsStart}, "${accountOffset}")
    // AND timestamp < ${tsEnd}
    // AND accountId <= "${accountEnd}" AND accountId >= "${accountStart}"
    // ORDER BY timestamp, accountId  LIMIT ${maxRecords}`

    // Write same query in SQL using BETWEEN
    sql += `accountId BETWEEN ? AND ? AND accountId >= ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC, accountId ASC LIMIT ${maxRecords}`
    values.push(accountStart, accountEnd, accountOffset, tsStart, tsEnd)
  } else {
    sql += `accountId BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC, accountId ASC LIMIT ${maxRecords} OFFSET ${offset}`
    values.push(accountStart, accountEnd, tsStart, tsEnd)
  }
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
      sql += `accountId BETWEEN ? AND ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC, accountId ASC LIMIT ${maxRecords} OFFSET ${offset}`
      values.push(accountStart, accountEnd, tsStart2, tsEnd2)
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
