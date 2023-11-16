import { Signature } from '@shardus/crypto-utils'
import * as Crypto from '../Crypto'
import * as Account from '../dbstore/accounts'
import * as Logger from '../Logger'
import { config } from '../Config'
import * as Utils from '../Utils'
import { globalAccountsMap } from '../GlobalAccount'
import * as NodeList from '../NodeList'
import { currentNetworkMode } from './Cycles'

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

type GlobalAccountReportResp = {
  ready: boolean
  combinedHash: string
  accounts: { id: string; hash: string; timestamp: number }[]
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

export interface GlobalAccountReportRequestSchema {
  sign: Signature
}

// This has to align with the queue sit time in the validator
const QUEUE_SIT_TIME = 6 * 1000 // 6 seconds

// maxValidatorsToServe
export const servingValidators: Map<string, number> = new Map() // key: validatorKey, value: lastServedTimestamp
const SERVING_VALIDATOR_TIMEOUT = 10 * 1000 // 10 seconds
let servingValidatorsRemovalInterval: NodeJS.Timeout

export const validateAccountDataRequest = (
  payload: AccountDataRequestSchema
): { success: boolean; error?: string } => {
  if (currentNetworkMode !== 'restore') {
    return { success: false, error: 'Account data can only be requested in restore mode!' }
  }
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
  const { accountStart, accountEnd, tsStart, maxRecords, offset, accountOffset, sign } = payload
  err = Utils.validateTypes(sign, { owner: 's', sig: 's' })
  if (err) {
    return { success: false, error: 'Invalid sign object attached' }
  }
  const nodePublicKey = sign.owner
  if (!NodeList.byPublicKey[nodePublicKey]) {
    return { success: false, error: 'This node is not found in the nodelist!' }
  }
  if (!servingValidators.has(nodePublicKey) && servingValidators.size >= config.maxValidatorsToServe) {
    return {
      success: false,
      error: 'Archiver is busy serving other validators at the moment!',
    }
  }
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
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  servingValidators.set(nodePublicKey, Date.now())
  return { success: true }
}

export const validateAccountDataByListRequest = (
  payload: AccountDataByListRequestSchema
): { success: boolean; error?: string } => {
  if (currentNetworkMode !== 'restore') {
    return { success: false, error: 'Account data by list can only be requested in restore mode!' }
  }
  let err = Utils.validateTypes(payload, {
    accountIds: 'a',
    sign: 'o',
  })
  if (err) {
    return { success: false, error: err }
  }
  const { accountIds, sign } = payload
  err = Utils.validateTypes(sign, { owner: 's', sig: 's' })
  if (err) {
    return { success: false, error: 'Invalid sign object attached' }
  }
  const nodePublicKey = sign.owner
  if (!NodeList.byPublicKey[nodePublicKey]) {
    return { success: false, error: 'This node is not found in the nodelist!' }
  }
  // TODO: Add max limit check for accountIds list query
  if (accountIds.length === 0 || accountIds.some((accountId) => accountId.length !== 64)) {
    return { success: false, error: 'Invalid account ids' }
  }
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}

export const validateGlobalAccountReportRequest = (
  payload: GlobalAccountReportRequestSchema
): { success: boolean; error?: string } => {
  let err = Utils.validateTypes(payload, {
    sign: 'o',
  })
  if (err) {
    return { success: false, error: err }
  }
  const { sign } = payload
  err = Utils.validateTypes(sign, { owner: 's', sig: 's' })
  if (err) {
    return { success: false, error: 'Invalid sign object attached' }
  }
  if (!Crypto.verify(payload)) {
    return { success: false, error: 'Invalid signature' }
  }
  return { success: true }
}
/**
 *
 * This function is contructed to provide data in similar way as the `getAccountDataByRangeSmart` function in the validator
 * @param payload
 * @returns GetAccountDataByRangeSmart
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
    sql += `accountId >= ? AND `
    values.push(accountOffset)
  }
  sql += queryString
  values.push(accountStart, accountEnd, tsStart, tsEnd)
  if (!accountOffset) sql += offsetCondition

  let accounts = await Account.fetchAccountsBySqlQuery(sql, values)
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
      accounts = await Account.fetchAccountsBySqlQuery(sql, values)
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
): Promise<WrappedStateArray> => {
  const { accountIds } = payload
  let wrappedAccounts: WrappedStateArray = []
  const sql = `SELECT * FROM accounts WHERE accountId IN (?)`
  const accounts = await Account.fetchAccountsBySqlQuery(sql, accountIds)
  for (const account of accounts) {
    wrappedAccounts.push({
      accountId: account.accountId,
      stateId: account.hash,
      data: account.data,
      timestamp: account.timestamp,
    })
  }
  return wrappedAccounts
}

export const provideGlobalAccountReportRequest = async (): Promise<GlobalAccountReportResp> => {
  const result = { ready: true, combinedHash: '', accounts: [] }
  for (const [key, value] of globalAccountsMap.entries()) {
    result.accounts.push({ id: key, hash: value.hash, timestamp: value.timestamp })
  }
  result.accounts.sort(Utils.byIdAsc)
  result.combinedHash = Crypto.hashObj(result)
  return result
}

// Remove validators from the list that have not requested data over 10 seconds, that way we can serve new validators
const clearTimeoutServingValidators = () => {
  const now = Date.now()
  for (const [validatorKey, lastServedTimestamp] of servingValidators.entries()) {
    if (now - lastServedTimestamp > SERVING_VALIDATOR_TIMEOUT) {
      servingValidators.delete(validatorKey)
    }
  }
}

export const clearServingValidatorsInterval = () => {
  clearInterval(servingValidatorsRemovalInterval)
}

export const initServingValidatorsInterval = () => {
  if (!servingValidatorsRemovalInterval)
    servingValidatorsRemovalInterval = setInterval(clearTimeoutServingValidators, SERVING_VALIDATOR_TIMEOUT)
}
