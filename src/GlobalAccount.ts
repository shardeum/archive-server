import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import { config } from './Config'
import * as AccountDB from './dbstore/accounts'

let cachedGlobalNetworkAccount: object
let cachedGlobalNetworkAccountHash: string
export interface GlobalAccountsHashAndTimestamp {
  hash: string
  timestamp: number
}
export const globalAccountsMap = new Map<string, GlobalAccountsHashAndTimestamp>()

export function getGlobalNetworkAccount(hash: boolean): object | string {
  if (hash) {
    return cachedGlobalNetworkAccountHash
  }

  return cachedGlobalNetworkAccount
}

export function setGlobalNetworkAccount(account: object): void {
  cachedGlobalNetworkAccount = rfdc()(account)
  cachedGlobalNetworkAccountHash = Crypto.hashObj(account)
}

export const loadGlobalAccounts = async () => {
  const sql = `SELECT * FROM accounts WHERE isGlobal=1`
  const values = []
  const accounts = await AccountDB.fetchAccountsBySqlQuery(sql, values)
  for (const account of accounts) {
    globalAccountsMap.set(account.accountId, { hash: account.hash, timestamp: account.timestamp })
    if (account.accountId === config.globalNetworkAccount) {
      setGlobalNetworkAccount(account)
    }
  }
}
