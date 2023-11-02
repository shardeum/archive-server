import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import { config } from './Config'
import * as AccountDB from './dbstore/accounts'

let cachedGlobalNetworkAccount: object
let cachedGlobalNetworkAccountHash: string
export const globalAccountsMap = new Map<string, object>()

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

export const loadGlobalNetworkAccountFromDB = async () => {
  const account = await AccountDB.queryAccountByAccountId(config.globalNetworkAccount)
  if (account) {
    setGlobalNetworkAccount(account)
  }
}
