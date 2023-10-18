import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import { config } from './Config'
import * as AccountDB from './dbstore/accounts'

let cachedGlobalAccount: object
let cachedGlobalAccountHash: string

export function getGlobalAccount(hash: boolean): object | string {
  if (hash) {
    return cachedGlobalAccountHash
  }

  return cachedGlobalAccount
}

export function setGlobalAccount(account: object): void {
  cachedGlobalAccount = rfdc()(account)
  cachedGlobalAccountHash = Crypto.hashObj(account)
}

export const loadGlobalNetworkAccountFromDB = async () => {
  const account = await AccountDB.queryAccountByAccountId(config.globalAccount)
  if (account) {
    setGlobalAccount(account)
  }
}
