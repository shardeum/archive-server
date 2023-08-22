import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'

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
