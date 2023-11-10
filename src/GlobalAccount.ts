import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import * as State from './State'
import * as Logger from './Logger'
import * as AccountDB from './dbstore/accounts'
import { config } from './Config'
import { postJson } from './P2P'
import { sleep, robustQuery } from './Utils'

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

export const syncGlobalAccount = async (): Promise<any> => {
  try {
    const queryFn = async (node: any): Promise<any> => {
      return await postJson(
        `http://${node.ip}:${node.port}/get_globalaccountreport_archiver`,
        Crypto.sign({})
      )
    }
    const {
      value: { accounts },
    } = await robustQuery(State.activeArchivers, queryFn)
    if (accounts.length === 0) {
      Logger.mainLogger.error(
        'No Global account data received from any archiver, trying again in 30 seconds...'
      )
      await sleep(30_000)
      await syncGlobalAccount()
    } else {
      globalAccountsMap.set(accounts[0].id, { hash: accounts[0].hash, timestamp: accounts[0].timestamp })
    }
  } catch (e) {
    Logger.mainLogger.error('Error in syncGlobalAccount()', e)
  }
}
