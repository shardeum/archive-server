import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import * as State from './State'
import * as Logger from './Logger'
import * as AccountDB from './dbstore/accounts'
import { config } from './Config'
import { postJson, getJson } from './P2P'
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

export const loadGlobalAccounts = async (): Promise<void> => {
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
    const globalAccsResponse = await robustQuery(State.activeArchivers, queryFn)
    if (!globalAccsResponse) {
      Logger.mainLogger.warn('get_globalaccountreport_archiver() - robustResponse is null')
      throw new Error('get_globalaccountreport_archiver() - robustResponse is null')
    }
    const {
      value: { accounts },
    } = globalAccsResponse
    for (let i = 0; i < accounts.length; i++) {
      globalAccountsMap.set(accounts[i].id, {
        hash: accounts[i].hash,
        timestamp: accounts[i].timestamp,
      })
    }

    if (globalAccountsMap.has(config.globalNetworkAccount) && !cachedGlobalNetworkAccountHash) {
      const queryFn = async (node: any): Promise<any> => {
        return await getJson(`http://${node.ip}:${node.port}/get-network-account?hash=false`)
      }
      const networkAccResponse = await robustQuery(State.activeArchivers, queryFn)
      if (!networkAccResponse) {
        Logger.mainLogger.warn('get-network-account() - robustResponse is null')
        throw new Error('get-network-account() - robustResponse is null')
      }
      const {
        value: { networkAccount },
      } = networkAccResponse
      if (networkAccount) {
        await AccountDB.insertAccount(networkAccount)
        setGlobalNetworkAccount(networkAccount)
      }
    }
  } catch (e) {
    Logger.mainLogger.error('Error in syncGlobalAccount()', e)
  }
}
