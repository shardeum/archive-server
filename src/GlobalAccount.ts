import * as Crypto from './Crypto'
import * as rfdc from 'rfdc'
import * as State from './State'
import * as Logger from './Logger'
import * as AccountDB from './dbstore/accounts'
import { config } from './Config'
import { postJson, getJson } from './P2P'
import { robustQuery, deepCopy } from './Utils'
import { isDeepStrictEqual } from 'util'
import { accountSpecificHash } from './shardeum/calculateAccountHash'

let cachedGlobalNetworkAccount: AccountDB.AccountsCopy
let cachedGlobalNetworkAccountHash: string

interface Node {
  ip: string
  port: number
}

export interface GlobalAccountsHashAndTimestamp {
  hash: string
  timestamp: number
}
export const globalAccountsMap = new Map<string, GlobalAccountsHashAndTimestamp>()
const appliedConfigChanges = new Set<string>()

export function getGlobalNetworkAccount(hash: boolean): AccountDB.AccountsCopy | string {
  if (hash) {
    return cachedGlobalNetworkAccountHash
  }

  return cachedGlobalNetworkAccount
}

export function setGlobalNetworkAccount(account: AccountDB.AccountsCopy): void {
  cachedGlobalNetworkAccount = rfdc()(account)
  cachedGlobalNetworkAccountHash = account.hash
}

interface NetworkConfigChanges {
  cycle: number
  change: any
  appData: any
}

export const updateGlobalNetworkAccount = async (cycleNumber: number): Promise<void> => {
  if (!cachedGlobalNetworkAccountHash) return
  const networkAccount = rfdc()(cachedGlobalNetworkAccount)
  const changes = networkAccount.data.listOfChanges as NetworkConfigChanges[]
  if (!changes || !Array.isArray(changes)) {
    return
  }
  const activeConfigChanges = new Set<string>()
  for (const change of changes) {
    // skip future changes
    if (change.cycle > cycleNumber) {
      continue
    }
    const changeHash = Crypto.hashObj(change)
    // skip handled changes
    if (appliedConfigChanges.has(changeHash)) {
      activeConfigChanges.add(changeHash)
      continue
    }
    // apply this change
    appliedConfigChanges.add(changeHash)
    activeConfigChanges.add(changeHash)
    const changeObj = change.change
    const appData = change.appData

    // If there is initShutdown change, if the latest cycle is greater than the cycle of the change, then skip it
    if (changeObj['p2p'] && changeObj['p2p']['initShutdown'] && change.cycle !== cycleNumber) continue

    const newChanges = pruneNetworkChangeQueue(changes, cycleNumber)
    networkAccount.data.listOfChanges = newChanges
    // https://github.com/shardeum/shardeum/blob/c449ecd21391747c5b7173da3a74415da2acb0be/src/index.ts#L6958
    // Increase the timestamp by 1 second
    // networkAccount.data.timestamp += 1000

    if (appData) {
      updateNetworkChangeQueue(networkAccount.data, appData)
      console.dir(networkAccount.data, { depth: null })
      // https://github.com/shardeum/shardeum/blob/c449ecd21391747c5b7173da3a74415da2acb0be/src/index.ts#L6889
      // Increase the timestamp by 1 second
      // networkAccount.data.timestamp += 1000
    }

    networkAccount.hash = accountSpecificHash(networkAccount.data)
    networkAccount.timestamp = networkAccount.data.timestamp
    Logger.mainLogger.debug('updateGlobalNetworkAccount', networkAccount)
    await AccountDB.updateAccount(networkAccount)
    setGlobalNetworkAccount(networkAccount)
  }
  if (activeConfigChanges.size > 0) {
    // clear the entries from appliedConfigChanges that are no longer in the changes list
    for (const changeHash of appliedConfigChanges) {
      if (!activeConfigChanges.has(changeHash)) {
        appliedConfigChanges.delete(changeHash)
      }
    }
  }
}

const generatePathKeys = (obj: any, prefix = ''): string[] => {
  /* eslint-disable security/detect-object-injection */
  let paths: string[] = []

  // Loop over each key in the object
  for (const key of Object.keys(obj)) {
    // If the value corresponding to this key is an object (and not an array or null),
    // then recurse into it.
    if (obj[key] !== null && typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
      paths = paths.concat(generatePathKeys(obj[key], prefix + key + '.'))
    } else {
      // Otherwise, just append this key to the path.
      paths.push(prefix + key)
    }
  }
  return paths
  /* eslint-enable security/detect-object-injection */
}

const pruneNetworkChangeQueue = (
  changes: NetworkConfigChanges[],
  currentCycle: number
): NetworkConfigChanges[] => {
  const configsMap = new Map()
  const keepAliveCount = config.configChangeMaxChangesToKeep
  for (let i = changes.length - 1; i >= 0; i--) {
    const thisChange = changes[i]
    let keepAlive = false

    let appConfigs = []
    if (thisChange.appData) {
      appConfigs = generatePathKeys(thisChange.appData, 'appdata.')
    }
    const shardusConfigs: string[] = generatePathKeys(thisChange.change)

    const allConfigs = appConfigs.concat(shardusConfigs)

    for (const config of allConfigs) {
      if (!configsMap.has(config)) {
        configsMap.set(config, 1)
        keepAlive = true
      } else if (configsMap.get(config) < keepAliveCount) {
        configsMap.set(config, configsMap.get(config) + 1)
        keepAlive = true
      }
    }

    if (currentCycle - thisChange.cycle <= config.configChangeMaxCyclesToKeep) {
      keepAlive = true
    }

    if (keepAlive == false) {
      changes.splice(i, 1)
    }
  }
  return changes
}

const updateNetworkChangeQueue = (data: object, appData: object): void => {
  if ('current' in data) patchAndUpdate(data?.current, appData)
}

const patchAndUpdate = (existingObject: any, changeObj: any, parentPath = ''): void => {
  /* eslint-disable security/detect-object-injection */
  for (const [key, value] of Object.entries(changeObj)) {
    if (existingObject[key] != null) {
      if (typeof value === 'object') {
        patchAndUpdate(existingObject[key], value, parentPath === '' ? key : parentPath + '.' + key)
      } else {
        existingObject[key] = value
      }
    }
  }
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

export const syncGlobalAccount = async (retry = 5): Promise<void> => {
  while (retry > 0) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const queryFn = async (node: Node): Promise<any> => {
        return await postJson(
          `http://${node.ip}:${node.port}/get_globalaccountreport_archiver`,
          Crypto.sign({})
        )
      }

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const equalFn = (info1: any, info2: any): boolean => {
        const cm1 = deepCopy(info1)
        const cm2 = deepCopy(info2)
        delete cm1.sign
        delete cm2.sign
        const equivalent = isDeepStrictEqual(cm1, cm2)
        return equivalent
      }

      const globalAccsResponse = await robustQuery(State.otherArchivers, queryFn, equalFn, 3, true)
      Logger.mainLogger.debug('syncGlobalAccount() - globalAccsResponse', globalAccsResponse)
      if (!globalAccsResponse) {
        Logger.mainLogger.warn('() - robustResponse is null')
        throw new Error('() - robustResponse is null')
      }
      const {
        value: { accounts },
      } = globalAccsResponse
      for (const { id, hash, timestamp } of accounts) {
        globalAccountsMap.set(id, { hash, timestamp })
      }

      if (globalAccountsMap.has(config.globalNetworkAccount)) {
        const savedNetworkAccount = await AccountDB.queryAccountByAccountId(config.globalNetworkAccount)
        if (
          savedNetworkAccount &&
          savedNetworkAccount.hash === globalAccountsMap.get(config.globalNetworkAccount).hash
        ) {
          setGlobalNetworkAccount(savedNetworkAccount)
          return
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const queryFn = async (node: Node): Promise<any> => {
          return await getJson(`http://${node.ip}:${node.port}/get-network-account?hash=false`)
        }
        const networkAccResponse = await robustQuery(State.otherArchivers, queryFn, equalFn, 3, true)
        Logger.mainLogger.debug('syncGlobalAccount() - networkAccResponse', networkAccResponse)
        if (!networkAccResponse) {
          Logger.mainLogger.warn('get-network-account() - robustResponse is null')
          throw new Error('get-network-account() - robustResponse is null')
        }
        const {
          value: { networkAccount },
        } = networkAccResponse
        if (networkAccount) {
          setGlobalNetworkAccount(networkAccount)
        }
      }
      return
    } catch (e) {
      Logger.mainLogger.error('Error in syncGlobalAccount()', e)
      retry--
    }
  }
}
