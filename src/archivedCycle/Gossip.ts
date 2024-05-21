import * as Crypto from '../Crypto'
import * as StateMetaData from './StateMetaData'
import * as State from '../State'
import * as P2P from '../P2P'
import { config } from '../Config'
import * as Logger from '../Logger'
import { Utils as StringUtils } from '@shardus/types'

interface HashItem {
  counter: number
  partitionHashes: { [key: string]: unknown }
  networkHash: string
  receiptMapHashes: { [key: string]: unknown }
  networkReceiptHash: string
}

interface Gossip extends StateMetadata {
  counter: number
}

interface GossipCounterItem {
  count: number
  gossip: Gossip
}

interface StateMetadata {
  stateHashes: HashItem[]
  receiptHashes: HashItem[]
  summaryHashes: HashItem[]
}

interface HashCollectorItem {
  counter: number
  stateHashes?: HashItem
  receiptHashes?: HashItem
  summaryHashes?: HashItem
}

const gossipCollector = new Map()

export async function sendGossip(type: string, payload: Record<string, unknown>): Promise<void> {
  let archivers: State.ArchiverNodeInfo[] = [...State.activeArchivers]

  if (archivers.length === 0) return
  const gossipPayload = {
    type,
    data: payload,
    sender: config.ARCHIVER_PUBLIC_KEY,
  }

  archivers = archivers.sort(
    (a: State.ArchiverNodeInfo, b: State.ArchiverNodeInfo) => Number(a.publicKey) - Number(b.publicKey)
  )

  // TODO: check if need to select random archivers instead of sending to all other archivers
  const recipients: State.ArchiverNodeInfo[] = archivers.filter(
    (a) => a.publicKey !== config.ARCHIVER_PUBLIC_KEY
  )

  if (recipients.length === 0) {
    Logger.mainLogger.debug('There is no other archivers to send our gossip')
    return
  }

  try {
    Logger.mainLogger.debug(
      `GossipingIn ${type} request to these nodes: ${StringUtils.safeStringify(
        recipients.map((node) => node.ip + ':' + node.port + `/gossip-${type}`)
      )}`
    )
    await tell(recipients, `gossip-${type}`, gossipPayload)
  } catch (ex) {
    Logger.mainLogger.debug(ex)
    Logger.mainLogger.debug('Fail to gossip')
  }
}

async function tell(
  nodes: State.ArchiverNodeInfo[],
  route: string,
  message: { [key: string]: unknown }
): Promise<void> {
  let InternalTellCounter = 0
  const promises = []
  for (const node of nodes) {
    InternalTellCounter++
    if (config.VERBOSE) {
      Logger.mainLogger.debug(`InternalTellCounter: ${InternalTellCounter}`)
    }
    const url = `http://${node.ip}:${node.port}/${route}`
    try {
      const promise = P2P.postJson(url, message)
      promise.catch((err) => {
        Logger.mainLogger.error(`Unable to tell node ${node.ip}: ${node.port}`, err)
      })
      promises.push(promise)
    } catch (e) {
      Logger.mainLogger.error('Error', e)
    }
  }
  try {
    await Promise.all(promises)
  } catch (err) {
    Logger.mainLogger.error('Network: ' + err)
  }
}

export function convertStateMetadataToHashArray(STATE_METATDATA: StateMetadata): HashCollectorItem[] {
  const hashCollector: Record<number, HashCollectorItem> = {}
  STATE_METATDATA.stateHashes.forEach((h: HashItem) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter,
      }
    }
    hashCollector[h.counter]['stateHashes'] = h
  })
  STATE_METATDATA.receiptHashes.forEach((h: HashItem) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter,
      }
    }
    hashCollector[h.counter]['receiptHashes'] = h
  })
  STATE_METATDATA.summaryHashes.forEach((h: HashItem) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter,
      }
    }
    hashCollector[h.counter]['summaryHashes'] = h
  })
  return Object.values(hashCollector)
}

export function addHashesGossip(sender: string, gossip: Gossip): void {
  const counter = gossip.counter
  if (gossipCollector.has(counter)) {
    const existingGossips = gossipCollector.get(counter)
    // eslint-disable-next-line security/detect-object-injection
    existingGossips[sender] = gossip
  } else {
    const obj: Record<string, Gossip> = {}
    // eslint-disable-next-line security/detect-object-injection
    obj[sender] = gossip
    gossipCollector.set(counter, obj)
  }
  const totalGossip = gossipCollector.get(counter)
  if (totalGossip && Object.keys(totalGossip).length > 0.5 * State.activeArchivers.length) {
    setTimeout(() => {
      processGossip(counter)
      gossipCollector.delete(counter)
    }, 500)
  }
}

function processGossip(counter: number): void {
  Logger.mainLogger.debug('Processing gossips for counter', counter, gossipCollector.get(counter))
  const gossips = gossipCollector.get(counter)
  if (!gossips) {
    return
  }
  const ourHashes = StateMetaData.StateMetaDataMap.get(counter)
  const gossipCounter: Record<string, GossipCounterItem> = {}
  for (const sender in gossips) {
    /* eslint-disable security/detect-object-injection */
    const hashedGossip = Crypto.hashObj(gossips[sender])
    if (!gossipCounter[hashedGossip]) {
      gossipCounter[hashedGossip] = {
        count: 1,
        gossip: gossips[sender],
      }
      // To count our StateMetaData also
      if (hashedGossip === Crypto.hashObj(ourHashes)) {
        gossipCounter[hashedGossip].count += 1
      }
    } else {
      gossipCounter[hashedGossip].count += 1
      /* eslint-enable security/detect-object-injection */
    }
  }
  const gossipWithHighestCount: Gossip[] = []
  let highestCount = 0
  let hashWithHighestCounter: string
  for (const key in gossipCounter) {
    // eslint-disable-next-line security/detect-object-injection
    if (gossipCounter[key].count > highestCount) {
      // eslint-disable-next-line security/detect-object-injection
      gossipWithHighestCount.push(gossipCounter[key].gossip)
      hashWithHighestCounter = key
      // eslint-disable-next-line security/detect-object-injection
      highestCount = gossipCounter[key].count
    }
  }

  if (!ourHashes) {
    Logger.mainLogger.error(`Unable to find our stored statemetadata hashes for counter ${counter}`)
    return
  }
  if (hashWithHighestCounter && hashWithHighestCounter !== Crypto.hashObj(ourHashes)) {
    if (gossipWithHighestCount.length === 0) {
      return
    }
    Logger.mainLogger.error('our hash is different from other archivers hashes. Storing the correct hashes')
    Logger.mainLogger.debug('gossipWithHighestCount', gossipWithHighestCount[0].summaryHashes)
    StateMetaData.processStateMetaData({ gossipWithHighestCount })
    StateMetaData.replaceDataSender(StateMetaData.currentDataSender)
  }
}
