import { Config, config } from './Config'
import * as Crypto from './Crypto'
import * as P2P from './P2P'
import * as NodeList from './NodeList'
import * as Data from './Data/Data'
import * as Utils from './Utils'
import * as Logger from './Logger'
import { getFinalArchiverList } from '@shardus/archiver-discovery'
import { P2P as P2PTypes } from '@shardus/types'
import { publicKey, secretKey, curvePublicKey, curveSecretKey } from '@shardus/crypto-utils'
import fetch from 'node-fetch'
import { getAdjacentLeftAndRightArchivers } from './Data/GossipData'
import { closeDatabase } from './dbstore'

export interface ArchiverNodeState {
  ip: string
  port: number
  publicKey: publicKey
  secretKey: secretKey
  curvePk: curvePublicKey
  curveSk: curveSecretKey
}

export type ArchiverNodeInfo = Omit<ArchiverNodeState, 'secretKey' | 'curveSk'>

const nodeState: ArchiverNodeState = {
  ip: '',
  port: -1,
  publicKey: '',
  secretKey: '',
  curvePk: '',
  curveSk: '',
}
export const joinedArchivers: ArchiverNodeInfo[] = [] // Add joined archivers to this list first and move to activeArchivers when they are active
export let activeArchivers: ArchiverNodeInfo[] = []
export let activeArchiversByPublicKeySorted: ArchiverNodeInfo[] = []
// archivers list without the current archiver
export let otherArchivers: ArchiverNodeInfo[] = []
export let isFirst = false
export let isActive = false
export const archiversReputation: Map<string, string> = new Map()

export async function initFromConfig(
  config: Config,
  shutDownMode = false,
  useArchiverDiscovery = true
): Promise<void> {
  // Get own nodeInfo from config
  nodeState.ip = config.ARCHIVER_IP
  nodeState.port = config.ARCHIVER_PORT
  nodeState.publicKey = config.ARCHIVER_PUBLIC_KEY
  nodeState.secretKey = config.ARCHIVER_SECRET_KEY
  nodeState.curvePk = Crypto.core.convertPkToCurve(nodeState.publicKey)
  nodeState.curveSk = Crypto.core.convertSkToCurve(nodeState.secretKey)

  if (useArchiverDiscovery === false) return

  let existingArchivers: ArchiverNodeInfo[] = []
  // Parse existing archivers list
  try {
    console.log('ARCHIVER_INFO', process.env.ARCHIVER_INFO)
    console.log('Getting existing archivers list from archiver-discovery.')
    existingArchivers = getFinalArchiverList().map(({ ip, port, publicKey }) => ({
      ip,
      port,
      publicKey,
      curvePk: Crypto.getOrCreateSharedKey(publicKey),
    }))
    console.log(`Got existing archivers list using archiver-discovery. [count: ${existingArchivers.length}]`)
  } catch (e) {
    console.warn('No existing archivers were found:', e)
  }

  if (existingArchivers.length === 0) {
    console.log('No existing archivers were found. This is the first archiver.')
    isFirst = true
    return
  }

  if (shutDownMode) return

  let retryCount = 1
  const waitTime = 1000 * 60

  while (retryCount < 10 && activeArchivers.length === 0) {
    Logger.mainLogger.debug(`Getting consensor list from other achivers. [round: ${retryCount}]`)
    /* eslint-disable security/detect-object-injection */
    for (let i = 0; i < existingArchivers.length; i++) {
      if (existingArchivers[i].publicKey === nodeState.publicKey) {
        continue
      }
      const response = (await P2P.getJson(
        `http://${existingArchivers[i].ip}:${existingArchivers[i].port}/nodelist`
      )) as Crypto.core.SignedObject
      Logger.mainLogger.debug(
        'response',
        `http://${existingArchivers[i].ip}:${existingArchivers[i].port}/nodelist`,
        response
      )
      if (!response) {
        Logger.mainLogger.warn(
          `No response when fetching from archiver ${existingArchivers[i].ip}:${existingArchivers[i].port}`
        )
        continue
      }
      if (!Crypto.verify(response)) {
        /* prettier-ignore */ console.log(`Invalid signature when fetching from archiver ${existingArchivers[i].ip}:${existingArchivers[i].port}`)
        continue
      }
      if (response && response.nodeList && response.nodeList.length > 0) {
        addArchiver(existingArchivers[i])
      }
    }
    /* eslint-enable security/detect-object-injection */
    if (activeArchivers.length === 0) {
      Logger.mainLogger.error(`Unable to find active archivers. Waiting for ${waitTime} before trying again.`)
      // wait for 1 min before retrying
      await Utils.sleep(waitTime)
      retryCount += 1
    }
  }
  if (activeArchivers.length === 0) {
    Logger.mainLogger.error(
      `We have tried ${retryCount} times to get nodeList from other archivers. But got no response or empty list. About to exit now.`
    )
    process.exit(0)
  }
}

export async function exitArchiver(): Promise<void> {
  try {
    const randomConsensors: NodeList.ConsensusNodeInfo[] = NodeList.getRandomActiveNodes(5)
    if (randomConsensors && randomConsensors.length > 0) {
      // Send a leave request to some random consensus nodes from the nodelist
      await Data.sendLeaveRequest(randomConsensors)
    }
    Logger.mainLogger.debug('Archiver will exit in 3 seconds.')
    setTimeout(() => {
      process.exit()
    }, 3000)
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export function addSigListeners(sigint = true, sigterm = true): void {
  if (sigint) {
    process.on('SIGINT', async () => {
      Logger.mainLogger.debug('Exiting on SIGINT', process.pid)
      await closeDatabase()
      if (isActive) exitArchiver()
      else process.exit(0)
    })
  }
  if (sigterm) {
    process.on('SIGTERM', async () => {
      Logger.mainLogger.debug('Exiting on SIGTERM', process.pid)
      await closeDatabase()
      if (isActive) exitArchiver()
      else process.exit(0)
    })
  }
  Logger.mainLogger.debug('Registerd exit signal listeners.')
}

export function addArchiver(archiver: ArchiverNodeInfo): void {
  const foundArchiver = activeArchivers.find((a) => a.publicKey === archiver.publicKey)
  if (!foundArchiver) {
    Logger.mainLogger.debug('Adding archiver', archiver)
    activeArchivers.push(archiver)
    Utils.insertSorted(activeArchiversByPublicKeySorted, archiver, NodeList.byAscendingPublicKey)
    Logger.mainLogger.debug(
      'activeArchiversByPublicKeySorted',
      activeArchiversByPublicKeySorted.map((archiver) => archiver.publicKey)
    )
    if (archiver.publicKey !== config.ARCHIVER_PUBLIC_KEY) {
      otherArchivers.push(archiver)
    }
    Logger.mainLogger.debug('New archiver added to active list', archiver)
  }
  Logger.mainLogger.debug('archivers list', activeArchivers)
}

export function removeActiveArchiver(publicKey: string): void {
  activeArchivers = activeArchivers.filter((a: ArchiverNodeInfo) => a.publicKey !== publicKey)
  activeArchiversByPublicKeySorted = activeArchiversByPublicKeySorted.filter(
    (a: ArchiverNodeInfo) => a.publicKey !== publicKey
  )
  otherArchivers = otherArchivers.filter((a: ArchiverNodeInfo) => a.publicKey !== publicKey)
  archiversReputation.delete(publicKey)
}

export function resetActiveArchivers(archivers: ArchiverNodeInfo[]): void {
  Logger.mainLogger.debug('Resetting active archivers.', archivers)
  activeArchivers = archivers
  activeArchiversByPublicKeySorted = [...archivers.sort(NodeList.byAscendingPublicKey)]
  otherArchivers = activeArchivers.filter((a) => a.publicKey !== config.ARCHIVER_PUBLIC_KEY)
  archiversReputation.clear()
  for (const archiver of activeArchivers) {
    archiversReputation.set(archiver.publicKey, 'up')
  }
  getAdjacentLeftAndRightArchivers()
}

export async function compareCycleRecordWithOtherArchivers(
  archivers: ArchiverNodeInfo[],
  ourCycleRecord: P2PTypes.CycleCreatorTypes.CycleRecord
): Promise<boolean> {
  const promises = archivers.map((archiver) =>
    fetch(`http://${archiver.ip}:${archiver.port}/cycleinfo/1`, {
      method: 'get',
      headers: { 'Content-Type': 'application/json' },
      timeout: 2000,
    }).then((res) => res.json())
  )
  let foundMatch = true
  await Promise.allSettled(promises)
    .then((responses) => {
      let i = 0
      for (const response of responses) {
        const archiver = activeArchivers[i] // eslint-disable-line security/detect-object-injection
        if (response.status === 'fulfilled') {
          const res = response.value
          if (res && res.cycleInfo && res.cycleInfo.length > 0) {
            const cycleInfo = res.cycleInfo[0] as P2PTypes.CycleCreatorTypes.CycleRecord
            if (cycleInfo.counter > ourCycleRecord.counter || cycleInfo.mode !== ourCycleRecord.mode) {
              Logger.mainLogger.debug(
                `Our cycle record does not match with archiver ${archiver.ip}:${archiver.port}`
              )
              // If our cycle record does not match with any of the archivers, maybe we are behind the network
              foundMatch = false
              break
            }
          }
        }
        i++
      }
    })
    .catch((error) => {
      // Handle any errors that occurred
      console.error(error)
    })
  return foundMatch
}

export function getNodeInfo(): ArchiverNodeInfo {
  const sanitizedNodeInfo = { ...nodeState }
  delete sanitizedNodeInfo.secretKey
  delete sanitizedNodeInfo.curveSk
  return sanitizedNodeInfo
}

export function getSecretKey(): secretKey {
  return nodeState.secretKey
}

export function getCurveSk(): curveSecretKey {
  return nodeState.curveSk
}

export function getCurvePk(): curvePublicKey {
  return nodeState.curvePk
}

export function setActive(): void {
  isActive = true
}

export function getRandomArchiver(): ArchiverNodeInfo {
  const randomArchiver = Utils.getRandomItemFromArr(otherArchivers)[0]
  return randomArchiver
}
