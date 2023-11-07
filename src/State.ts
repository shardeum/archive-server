import { Config } from './Config'
import * as Crypto from './Crypto'
import * as P2P from './P2P'
import * as NodeList from './NodeList'
import * as Data from './Data/Data'
import * as Utils from './Utils'
import * as Logger from './Logger'
import { getFinalArchiverList } from '@shardus/archiver-discovery'
import * as ShardusCrypto from '@shardus/crypto-utils'
import { P2P as P2PTypes } from '@shardus/types'
import fetch from 'node-fetch'
import { getAdjacentLeftAndRightArchivers } from './Data/GossipData'

export interface ArchiverNodeState {
  ip: string
  port: number
  publicKey: Crypto.types.publicKey
  secretKey: Crypto.types.secretKey
  curvePk: Crypto.types.curvePublicKey
  curveSk: Crypto.types.curveSecretKey
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
export let joinedArchivers: ArchiverNodeInfo[] = [] // Add joined archivers to this list first and move to activeArchivers when they are active
export let activeArchivers: ArchiverNodeInfo[] = []
export let activeArchiversByPublicKeySorted: ArchiverNodeInfo[] = []
export let isFirst = false
export let archiversReputation: Map<string, string> = new Map()

export async function initFromConfig(config: Config, shutDownMode: boolean = false) {
  // Get own nodeInfo from config
  nodeState.ip = config.ARCHIVER_IP
  nodeState.port = config.ARCHIVER_PORT
  nodeState.publicKey = config.ARCHIVER_PUBLIC_KEY
  nodeState.secretKey = config.ARCHIVER_SECRET_KEY
  nodeState.curvePk = Crypto.core.convertPkToCurve(nodeState.publicKey)
  nodeState.curveSk = Crypto.core.convertSkToCurve(nodeState.secretKey)

  let existingArchivers: ArchiverNodeInfo[] = []
  // Parse existing archivers list
  try {
    console.log('ARCHIVER_INFO', process.env.ARCHIVER_INFO)
    console.log('Getting existing archivers list from archiver-discovery.')
    existingArchivers = getFinalArchiverList().map(({ ip, port, publicKey }) => ({
      ip,
      port,
      publicKey,
      curvePk: ShardusCrypto.convertPkToCurve(publicKey),
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
  let waitTime = 1000 * 60

  while (retryCount < 10 && activeArchivers.length === 0) {
    Logger.mainLogger.debug(`Getting consensor list from other achivers. [round: ${retryCount}]`)
    for (let i = 0; i < existingArchivers.length; i++) {
      if (existingArchivers[i].publicKey === nodeState.publicKey) {
        continue
      }
      let response: any = await P2P.getJson(
        `http://${existingArchivers[i].ip}:${existingArchivers[i].port}/nodelist`
      )
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
      if (!ShardusCrypto.verifyObj(response)) {
        /* prettier-ignore */ console.log(`Invalid signature when fetching from archiver ${existingArchivers[i].ip}:${existingArchivers[i].port}`)
        continue
      }
      if (response && response.nodeList && response.nodeList.length > 0) {
        activeArchivers.push(existingArchivers[i])
        Utils.insertSorted(
          activeArchiversByPublicKeySorted,
          existingArchivers[i],
          NodeList.byAscendingPublicKey
        )
        Logger.mainLogger.debug(
          'activeArchiversByPublicKeySorted',
          activeArchiversByPublicKeySorted.map((archiver) => archiver.publicKey)
        )
      }
    }
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

export async function exitArchiver() {
  try {
    const randomConsensors: NodeList.ConsensusNodeInfo[] = NodeList.getRandomActiveNodes(5)
    if (randomConsensors.length > 0) {
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

export function addSigListeners(sigint = true, sigterm = true) {
  if (sigint) {
    process.on('SIGINT', async () => {
      Logger.mainLogger.debug('Exiting on SIGINT')
      exitArchiver()
    })
  }
  if (sigterm) {
    process.on('SIGTERM', async () => {
      Logger.mainLogger.debug('Exiting on SIGTERM')
      exitArchiver()
    })
  }
  Logger.mainLogger.debug('Registerd exit signal listeners.')
}

export function removeActiveArchiver(publicKey: string) {
  activeArchivers = activeArchivers.filter((a: any) => a.publicKey !== publicKey)
  activeArchiversByPublicKeySorted = activeArchiversByPublicKeySorted.filter(
    (a: any) => a.publicKey !== publicKey
  )
}

export function resetActiveArchivers(archivers: ArchiverNodeInfo[]) {
  Logger.mainLogger.debug('Resetting active archivers.', archivers)
  activeArchivers = archivers
  activeArchiversByPublicKeySorted = [...archivers.sort(NodeList.byAscendingPublicKey)]
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
        const archiver = activeArchivers[i]
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

export function getSecretKey() {
  return nodeState.secretKey
}

export function getCurveSk() {
  return nodeState.curveSk
}

export function getCurvePk() {
  return nodeState.curvePk
}
