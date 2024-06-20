import * as State from '../State'
import * as Logger from '../Logger'
import * as Crypto from '../Crypto'
import { postJson } from '../P2P'
import { Signature } from '@shardus/crypto-utils'
import { P2P as P2PTypes } from '@shardus/types'
import { getRandomActiveNodes } from '../NodeList'
import * as Utils from '../Utils'

// adjacentArchivers are one archiver from left and one archiver from right of the current archiver
export let adjacentArchivers: Map<string, State.ArchiverNodeInfo> = new Map()

export enum DataType {
  RECEIPT = 'RECEIPT',
  ORIGINAL_TX_DATA = 'ORIGINAL_TX_DATA',
  CYCLE = 'CYCLE',
}

export type TxData = { txId: string; timestamp: number }

export interface GossipData {
  dataType: DataType
  data: TxData[] | P2PTypes.CycleCreatorTypes.CycleData[]
  sender: string
  sign: Signature
}

// For debugging purpose, set this to true to stop gossiping tx data
const stopGossipTxData = false

const gossipToRandomArchivers = true // To gossip to random archivers in addition to adjacent archivers
const randomGossipArchiverCount = 2 // Number of random archivers to gossip to

// List of archivers that are not adjacent to the current archiver
const remainingArchivers = []

export const getAdjacentLeftAndRightArchivers = (): void => {
  if (State.activeArchivers.length <= 1) {
    adjacentArchivers = new Map()
    return
  }
  // Treat the archivers list as a circular list and get one left and one right archivers of the current archiver
  const currentArchiverIndex = State.activeArchiversByPublicKeySorted.findIndex(
    (archiver) => archiver.publicKey === State.getNodeInfo().publicKey
  )
  let leftArchiver: State.ArchiverNodeInfo | null = null
  let rightArchiver: State.ArchiverNodeInfo | null = null
  if (State.activeArchiversByPublicKeySorted.length === 2) {
    if (currentArchiverIndex === 0) rightArchiver = State.activeArchiversByPublicKeySorted[1]
    else leftArchiver = State.activeArchiversByPublicKeySorted[0]
  } else {
    let leftArchiverIndex = currentArchiverIndex - 1
    let rightArchiverIndex = currentArchiverIndex + 1
    if (leftArchiverIndex < 0) leftArchiverIndex = State.activeArchiversByPublicKeySorted.length - 1
    if (rightArchiverIndex > State.activeArchiversByPublicKeySorted.length - 1) rightArchiverIndex = 0
    /* eslint-disable security/detect-object-injection */
    leftArchiver = State.activeArchiversByPublicKeySorted[leftArchiverIndex]
    rightArchiver = State.activeArchiversByPublicKeySorted[rightArchiverIndex]
    /* eslint-enable security/detect-object-injection */
  }
  adjacentArchivers = new Map()
  if (leftArchiver) adjacentArchivers.set(leftArchiver.publicKey, leftArchiver)
  if (rightArchiver) adjacentArchivers.set(rightArchiver.publicKey, rightArchiver)
  remainingArchivers.length = 0
  for (const archiver of State.activeArchivers) {
    if (!adjacentArchivers.has(archiver.publicKey) || archiver.publicKey !== State.getNodeInfo().publicKey) {
      remainingArchivers.push(archiver)
    }
  }
}

export async function sendDataToAdjacentArchivers(
  dataType: DataType,
  data: GossipData['data']
): Promise<void> {
  if (stopGossipTxData) return
  if (adjacentArchivers.size === 0) return
  const gossipPayload = {
    dataType,
    data,
    sender: State.getNodeInfo().publicKey,
  } as GossipData
  const signedDataToSend = Crypto.sign(gossipPayload)
  try {
    Logger.mainLogger.debug(
      `Sending ${dataType} data to the archivers: ${Array.from(adjacentArchivers.values()).map(
        (n) => `${n.ip}:${n.port}`
      )}`
    )
    const promises = []
    for (const [, archiver] of adjacentArchivers) {
      const url = `http://${archiver.ip}:${archiver.port}/gossip-data`
      try {
        const GOSSIP_DATA_TIMEOUT_SECOND = 10 // 10 seconds
        const promise = postJson(url, signedDataToSend, GOSSIP_DATA_TIMEOUT_SECOND)
        promise.catch((err) => {
          Logger.mainLogger.error(`Unable to send archiver ${archiver.ip}: ${archiver.port}`, err)
        })
        promises.push(promise)
      } catch (e) {
        Logger.mainLogger.error('Error', e)
      }
    }
    if (gossipToRandomArchivers) {
      const randomArchivers = getRandomActiveNodes
    }
    try {
      await Promise.all(promises)
    } catch (err) {
      Logger.mainLogger.error('Network: ' + err)
    }
  } catch (ex) {
    Logger.mainLogger.debug(ex)
    Logger.mainLogger.debug('Fail to gossip')
  }
}

export const getArchiversToUse = (): State.ArchiverNodeInfo[] => {
  let archiversToUse: State.ArchiverNodeInfo[] = []
  const MAX_ARCHIVERS_TO_SELECT = 3
  // Choosing MAX_ARCHIVERS_TO_SELECT random archivers from the remaining archivers list
  if (State.otherArchivers.length <= MAX_ARCHIVERS_TO_SELECT) {
    archiversToUse = [...State.otherArchivers]
  } else {
    archiversToUse = Utils.getRandomItemFromArr(remainingArchivers, 0, MAX_ARCHIVERS_TO_SELECT)
    if (archiversToUse.length < MAX_ARCHIVERS_TO_SELECT) {
      const requiredArchivers = MAX_ARCHIVERS_TO_SELECT - archiversToUse.length
      // If the required archivers are not selected, then get it from the adjacent archivers
      archiversToUse = [
        ...archiversToUse,
        ...Utils.getRandomItemFromArr([...adjacentArchivers.values()], requiredArchivers),
      ]
    }
  }
  return archiversToUse
}
