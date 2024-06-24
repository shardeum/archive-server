import * as State from '../State'
import * as Logger from '../Logger'
import * as Crypto from '../Crypto'
import { postJson } from '../P2P'
import { Signature } from '@shardus/crypto-utils'
import { P2P as P2PTypes } from '@shardus/types'
import * as Utils from '../Utils'
import { config } from '../Config'

// adjacentArchivers are one archiver from left and one archiver from right of the current archiver
export let adjacentArchivers: State.ArchiverNodeInfo[] = []

export enum DataType {
  RECEIPT = 'RECEIPT',
  ORIGINAL_TX_DATA = 'ORIGINAL_TX_DATA',
  CYCLE = 'CYCLE',
}

export type TxData = { txId: string; timestamp: number }

export interface GossipData {
  dataType: DataType
  data: TxData[] | P2PTypes.CycleCreatorTypes.CycleData[]
  sign: Signature
}

// For debugging purpose, set this to true to stop gossiping tx data
const stopGossipTxData = false

// List of archivers that are not adjacent to the current archiver
const remainingArchivers = []

export const getAdjacentLeftAndRightArchivers = (): void => {
  if (State.activeArchivers.length <= 1) {
    adjacentArchivers = []
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
  adjacentArchivers.length = 0
  if (leftArchiver) adjacentArchivers.push(leftArchiver)
  if (rightArchiver) adjacentArchivers.push(rightArchiver)
  remainingArchivers.length = 0
  for (const archiver of State.otherArchivers) {
    if (!adjacentArchivers.some((a) => a.publicKey === archiver.publicKey)) {
      remainingArchivers.push(archiver)
    }
  }
}

export async function sendDataToAdjacentArchivers(
  dataType: DataType,
  data: GossipData['data']
): Promise<void> {
  if (stopGossipTxData) return
  if (State.otherArchivers.length === 0) return
  const gossipPayload = {
    dataType,
    data,
  } as GossipData
  const signedDataToSend = Crypto.sign(gossipPayload)
  try {
    const promises = []
    const archiversToSend = [...adjacentArchivers]
    if (config.gossipToMoreArchivers && remainingArchivers.length > 0) {
      const randomArchivers = Utils.getRandomItemFromArr(
        remainingArchivers,
        0,
        config.randomGossipArchiversCount
      )
      if (randomArchivers.length > 0) archiversToSend.push(...randomArchivers)
    }
    if (config.VERBOSE)
      Logger.mainLogger.debug(
        `Sending ${dataType} data to the archivers: ${archiversToSend.map((n) => `${n.ip}:${n.port}`)}`
      )
    for (const archiver of archiversToSend) {
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
    try {
      await Promise.allSettled(promises)
    } catch (err) {
      Logger.mainLogger.error('Network: ' + err)
    }
  } catch (ex) {
    Logger.mainLogger.debug(ex)
    Logger.mainLogger.debug('Fail to gossip')
  }
}
