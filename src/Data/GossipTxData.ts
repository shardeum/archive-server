import * as State from '../State'
import * as Logger from '../Logger'
import { postJson } from '../P2P'
import { Signature } from 'shardus-crypto-types'
import { TxsData } from './Collector'

// adjacentArchivers are one archiver from left and one archiver from right of the current archiver
export let adjacentArchivers: Map<string, State.ArchiverNodeInfo> = new Map()

export enum TxDataType {
  RECEIPT = 'RECEIPT',
  ORIGINAL_TX_DATA = 'ORIGINAL_TX_DATA',
}

export interface GossipTxData {
  txDataType: TxDataType
  txsData: TxsData[]
  sender: string
  sign: Signature
}

export const getAdjacentLeftAndRightArchivers = () => {
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
    leftArchiver = State.activeArchiversByPublicKeySorted[leftArchiverIndex]
    rightArchiver = State.activeArchiversByPublicKeySorted[rightArchiverIndex]
  }
  adjacentArchivers = new Map()
  if (leftArchiver) adjacentArchivers.set(leftArchiver.publicKey, leftArchiver)
  if (rightArchiver) adjacentArchivers.set(rightArchiver.publicKey, rightArchiver)
}

export async function sendDataToAdjacentArchivers(txDataType: TxDataType, txsData: TxsData[]) {
  if (adjacentArchivers.size === 0) return
  const gossipPayload = {
    txDataType,
    txsData,
    sender: State.getNodeInfo().publicKey,
  } as GossipTxData
  try {
    Logger.mainLogger.debug(
      `Sending data request to these archivers: ${JSON.stringify(
        adjacentArchivers.forEach((archiver) => archiver.ip + ':' + archiver.port)
      )}`
    )
    const promises = []
    for (const [, archiver] of adjacentArchivers) {
      const url = `http://${archiver.ip}:${archiver.port}/gossip-tx-data`
      try {
        const promise = postJson(url, gossipPayload)
        promise.catch((err) => {
          Logger.mainLogger.error(`Unable to send archiver ${archiver.ip}: ${archiver.port}`, err)
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
  } catch (ex) {
    Logger.mainLogger.debug(ex)
    Logger.mainLogger.debug('Fail to gossip')
  }
}
