import { P2P } from "@shardus/types";
import * as Logger from '../src/Logger'
import { stringifyReduce } from "./profiler/StringifyReduce";
import * as crypto from '@shardus/crypto-utils'

const txList: Array<{ hash: string; tx: P2P.ServiceQueueTypes.AddNetworkTx }> = []

export function addTxs(addTxs: P2P.ServiceQueueTypes.AddNetworkTx[]): boolean {
  try {
    for (const addTx of addTxs) {
      const txHash = crypto.hash(addTx.txData)

      Logger.mainLogger.info(`Adding network tx of type ${addTx.type} and payload ${stringifyReduce(addTx.txData)}`)
      sortedInsert({ hash: txHash, tx: { txData: addTx.txData, type: addTx.type, cycle: addTx.cycle } })
    }
    return true
  } catch (e) {
    Logger.mainLogger.error(`ServiceQueue:addTxs: Error adding txs: ${e}`)
    return false
  }
}

export function removeTxs(removeTxs: P2P.ServiceQueueTypes.RemoveNetworkTx[]): boolean {
  try {
    for (const removeTx of removeTxs) {
      const index = txList.findIndex((entry) => entry.hash === removeTx.txHash)
      if (index === -1) {
        Logger.mainLogger.error(`TxHash ${removeTx.txHash} does not exist in txList`)
      } else {
        txList.splice(index, 1)
      }
    }
    return true
  } catch (e) {
    Logger.mainLogger.error(`ServiceQueue:removeTxs: Error removing txs: ${e}`)
    return false
  }
}

export function getNetworkTxsList(): { hash: string; tx: P2P.ServiceQueueTypes.AddNetworkTx }[] {
  return txList
}

function sortedInsert(entry: { hash: string; tx: P2P.ServiceQueueTypes.AddNetworkTx }): void {
  const index = txList.findIndex(
    (item) => item.tx.cycle > entry.tx.cycle || (item.tx.cycle === entry.tx.cycle && item.hash > entry.hash)
  )
  if (index === -1) {
    txList.push(entry)
  } else {
    txList.splice(index, 0, entry)
  }
}