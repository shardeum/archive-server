import { P2P } from "@shardus/types";
import * as Logger from './Logger'
import { stringifyReduce } from "./profiler/StringifyReduce";
import * as crypto from './Crypto'

let txList: P2P.ServiceQueueTypes.NetworkTxEntry[] = []

export function addTxs(addTxs: P2P.ServiceQueueTypes.AddNetworkTx[]): boolean {
  try {
    for (const addTx of addTxs) {
      Logger.mainLogger.info(`Adding network tx of type ${addTx.type} and payload ${stringifyReduce(addTx.txData)}`)
      const { sign, ...txDataWithoutSign } = addTx.txData
      sortedInsert(txList, {
        hash: addTx.hash,
        tx: {
          hash: addTx.hash,
          txData: txDataWithoutSign,
          type: addTx.type,
          cycle: addTx.cycle,
          ...(addTx.subQueueKey && { subQueueKey: addTx.subQueueKey }),
        },
      })
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

export function setTxList(_txList: P2P.ServiceQueueTypes.NetworkTxEntry[]): void {
  txList = _txList
}

export function getTxList(): P2P.ServiceQueueTypes.NetworkTxEntry[] {
  return txList
}

export function getNetworkTxsListHash(): string {
  return crypto.hashObj(txList)
}

function sortedInsert(
  list: P2P.ServiceQueueTypes.NetworkTxEntry[],
  entry: P2P.ServiceQueueTypes.NetworkTxEntry
): void {
  const index = list.findIndex(
    (item) =>
      item.tx.cycle > entry.tx.cycle || (item.tx.cycle === entry.tx.cycle && item.hash > entry.tx.hash)
  )
  if (index === -1) {
    list.push(entry)
  } else {
    list.splice(index, 0, entry)
  }
}
