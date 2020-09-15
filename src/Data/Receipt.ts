import { Cycle, currentCycleCounter } from './Cycles'
import * as Storage from '../Storage'
export type receiptMapHashes = Map<
  number,
  string
>

export type NetworkStateHash = string

export interface ReceiptHashes {
    counter: Cycle['counter']
    receiptMapHashes: object
    networkReceiptHash: NetworkStateHash
}

export function processReceiptHashes(receiptHashes: ReceiptHashes[]) {
  for (const receiptHash of receiptHashes) {
    // Skip if already processed [TODO] make this check more secure
    // if (stateHash.counter < currentCycleCounter) continue

    // Save the cycle to db
    Storage.storeReceiptHashes(receiptHash)

    console.log(`Processed receipt ${receiptHash.counter}`)
  }
}