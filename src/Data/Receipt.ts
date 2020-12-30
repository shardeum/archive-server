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
