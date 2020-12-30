import { Cycle, currentCycleCounter } from './Cycles'
import * as Storage from '../Storage'
export type hashMap = Map<
  number,
  string
>

export type NetworkSummaryHash = string

export interface SummaryHashes {
  counter: Cycle['counter']
  summaryHashes: hashMap
  networkSummaryHash: NetworkSummaryHash
}
