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
  networkHash: NetworkSummaryHash
}

export function processSummaryHashes(summaryHashes: SummaryHashes[]) {
  for (const item of summaryHashes) {
    // Skip if already processed [TODO] make this check more secure
    // if (stateHash.counter < currentCycleCounter) continue

    // Save the cycle to db
    Storage.storeSummaryHashes(item)
    console.log(`Processed state ${item.counter}`)
  }
}