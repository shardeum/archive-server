import { Cycle, currentCycleCounter } from './Cycles'
import * as Storage from '../Storage'
export type PartitionHashes = Map<
  number,
  string
>

export type NetworkStateHash = string

export interface StateHashes {
  counter: Cycle['counter']
  partitionHashes: object
  networkHash: NetworkStateHash
}

export function processStateHashes(stateHashes: StateHashes[]) {
  for (const stateHash of stateHashes) {
    // Skip if already processed [TODO] make this check more secure
    // if (stateHash.counter < currentCycleCounter) continue

    // Save the cycle to db
    Storage.storeStateHashes(stateHash)

    console.log(`Processed state ${stateHash.counter}`)
  }
}