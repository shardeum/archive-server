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
    // Skip if already processed [TODO] make this check more secure
    // if (stateHash.counter < currentCycleCounter) continue
    // Save the stateHash to db
    Storage.storeStateHashes(stateHashes)
}