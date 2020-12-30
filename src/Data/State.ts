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

