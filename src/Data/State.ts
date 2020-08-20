export type PartitionHashes = Map<
  number,
  string
>

export type NetworkStateHash = string

export interface StateHashes {
  counter: number
  partitionHashes: PartitionHashes
  networkHash: NetworkStateHash
}