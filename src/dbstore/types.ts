import { P2P, StateManager } from '@shardus/types'

export interface Cycle {
  counter: P2P.CycleCreatorTypes.CycleData['counter']
  cycleRecord: P2P.CycleCreatorTypes.CycleData
  cycleMarker: StateManager.StateMetaDataTypes.CycleMarker
}

export type DbCycle = Cycle & {
  cycleRecord: string
}
