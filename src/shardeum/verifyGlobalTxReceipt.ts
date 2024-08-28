import { P2P } from '@shardus/types'
import { isDeepStrictEqual } from 'util'
import { ArchiverReceipt } from '../dbstore/receipts'

// Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/shardeum/shardeumTypes.ts#L242
export interface SetGlobalTxValue {
  isInternalTx: boolean
  internalTXType: InternalTXType
  timestamp: number
  from: string
  change: {
    cycle: number
    change: object
  }
}

// Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/shardeum/shardeumTypes.ts#L87-L88
export enum InternalTXType {
  SetGlobalCodeBytes = 0, //Deprecated
  InitNetwork = 1,
  NodeReward = 2, //Deprecated
  ChangeConfig = 3,
  ApplyChangeConfig = 4,
  SetCertTime = 5,
  Stake = 6,
  Unstake = 7,
  InitRewardTimes = 8,
  ClaimReward = 9,
  ChangeNetworkParam = 10,
  ApplyNetworkParam = 11,
  Penalty = 12,
}

export const verifyGlobalTxAccountChange = (
  account: any,
  receipt: ArchiverReceipt,
  failedReasons = [],
  nestedCounterMessages = []
): boolean => {
  try {
    const appliedReceipt = receipt.appliedReceipt as P2P.GlobalAccountsTypes.GlobalTxReceipt
    const internalTx = appliedReceipt.tx.value as SetGlobalTxValue

    // Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/index.ts#L2440
    // Refer to https://github.com/shardeum/shardeum/blob/89db23e1d4ffb86b4353b8f37fb360ea3cd93c5b/src/index.ts#L2515
    if (
      internalTx.internalTXType === InternalTXType.ApplyChangeConfig ||
      internalTx.internalTXType === InternalTXType.ApplyNetworkParam
    ) {
      console.dir(account?.data, { depth: null })
      const latestChange = account?.data?.listOfChanges?.[account?.data?.listOfChanges?.length - 1]
      console.dir(latestChange, { depth: null })
      console.dir(internalTx.change, { depth: null })
      if (!latestChange) {
        failedReasons.push(
          `No latest change found in the global account data ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
        nestedCounterMessages.push(`No latest change found in the global account data`)
        return false
      }
      if (!isDeepStrictEqual(latestChange, internalTx.change)) {
        failedReasons.push(
          `Latest change does not match the change specified in the globalTx data ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
        )
        nestedCounterMessages.push(`Latest change does not match the change specified in the globalTx data`)
        return false
      }
      return true
    } else {
      failedReasons.push(
        `Unexpected internal transaction type in the globalModification tx ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}`
      )
      nestedCounterMessages.push(`Unexpected internal transaction type in the globalModification tx`)
      return false
    }
  } catch (error) {
    console.error(`verifyGlobalTxAccountChange error`, error)
    failedReasons.push(
      `Error while verifying global account change ${receipt.tx.txId} , ${receipt.cycle} , ${receipt.tx.timestamp}, ${error}`
    )
    nestedCounterMessages.push(`Error while verifying global account change`)
    return false
  }
}
