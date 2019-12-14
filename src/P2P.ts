import { state, ArchiverNodeInfo } from './State'
import { crypto } from './Crypto'

export interface ArchiverJoinRequest {
  nodeInfo: ArchiverNodeInfo
}

export function createJoinRequest(): ArchiverJoinRequest {
  const nodeInfo = { ...state.nodeInfo }
  delete nodeInfo.secretKey
  const joinRequest = {
    nodeInfo,
  }
  crypto.signObj(
    joinRequest,
    state.nodeInfo.secretKey,
    state.nodeInfo.publicKey
  )
  return joinRequest
}
