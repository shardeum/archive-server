import { state, ArchiverNodeInfo } from './State'
import { crypto } from './Crypto'

interface ArchiverJoinRequest {
  nodeInfo: ArchiverNodeInfo
}

export function createJoinRequest() {
  const nodeInfo = { ...state.nodeInfo }
  delete nodeInfo.secretKey
  const joinRequest = {
    nodeInfo,
  }
  crypto.signObj(joinRequest, state.nodeInfo.secretKey, state.nodeInfo.publicKey)
  return joinRequest
}
