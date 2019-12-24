import { Config } from './Config'
import { core } from './Crypto'

export interface ArchiverNodeState {
  ip: string
  port: number
  publicKey: core.publicKey
  secretKey: core.secretKey
  curvePk: core.curvePublicKey
  curveSk: core.curveSecretKey
}

export type ArchiverNodeInfo = Omit<ArchiverNodeState, 'secretKey' | 'curveSk'>

const nodeState: ArchiverNodeState = {
  ip: '',
  port: -1,
  publicKey: '',
  secretKey: '',
  curvePk: '',
  curveSk: '',
}
export let existingArchivers: ArchiverNodeState[] = []
export let isFirst = false
export let dbFile = ''

export function initFromConfig(config: Config) {
  // Get own nodeInfo from config
  nodeState.ip = config.ARCHIVER_IP
  nodeState.port = config.ARCHIVER_PORT
  nodeState.publicKey = config.ARCHIVER_PUBLIC_KEY
  nodeState.secretKey = config.ARCHIVER_SECRET_KEY
  nodeState.curvePk = core.convertPkToCurve(nodeState.publicKey)
  nodeState.curveSk = core.convertSkToCurve(nodeState.secretKey)

  // Parse existing archivers list
  try {
    existingArchivers = JSON.parse(config.ARCHIVER_EXISTING)
  } catch (e) {
    console.warn(
      'Failed to parse ARCHIVER_EXISTING array:',
      config.ARCHIVER_EXISTING
    )
  }

  // You're first, unless existing archiver info is given
  isFirst = existingArchivers.length <= 0

  // Get db file location from config
  dbFile = config.ARCHIVER_DB
}

export function getNodeInfo(): ArchiverNodeInfo {
  const sanitizedNodeInfo = { ...nodeState }
  delete sanitizedNodeInfo.secretKey
  delete sanitizedNodeInfo.curveSk
  return sanitizedNodeInfo
}

export function getSecretKey() {
  return nodeState.secretKey
}

export function getCurveSk() {
  return nodeState.curveSk
}
