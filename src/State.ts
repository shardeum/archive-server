import { Config } from './Config'
import * as Crypto from './Crypto'
import * as P2P from './P2P'

export interface ArchiverNodeState {
  ip: string
  port: number
  publicKey: Crypto.types.publicKey
  secretKey: Crypto.types.secretKey
  curvePk: Crypto.types.curvePublicKey
  curveSk: Crypto.types.curveSecretKey
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
export let existingArchivers: ArchiverNodeInfo[] = []
export let activeArchivers: ArchiverNodeInfo[] = []
export let isFirst = false
export let dbFile = ''

export async function initFromConfig(config: Config) {
  // Get own nodeInfo from config
  nodeState.ip = config.ARCHIVER_IP
  nodeState.port = config.ARCHIVER_PORT
  nodeState.publicKey = config.ARCHIVER_PUBLIC_KEY
  nodeState.secretKey = config.ARCHIVER_SECRET_KEY
  nodeState.curvePk = Crypto.core.convertPkToCurve(nodeState.publicKey)
  nodeState.curveSk = Crypto.core.convertSkToCurve(nodeState.secretKey)

  // Parse existing archivers list
  try {
    existingArchivers = config.ARCHIVER_EXISTING
  } catch (e) {
    console.warn(
      'Failed to parse ARCHIVER_EXISTING array:',
      config.ARCHIVER_EXISTING
    )
  }
  for (let i = 0; i < existingArchivers.length; i++) {
    if (existingArchivers[i].publicKey === nodeState.publicKey) {
      continue
    }
    let response:any = await P2P.getJson(
      `http://${existingArchivers[i].ip}:${existingArchivers[i].port}/nodelist`
    )
    if(response && response.nodeList) {
      // TODO: validate the reponse is from archiver
      activeArchivers.push(existingArchivers[i])
    }
  }


  // You're first, unless existing archiver info is given
  isFirst = activeArchivers.length <= 0

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

export function getCurvePk() {
  return nodeState.curvePk
}
