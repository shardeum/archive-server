import { Config } from './Config'

export interface ArchiverNodeInfo {
  ip: string
  port: number
  publicKey: string
  secretKey?: string
}

const nodeInfo: ArchiverNodeInfo = {
  ip: '',
  port: -1,
  publicKey: '',
  secretKey: '',
}
export let existingArchivers: ArchiverNodeInfo[] = []
export let isFirst = false
export let dbFile = ''

export function initFromConfig(config: Config) {
  // Get own nodeInfo from config
  nodeInfo.ip = config.ARCHIVER_IP
  nodeInfo.port = config.ARCHIVER_PORT
  nodeInfo.publicKey = config.ARCHIVER_PUBLIC_KEY
  nodeInfo.secretKey = config.ARCHIVER_SECRET_KEY

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
  const sanitizedNodeInfo = { ...nodeInfo }
  delete sanitizedNodeInfo.secretKey
  return sanitizedNodeInfo
}

export function getSecretKey() {
  return nodeInfo.secretKey
}
