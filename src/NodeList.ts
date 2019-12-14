import { config } from './Config'

export interface ConsensusNodeInfo {
  ip: string
  port: number
  publicKey: string
}

export interface SignedList {
  nodeList: ConsensusNodeInfo[]
}

const list: ConsensusNodeInfo[] = []

export function isEmpty(): boolean {
  return list.length <= 0
}

export function addNodes(...nodes: ConsensusNodeInfo[]) {
  list.push(...nodes)
}

export function removeNode(partial: Partial<ConsensusNodeInfo>) {
  let node
  for (let i = 0; i < list.length; i++) {
    node = list[i]
    // publicKey takes precedence
    if (partial.publicKey && partial.publicKey === node.publicKey) {
      list.splice(i, 1)
    }
    // next, ip && port
    else if (
      partial.ip &&
      partial.port &&
      partial.ip === node.ip &&
      partial.port === node.port
    ) {
      list.splice(i, 1)
    }
  }
}

export function getList() {
  return list
}
