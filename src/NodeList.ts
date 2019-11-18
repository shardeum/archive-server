import { config } from './Config'

export interface ConsensusNodeInfo {
  ip: string
  port: number
}

export interface SignedList {
  nodeList: ConsensusNodeInfo[]
}

const list: ConsensusNodeInfo[] = []

export function isEmpty(): boolean {
  return list.length <= 0
}

export function addNode(node: ConsensusNodeInfo) {
  list.push(node)
}

export function getList() {
  return list
}
