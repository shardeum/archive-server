export interface NodeInfo {
  publicKey: string,
  ip: string,
  port: number
}

const list: NodeInfo[] = []

export function addNode (node: NodeInfo) {
  list.push(node)
}

export function getList () {
  return list
}