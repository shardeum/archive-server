import * as State from './State'
import * as Crypto from './Crypto'
import * as Data from './Data/Data'
import * as NodeList from './NodeList'
import 'node-fetch'
import fetch from 'node-fetch'
import { Cycle } from './Data/Cycles'
import { StateHashes } from './Data/State'
import { ReceiptHashes } from './Data/Receipt'

export interface ArchiverJoinRequest {
  nodeInfo: State.ArchiverNodeInfo
}

export interface FirstNodeInfo {
  nodeInfo: {
    externalIp: string
    externalPort: number
    publicKey: string
  }
}

export interface FirstNodeResponse {
  nodeList: NodeList.ConsensusNodeInfo[]
  joinRequest?: ArchiverJoinRequest & Crypto.SignedMessage
  dataRequestCycle?: Data.DataRequest<Cycle> & Crypto.TaggedMessage
  dataRequestStateMetaData?: Data.DataRequest<Data.StateMetaData> & Crypto.TaggedMessage
}

export function createArchiverJoinRequest() {
  const joinRequest: ArchiverJoinRequest = {
    nodeInfo: State.getNodeInfo(),
  }
  return Crypto.sign(joinRequest)
}

export async function postJson(
  url: string,
  body: object
): Promise<Data.DataQueryResponse | null> {
  try {
    const res = await fetch(url, {
      method: 'post',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' },
    })
    if (res.ok) {
      return await res.json()
    } else {
      console.warn('postJson failed: got bad response')
      console.warn(res.headers)
      console.warn(res.statusText)
      console.warn(await res.text())
      return null
    }
  } catch (err) {
    console.warn('postJson failed: could not reach host')
    console.warn(err)
    return null
  }
}
