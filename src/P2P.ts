import * as State from './State'
import * as Crypto from './Crypto'
import * as Data from './Data/Data'
import * as NodeList from './NodeList'
import 'node-fetch'
import fetch from 'node-fetch'
import { Cycle } from './Data/Cycles'
import { P2P as P2PTypes, StateManager } from '@shardus/types'

export enum RequestTypes {
  JOIN = 'JOIN',
  LEAVE = 'LEAVE',
}
export interface ArchiverJoinRequest {
  nodeInfo: State.ArchiverNodeInfo
  requestType: RequestTypes.JOIN
}
export interface ArchiverLeaveRequest {
  nodeInfo: State.ArchiverNodeInfo
  requestType: RequestTypes.LEAVE
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
  dataRequestStateMetaData?: Data.DataRequest<P2PTypes.SnapshotTypes.StateMetaData> & Crypto.TaggedMessage
}

export function createArchiverJoinRequest() {
  const joinRequest: ArchiverJoinRequest = {
    nodeInfo: State.getNodeInfo(),
    requestType: RequestTypes.JOIN,
  }
  return Crypto.sign(joinRequest)
}

export function createArchiverLeaveRequest() {
  const leaveRequest: ArchiverLeaveRequest = {
    nodeInfo: State.getNodeInfo(),
    requestType: RequestTypes.LEAVE,
  }
  return Crypto.sign(leaveRequest)
}

export async function postJson(url: string, body: object): Promise<Data.DataQueryResponse | null> {
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

export async function getJson(url: string): Promise<object | null> {
  try {
    const res = await fetch(url, {
      method: 'get',
      headers: { 'Content-Type': 'application/json' },
    })
    if (res.ok) {
      return await res.json()
    } else {
      console.warn('getJson failed: got bad response')
      console.warn(res.headers)
      console.warn(res.statusText)
      console.warn(await res.text())
      return null
    }
  } catch (err) {
    console.warn('getJson failed: could not reach host')
    console.warn(err)
    return null
  }
}
