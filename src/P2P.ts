import * as State from './State'
import * as Crypto from './Crypto'
import * as Data from './Data/Data'
import * as NodeList from './NodeList'
import 'node-fetch'
import fetch from 'node-fetch'
import { P2P as P2PTypes } from '@shardus/types'
import { RequestInit, Response } from 'node-fetch'
import { SignedObject } from '@shardus/crypto-utils'
import { Utils as StringUtils } from '@shardus/types'
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { version } = require('../package.json')

export enum RequestTypes {
  JOIN = 'JOIN',
  ACTIVE = 'ACTIVE',
  LEAVE = 'LEAVE',
}
export interface ArchiverJoinRequest {
  nodeInfo: State.ArchiverNodeInfo
  appData: unknown
  requestType: RequestTypes.JOIN
  requestTimestamp: number // in ms
  cycleRecord?: P2PTypes.CycleCreatorTypes.CycleRecord
}
export interface ArchiverActiveRequest {
  nodeInfo: State.ArchiverNodeInfo
  requestType: RequestTypes.ACTIVE
}
export interface ArchiverLeaveRequest {
  nodeInfo: State.ArchiverNodeInfo
  requestType: RequestTypes.LEAVE
  requestTimestamp: number // in ms
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
  dataRequestCycle?:
    | (Data.DataRequest<P2PTypes.CycleCreatorTypes.CycleRecord> & Crypto.TaggedMessage)
    | number
  dataRequestStateMetaData?: Data.DataRequest<P2PTypes.SnapshotTypes.StateMetaData> & Crypto.TaggedMessage
}

export function createArchiverJoinRequest(): ArchiverJoinRequest & SignedObject {
  const joinRequest: ArchiverJoinRequest = {
    nodeInfo: State.getNodeInfo(),
    appData: { version },
    requestType: RequestTypes.JOIN,
    requestTimestamp: Date.now(),
  }
  return Crypto.sign(joinRequest)
}

export function createArchiverActiveRequest(): ArchiverActiveRequest & SignedObject {
  const activeRequest: ArchiverActiveRequest = {
    nodeInfo: State.getNodeInfo(),
    requestType: RequestTypes.ACTIVE,
  }
  return Crypto.sign(activeRequest)
}

export function createArchiverLeaveRequest(): ArchiverLeaveRequest & SignedObject {
  const leaveRequest: ArchiverLeaveRequest = {
    nodeInfo: State.getNodeInfo(),
    requestType: RequestTypes.LEAVE,
    requestTimestamp: Date.now(),
  }
  return Crypto.sign(leaveRequest)
}

export async function postJson(
  url: string,
  body: object,
  timeoutInSecond = 5
): Promise<(object & { success?: boolean }) | null> {
  try {
    const res = await fetch(url, {
      method: 'post',
      body: StringUtils.safeStringify(body),
      headers: { 'Content-Type': 'application/json' },
      timeout: timeoutInSecond * 1000,
    })
    if (res.ok) {
      const text = await res.text();
      try {
        return StringUtils.safeJsonParse(text);
      } catch (parseError) {
        console.warn('getJson failed: invalid JSON response');
        console.warn(url);
        console.warn(parseError);
        return null;
      }
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

export async function getJson(url: string, timeoutInSecond = 5): Promise<object | null> {
  try {
    const res = await get(url, timeoutInSecond, {
      headers: { 'Content-Type': 'application/json' },
    })
    if (res.ok) {
      const text = await res.text();
      try {
        return StringUtils.safeJsonParse(text);
      } catch (parseError) {
        console.warn('getJson failed: invalid JSON response');
        console.warn(url);
        console.warn(parseError);
        return null;
      }
    } else {
      console.warn('getJson failed: got bad response')
      console.warn(url)
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

export async function get(url: string, timeoutInSecond = 20, opts?: RequestInit): Promise<Response> {
  return fetch(url, {
    method: 'get',
    timeout: timeoutInSecond * 1000,
    ...opts,
  })
}
