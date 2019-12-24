import * as State from './State'
import * as Crypto from './Crypto'
import { ConsensusNodeInfo } from './NodeList'
import 'node-fetch'
import fetch, { FetchError, Response } from 'node-fetch'

export interface ArchiverJoinRequest {
  nodeInfo: State.ArchiverNodeInfo
}

export function createJoinRequest(): ArchiverJoinRequest {
  const nodeInfo = State.getNodeInfo()
  const joinRequest = {
    nodeInfo,
  }
  Crypto.sign(joinRequest)
  return joinRequest
}

export async function postJson(
  url: string,
  body: object
): Promise<object | null> {
  try {
    const res = await fetch(url, {
      method: 'post',
      body: JSON.stringify(body),
      headers: { 'Content-Type': 'application/json' },
    })
    if (res.ok) {
      return res.json()
    } else {
      console.warn('postJson failed: got bad response')
      console.warn(res.headers)
      console.warn(res.statusText)
      console.warn(res.text())
      return null
    }
  } catch (err) {
    console.warn('postJson failed: could not reach host')
    console.warn(err)
    return null
  }
}
