import * as core from '@shardus/crypto-utils'
import { SignedObject, TaggedObject, publicKey, curvePublicKey, sharedKey } from '@shardus/crypto-utils'
import { safeJsonParse, safeStringify } from '@shardus/types/build/src/utils/functions/stringify'
import * as State from './State'
import { cryptoStringify } from './utils/stringify'

// Crypto initialization fns

export function setCryptoHashKey(hashkey: string): void {
  core.init(hashkey)
  core.setCustomStringifier(safeStringify, 'shardus_types_safeStringify')
}

export const hashObj = core.hashObj

// Asymmetric Encyption Sign/Verify API
export type SignedMessage = SignedObject

export function sign<T>(obj: T): T & SignedObject {
  const objCopy = safeJsonParse(core.stringify(obj))
  core.signObj(objCopy, State.getSecretKey(), State.getNodeInfo().publicKey)
  return objCopy
}

export function verify(obj: SignedObject): boolean {
  return core.verifyObj(obj)
}

// HMAC Tag/Authenticate API

export interface TaggedMessage extends TaggedObject {
  publicKey: publicKey
}

const curvePublicKeys: Map<publicKey, curvePublicKey> = new Map()
const sharedKeys: Map<publicKey, sharedKey> = new Map()

export function getOrCreateCurvePk(pk: publicKey): curvePublicKey {
  let curvePk = curvePublicKeys.get(pk)
  if (!curvePk) {
    curvePk = core.convertPkToCurve(pk)
    curvePublicKeys.set(pk, curvePk)
  }
  return curvePk
}

export function getOrCreateSharedKey(pk: publicKey): sharedKey {
  let sharedK = sharedKeys.get(pk)
  if (!sharedK) {
    const ourCurveSk = State.getCurveSk()
    const theirCurvePk = getOrCreateCurvePk(pk)
    sharedK = core.generateSharedKey(ourCurveSk, theirCurvePk) as unknown as string
  }
  return sharedK
}

export function tag<T>(obj: T, recipientPk: publicKey): T & TaggedMessage {
  const sharedKey = getOrCreateSharedKey(recipientPk)
  const objCopy = safeJsonParse(core.stringify(obj))
  objCopy.publicKey = State.getNodeInfo().publicKey
  core.tagObj(objCopy, sharedKey)
  return objCopy
}

export function authenticate(msg: TaggedMessage): boolean {
  const sharedKey = getOrCreateSharedKey(msg.publicKey)
  return core.authenticateObj(msg, sharedKey)
}

export { core }
