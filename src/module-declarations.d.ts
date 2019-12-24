declare module 'shardus-crypto-utils' {
  export default function(hashKey: string): void

  export type hexstring = string
  export type publicKey = hexstring
  export type secretKey = hexstring
  export type curvePublicKey = hexstring
  export type curveSecretKey = hexstring
  export type sharedKey = hexstring

  export interface Keypair {
    publicKey: publicKey
    secretKey: secretKey
  }

  export interface Signature {
    owner: publicKey
    sig: hexstring
  }

  export interface LooseObject {
    [index: string]: any
  }

  export interface TaggedObject extends LooseObject {
    tag: hexstring
  }

  export interface SignedObject extends LooseObject {
    sign: Signature
  }

  export function generateKeypair(): Keypair
  export function stringify(obj: LooseObject): string
  export function signObj(
    obj: LooseObject,
    secretKey: secretKey,
    publicKey: publicKey
  ): void
  export function verifyObj(obj: SignedObject): boolean
  export function tagObj(obj: LooseObject, sharedK: sharedKey): void
  export function authenticateObj(
    obj: TaggedObject,
    sharedK: sharedKey
  ): boolean
  export function convertSkToCurve(sk: secretKey): curveSecretKey
  export function convertPkToCurve(pk: publicKey): curvePublicKey
  export function generateSharedKey(
    curveSk: curveSecretKey,
    curvePk: curvePublicKey
  ): sharedKey
}

declare module 'minimist'
