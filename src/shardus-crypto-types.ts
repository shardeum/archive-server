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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [index: string]: any
}

export interface TaggedObject extends LooseObject {
  tag: hexstring
}

export interface SignedObject extends LooseObject {
  sign: Signature
}
