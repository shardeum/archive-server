export declare type hexstring = string;
export declare type publicKey = hexstring;
export declare type secretKey = hexstring;
export declare type curvePublicKey = hexstring;
export declare type curveSecretKey = hexstring;
export declare type sharedKey = hexstring;
export interface Keypair {
    publicKey: publicKey;
    secretKey: secretKey;
}
export interface Signature {
    owner: publicKey;
    sig: hexstring;
}
export interface LooseObject {
    [index: string]: any;
}
export interface TaggedObject extends LooseObject {
    tag: hexstring;
}
export interface SignedObject extends LooseObject {
    sign: Signature;
}
