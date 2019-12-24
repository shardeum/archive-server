import * as core from 'shardus-crypto-utils';
export declare function setCryptoHashKey(hashkey: string): void;
export declare function sign(obj: core.LooseObject): void;
export declare function verify(obj: core.SignedObject): boolean;
export interface TaggedMessage extends core.TaggedObject {
    publicKey: core.publicKey;
}
export declare function tag(obj: core.LooseObject, recipientPk: core.publicKey): TaggedMessage;
export declare function authenticate(msg: TaggedMessage): boolean;
export { core };
