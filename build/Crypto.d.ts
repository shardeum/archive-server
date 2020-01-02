import core = require('shardus-crypto-utils');
import * as cryptoTypes from './shardus-crypto-types';
export declare function setCryptoHashKey(hashkey: string): void;
export declare type SignedMessage = cryptoTypes.SignedObject;
export declare function sign<T>(obj: T): T & cryptoTypes.SignedObject;
export declare function verify(obj: cryptoTypes.SignedObject): boolean;
export interface TaggedMessage extends cryptoTypes.TaggedObject {
    publicKey: cryptoTypes.publicKey;
}
export declare function tag<T>(obj: T, recipientPk: cryptoTypes.publicKey): T & TaggedMessage;
export declare function authenticate(msg: TaggedMessage): boolean;
export { core, cryptoTypes as types };
