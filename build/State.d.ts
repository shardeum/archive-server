import { Config } from './Config';
import * as Crypto from './Crypto';
export interface ArchiverNodeState {
    ip: string;
    port: number;
    publicKey: Crypto.types.publicKey;
    secretKey: Crypto.types.secretKey;
    curvePk: Crypto.types.curvePublicKey;
    curveSk: Crypto.types.curveSecretKey;
}
export declare type ArchiverNodeInfo = Omit<ArchiverNodeState, 'secretKey' | 'curveSk'>;
export declare let existingArchivers: ArchiverNodeState[];
export declare let isFirst: boolean;
export declare let dbFile: string;
export declare function initFromConfig(config: Config): void;
export declare function getNodeInfo(): ArchiverNodeInfo;
export declare function getSecretKey(): string;
export declare function getCurveSk(): string;
