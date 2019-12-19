import { Config } from './Config';
export interface ArchiverNodeInfo {
    ip: string;
    port: number;
    publicKey: string;
    secretKey?: string;
}
export declare let existingArchivers: ArchiverNodeInfo[];
export declare let isFirst: boolean;
export declare let dbFile: string;
export declare function initFromConfig(config: Config): void;
export declare function getNodeInfo(): ArchiverNodeInfo;
export declare function getSecretKey(): string | undefined;
