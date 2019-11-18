import { Config } from './Config';
import { ConsensusNodeInfo } from './NodeList';
export interface ArchiverNodeInfo {
    ip: string;
    port: number;
    publicKey: string;
    secretKey?: string;
}
interface State {
    nodeInfo: ArchiverNodeInfo;
    existingArchivers: ArchiverNodeInfo[];
    isFirst: boolean;
    dbFile: string;
    cycleSender: ConsensusNodeInfo;
}
declare const state: State;
export declare function initStateFromConfig(config: Config): void;
export { state };
