import * as State from './State';
import { ConsensusNodeInfo } from './NodeList';
import 'node-fetch';
export interface ArchiverJoinRequest {
    nodeInfo: State.ArchiverNodeInfo;
}
export declare function createJoinRequest(): ArchiverJoinRequest;
export declare function addToCycleRecipients(node: ConsensusNodeInfo): Promise<void>;
export declare function removeFromCycleRecipients(node: ConsensusNodeInfo): void;
