import { ArchiverNodeInfo } from './State';
export interface ArchiverJoinRequest {
    nodeInfo: ArchiverNodeInfo;
}
export declare function createJoinRequest(): ArchiverJoinRequest;
