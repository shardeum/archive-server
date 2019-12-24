import * as State from './State';
import 'node-fetch';
export interface ArchiverJoinRequest {
    nodeInfo: State.ArchiverNodeInfo;
}
export declare function createJoinRequest(): ArchiverJoinRequest;
export declare function postJson(url: string, body: object): Promise<object | null>;
