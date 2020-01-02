import * as State from './State';
import * as Crypto from './Crypto';
import * as Data from './Data/Data';
import * as NodeList from './NodeList';
import 'node-fetch';
import { Cycle } from './Data/Cycles';
export interface ArchiverJoinRequest {
    nodeInfo: State.ArchiverNodeInfo;
}
export interface FirstNodeInfo {
    nodeInfo: {
        ip: string;
        externalPort: number;
        publicKey: string;
    };
    firstCycleMarker: string;
}
export interface FirstNodeResponse {
    nodeList: NodeList.ConsensusNodeInfo[];
    joinRequest?: ArchiverJoinRequest & Crypto.SignedMessage;
    dataRequest?: Data.DataRequest<Cycle> & Crypto.TaggedMessage;
}
export declare function createArchiverJoinRequest(): ArchiverJoinRequest & Crypto.types.SignedObject;
export declare function postJson(url: string, body: object): Promise<object | null>;
