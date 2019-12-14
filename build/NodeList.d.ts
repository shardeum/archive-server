export interface ConsensusNodeInfo {
    ip: string;
    port: number;
    publicKey: string;
}
export interface SignedList {
    nodeList: ConsensusNodeInfo[];
}
export declare function isEmpty(): boolean;
export declare function addNodes(...nodes: ConsensusNodeInfo[]): void;
export declare function removeNode(partial: Partial<ConsensusNodeInfo>): void;
export declare function getList(): ConsensusNodeInfo[];
