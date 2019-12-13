export interface ConsensusNodeInfo {
    ip: string;
    port: number;
}
export interface SignedList {
    nodeList: ConsensusNodeInfo[];
}
export declare function isEmpty(): boolean;
export declare function addNode(node: ConsensusNodeInfo): void;
export declare function getList(): ConsensusNodeInfo[];
