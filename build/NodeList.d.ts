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
export declare function removeNodes(...publicKeys: string[]): string[];
export declare function getList(): ConsensusNodeInfo[];
export declare function getNodeInfo(node: Partial<ConsensusNodeInfo>): ConsensusNodeInfo | undefined;
