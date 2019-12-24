export declare enum Statuses {
    ACTIVE = "active",
    SYNCING = "syncing"
}
export interface ConsensusNodeInfo {
    ip: string;
    port: number;
    publicKey: string;
}
export interface SignedList {
    nodeList: ConsensusNodeInfo[];
}
export declare function isEmpty(): boolean;
export declare function addNodes(status: Statuses, ...nodes: ConsensusNodeInfo[]): void;
export declare function removeNodes(...publicKeys: string[]): string[];
export declare function setStatus(status: Statuses, ...publicKeys: string[]): void;
export declare function getList(): ConsensusNodeInfo[];
export declare function getActiveList(): ConsensusNodeInfo[];
export declare function getSyncingList(): ConsensusNodeInfo[];
export declare function getNodeInfo(node: Partial<ConsensusNodeInfo>): ConsensusNodeInfo | undefined;
