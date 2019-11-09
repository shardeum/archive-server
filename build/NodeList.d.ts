export interface NodeInfo {
    ip: string;
    port: number;
}
export interface SignedList {
    nodeList: NodeInfo[];
}
export declare function isFirst(): boolean;
export declare function addNode(node: NodeInfo): void;
export declare function getSignedList(): SignedList;
