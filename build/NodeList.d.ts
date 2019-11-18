export interface NodeInfo {
    ip: string;
    port: number;
}
export interface SignedList {
    nodeList: NodeInfo[];
}
export declare function isEmpty(): boolean;
export declare function addNode(node: NodeInfo): void;
export declare function getList(): NodeInfo[];
