export interface NodeInfo {
    publicKey: string;
    ip: string;
    port: number;
}
export declare function addNode(node: NodeInfo): void;
export declare function getList(): NodeInfo[];
