export interface Cycle {
    counter: number;
    certificate: string;
    previous: string;
    marker: string;
    start: number;
    duration: number;
    active: number;
    desired: number;
    expired: number;
    joined: string;
    joinedArchivers: string;
    joinedConsensors: string;
    activated: string;
    removed: string;
    returned: string;
    lost: string;
    refuted: string;
    apoptosized: string;
}
export declare function initStorage(dbFile: string): Promise<void>;
export declare function storeCycle(cycle: Cycle): Promise<void>;
