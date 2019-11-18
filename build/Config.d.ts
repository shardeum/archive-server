/// <reference types="node" />
export interface Config {
    [index: string]: string | number | boolean;
    ARCHIVER_IP: string;
    ARCHIVER_PORT: number;
    ARCHIVER_HASH_KEY: string;
    ARCHIVER_PUBLIC_KEY: string;
    ARCHIVER_SECRET_KEY: string;
    ARCHIVER_EXISTING: string;
    ARCHIVER_DB: string;
}
declare let config: Config;
export declare function overrideDefaultConfig(file: string, env: NodeJS.ProcessEnv, args: string[]): void;
export { config };
