declare const config: {
    ARCHIVER_IP: string;
    ARCHIVER_PORT: number;
    ARCHIVER_HASH_KEY: string;
    ARCHIVER_PUBLIC_KEY: string;
    ARCHIVER_SECRET_KEY: string;
    ARCHIVER_EXISTING_IP: string;
    ARCHIVER_EXISTING_PORT: string;
    ARCHIVER_EXISTING_PUBLIC_KEY: string;
};
export declare function overrideDefaultConfig(file: string, env: {}, args: string[]): void;
export { config };
