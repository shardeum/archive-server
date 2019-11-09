const config = {
  ARCHIVER_IP: 'localhost',
  ARCHIVER_PORT: 4000,
  ARCHIVER_HASH_KEY:
    '69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc',
  ARCHIVER_PUBLIC_KEY:
    '758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_SECRET_KEY:
    '3be00019f23847529bd63e41124864983175063bb524bd54ea3c155f2fa12969758b1c119412298802cd28dbfa394cdfeecc4074492d60844cc192d632d84de3',
  ARCHIVER_EXISTING_IP: '',
  ARCHIVER_EXISTING_PORT: '',
  ARCHIVER_EXISTING_PUBLIC_KEY: '',
};

export function overrideDefaultConfig(file: string, env: {}, args: string[]) {
  // [TODO] Override config from config file
  // [TODO] Override config from env vars
  // [TODO] Override config from cli args
}

export { config };
