import { DatalensClient } from "@helixbox/datalens";

import {
  buildExampleQueries,
  buildRuntimeConfig,
  runExample,
} from "./example.js";

async function main(): Promise<void> {
  const config = buildRuntimeConfig();
  const queries = buildExampleQueries(config);
  const client = new DatalensClient({
    endpoint: config.endpoint,
    token: config.token,
    application: config.application,
  });

  for (const line of await runExample(client, queries)) {
    console.log(line);
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
