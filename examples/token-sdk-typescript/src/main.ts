import { DatalensClient } from "@helixbox/datalens";

import {
  buildExampleQueries,
  buildRuntimeConfig,
  formatDecodedTransfers,
  formatNativeRows,
} from "./example.js";

async function main(): Promise<void> {
  const config = buildRuntimeConfig();
  const queries = buildExampleQueries(config);
  const client = new DatalensClient({
    endpoint: config.endpoint,
    token: config.token,
    application: config.application,
  });

  const ethereum = await client.index.queryDecodedEvents(queries.ethereum);
  for (const line of formatDecodedTransfers(ethereum.nodes)) {
    console.log(line);
  }

  const solana = await client.native.query(queries.solana);
  for (const line of formatNativeRows("solana-usdc", solana)) {
    console.log(line);
  }

  const tron = await client.native.query(queries.tron);
  for (const line of formatNativeRows("tron-usdt", tron)) {
    console.log(line);
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
