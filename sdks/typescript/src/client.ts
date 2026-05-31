import { DatalensIndexClient } from "./index-client.js";
import { DatalensNativeClient } from "./native-client.js";
import { GraphQLTransport } from "./transport.js";
import type { DatalensClientOptions } from "./types.js";

export class DatalensClient {
  readonly index: DatalensIndexClient;
  readonly native: DatalensNativeClient;

  constructor(options: DatalensClientOptions) {
    const transport = new GraphQLTransport(options);
    this.index = new DatalensIndexClient(transport);
    this.native = new DatalensNativeClient(transport);
  }
}
