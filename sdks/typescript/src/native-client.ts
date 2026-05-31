import type { GraphQLTransport } from "./transport.js";
import type { Discovery, NativeQueryInput, NativeQueryResponse } from "./types.js";

type QueryData = {
  query: NativeQueryResponse;
};

type DiscoveryData = {
  discovery: Discovery;
};

type ChainsData = {
  chains: string[];
};

export class DatalensNativeClient {
  constructor(private readonly transport: GraphQLTransport) {}

  async query(input: NativeQueryInput): Promise<NativeQueryResponse> {
    const data = await this.transport.graphql<QueryData>(
      "/native/graphql",
      `
      query DatalensNativeQuery($input: QueryInput!) {
        query(input: $input) {
          chain
          datasetKey
          range
          cache
          rows
        }
      }
      `,
      { input },
      "DatalensNativeQuery",
    );

    return data.query;
  }

  async discovery(): Promise<Discovery> {
    const data = await this.transport.graphql<DiscoveryData>(
      "/native/graphql",
      `
      query DatalensNativeDiscovery {
        discovery {
          chains {
            identity
            datasets {
              datasetKey
              rangeKinds
              selectors
              enabled
            }
          }
        }
      }
      `,
      {},
      "DatalensNativeDiscovery",
    );

    return data.discovery;
  }

  async chains(): Promise<string[]> {
    const data = await this.transport.graphql<ChainsData>(
      "/native/graphql",
      `
      query DatalensNativeChains {
        chains
      }
      `,
      {},
      "DatalensNativeChains",
    );

    return data.chains;
  }
}
