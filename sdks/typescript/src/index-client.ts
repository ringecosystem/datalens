import type { GraphQLTransport } from "./transport.js";
import type { ConnectionPage, DecodedEvent, EventQuery, RawIndexedEvent } from "./types.js";

type DecodedEventsData = {
  decodedEventsConnection: ConnectionPage<DecodedEvent>;
};

type RawEventsData = {
  eventsConnection: ConnectionPage<RawIndexedEvent>;
};

const eventVariableDefinitions = [
  "$indexName: String",
  "$chain: String",
  "$chainId: Int",
  "$dataset: String!",
  "$address: String",
  "$eventName: String",
  "$signature: String",
  "$fromBlock: Int",
  "$toBlock: Int",
  "$topic0: String",
  "$first: Int",
  "$after: String",
].join(", ");

const eventArguments = [
  "indexName: $indexName",
  "chain: $chain",
  "chainId: $chainId",
  "dataset: $dataset",
  "address: $address",
  "eventName: $eventName",
  "signature: $signature",
  "fromBlock: $fromBlock",
  "toBlock: $toBlock",
  "topic0: $topic0",
  "first: $first",
  "after: $after",
].join(", ");

const decodedEventFields = `
  indexName
  chain
  chainId
  dataset
  blockNumber
  blockHash
  transactionHash
  transactionIndex
  logIndex
  address
  eventName
  signature
  topic0
  decodedArgs
  decodeStatus
  decodeError
  payload
  createdAt
`;

const rawEventFields = `
  indexName
  chain
  chainId
  dataset
  blockNumber
  blockHash
  transactionHash
  transactionIndex
  eventIndex
  address
  selector
  topics
  topic0
  signature
  eventName
  decoded
  data
  payload
  createdAt
`;

export class DatalensIndexClient {
  constructor(private readonly transport: GraphQLTransport) {}

  async queryDecodedEvents(query: EventQuery): Promise<ConnectionPage<DecodedEvent>> {
    const data = await this.transport.graphql<DecodedEventsData>(
      "/index/graphql",
      `
      query DatalensDecodedEvents(${eventVariableDefinitions}) {
        decodedEventsConnection(${eventArguments}) {
          edges {
            cursor
            node {
              ${decodedEventFields}
            }
          }
          nodes {
            ${decodedEventFields}
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
      `,
      compactVariables(query),
      "DatalensDecodedEvents",
    );

    return data.decodedEventsConnection;
  }

  async queryRawEvents(query: EventQuery): Promise<ConnectionPage<RawIndexedEvent>> {
    const data = await this.transport.graphql<RawEventsData>(
      "/index/graphql",
      `
      query DatalensRawEvents(${eventVariableDefinitions}) {
        eventsConnection(${eventArguments}) {
          edges {
            cursor
            node {
              ${rawEventFields}
            }
          }
          nodes {
            ${rawEventFields}
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
      `,
      compactVariables(query),
      "DatalensRawEvents",
    );

    return data.eventsConnection;
  }

  async *paginateDecodedEvents(query: EventQuery): AsyncGenerator<DecodedEvent> {
    for await (const event of paginate(query, (pageQuery) => this.queryDecodedEvents(pageQuery))) {
      yield event;
    }
  }

  async *paginateRawEvents(query: EventQuery): AsyncGenerator<RawIndexedEvent> {
    for await (const event of paginate(query, (pageQuery) => this.queryRawEvents(pageQuery))) {
      yield event;
    }
  }
}

async function* paginate<TNode>(
  query: EventQuery,
  fetchPage: (query: EventQuery) => Promise<ConnectionPage<TNode>>,
): AsyncGenerator<TNode> {
  let after = query.after;

  while (true) {
    const page = await fetchPage({
      ...query,
      after,
    });

    for (const node of page.nodes) {
      yield node;
    }

    if (!page.pageInfo.hasNextPage || page.pageInfo.endCursor == null) {
      return;
    }

    after = page.pageInfo.endCursor;
  }
}

function compactVariables(query: EventQuery): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(query).filter(([, value]) => value !== undefined),
  );
}
