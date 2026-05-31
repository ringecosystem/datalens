package datalens

const indexedEventFields = `
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
`

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
`

const rawEventsQuery = `
query DatalensRawEvents(
	$indexName: String
	$chain: String
	$chainId: Int
	$dataset: String!
	$address: String
	$eventName: String
	$signature: String
	$fromBlock: Int
	$toBlock: Int
	$topic0: String
	$limit: Int
	$after: String
) {
	events(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, limit: $limit, after: $after) {
` + indexedEventFields + `
	}
}`

const rawEventsConnectionQuery = `
query DatalensRawEventsConnection(
	$indexName: String
	$chain: String
	$chainId: Int
	$dataset: String!
	$address: String
	$eventName: String
	$signature: String
	$fromBlock: Int
	$toBlock: Int
	$topic0: String
	$first: Int
	$after: String
) {
	eventsConnection(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, first: $first, after: $after) {
		edges {
			cursor
			node {
` + indexedEventFields + `
			}
		}
		nodes {
` + indexedEventFields + `
		}
		pageInfo {
			endCursor
			hasNextPage
		}
	}
}`

const decodedEventsQuery = `
query DatalensDecodedEvents(
	$indexName: String
	$chain: String
	$chainId: Int
	$dataset: String!
	$address: String
	$eventName: String
	$signature: String
	$fromBlock: Int
	$toBlock: Int
	$topic0: String
	$limit: Int
	$after: String
) {
	decodedEvents(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, limit: $limit, after: $after) {
` + decodedEventFields + `
	}
}`

const decodedEventsConnectionQuery = `
query DatalensDecodedEventsConnection(
	$indexName: String
	$chain: String
	$chainId: Int
	$dataset: String!
	$address: String
	$eventName: String
	$signature: String
	$fromBlock: Int
	$toBlock: Int
	$topic0: String
	$first: Int
	$after: String
) {
	decodedEventsConnection(indexName: $indexName, chain: $chain, chainId: $chainId, dataset: $dataset, address: $address, eventName: $eventName, signature: $signature, fromBlock: $fromBlock, toBlock: $toBlock, topic0: $topic0, first: $first, after: $after) {
		edges {
			cursor
			node {
` + decodedEventFields + `
			}
		}
		nodes {
` + decodedEventFields + `
		}
		pageInfo {
			endCursor
			hasNextPage
		}
	}
}`

const chainsQuery = `
query DatalensChains {
	chains
}`

const discoveryQuery = `
query DatalensDiscovery {
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
}`

const nativeQuery = `
query DatalensNativeQuery($input: QueryInput!) {
	query(input: $input) {
		chain
		datasetKey
		range
		cache
		rows
	}
}`
