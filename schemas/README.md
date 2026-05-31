# Datalens GraphQL SDK Contracts

Purpose: Define ownership rules for checked-in GraphQL schema artifacts consumed by SDKs.

Status: normative

Read this when changing Datalens GraphQL contracts or generating SDK inputs.

Not this document: SDK code generation instructions or resolver implementation details.

Defines:

- `schemas/native.graphql` is the contract artifact for the native query GraphQL API.
- `schemas/index.graphql` is the contract artifact for the index query GraphQL API.
- Rust server crates own resolver implementation and SDL export only; SDKs must consume `schemas/*.graphql` and must not import Rust server crates as protocol packages.
- Run `just export-schemas` after intentional GraphQL schema changes.
- Run `just schema-check` to verify checked-in schemas match the server implementation.
