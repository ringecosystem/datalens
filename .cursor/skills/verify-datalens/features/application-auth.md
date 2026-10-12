# Application auth and discovery

## Sub-features

- `x-datalens-application` + `Authorization: Bearer <token>` on every API route except
  `/health`.
- Per-application allowlist of chains, datasets, operations, and quota.
- `GET /v1/discovery`, `GET /v1/chains`.

## How to get to it (user POV)

Each consumer (degov-live, ormp) is configured as an application with its own token.

## Driving it with verify-datalens

```sh
source ../../../.data/verify/$RUN/state.env
curl -s -w ' %{http_code}\n' "$BASE_URL/v1/chains"                                   # 401
curl -s -w ' %{http_code}\n' -H "x-datalens-application: $APP" -H "Authorization: Bearer wrong" "$BASE_URL/v1/chains"  # 401
curl -s -w ' %{http_code}\n' -H "x-datalens-application: $APP" -H "Authorization: Bearer $TOKEN" "$BASE_URL/v1/discovery"
```

Expect error body `{"error":{"kind":"authentication_failed",...}}` for the 401s.

## Gotchas

- Path is from the skill dir; `state.env` holds the token — never paste it into reports.
- The verify app only allows `ethereum` and `evm.blocks`/`evm.logs`; other datasets are
  rejected by the allowlist, not by the planner.
