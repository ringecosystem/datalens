create extension if not exists btree_gist;

create table chains (
  chain_id integer primary key,
  name     text not null unique
);

create table selectors (
  id            bigserial primary key,
  chain_id      integer not null references chains,
  dataset       text not null,
  fingerprint   text not null,
  canonical_key text not null,
  addresses     bytea[] not null default '{}',
  topics        jsonb not null default '[]', -- each position: null = wildcard, non-empty array = one of; [] is rejected
  created_at    timestamptz not null default now(),
  last_used_at  timestamptz not null default now(),
  unique (chain_id, dataset, fingerprint)
);
create index selectors_addresses_idx on selectors using gin (addresses);

create table coverage (
  chain_id    integer not null,
  dataset     text not null,
  selector_id bigint not null references selectors,
  blocks      int8range not null,
  finality    smallint not null,
  updated_at  timestamptz not null default now(),
  exclude using gist (chain_id with =, dataset with =, selector_id with =, blocks with &&)
);

create table evm_blocks (
  chain_id    integer not null,
  number      bigint not null,
  hash        bytea not null,
  parent_hash bytea not null,
  timestamp   bigint not null,
  finality    smallint not null,
  header      jsonb not null,
  primary key (chain_id, number)
) partition by list (chain_id);

create table evm_logs (
  chain_id          integer not null,
  block_number      bigint not null,
  log_index         integer not null,
  block_hash        bytea not null,
  transaction_index integer not null,
  transaction_hash  bytea not null,
  address           bytea not null,
  topic0            bytea,
  topic1            bytea,
  topic2            bytea,
  topic3            bytea,
  data              bytea not null,
  finality          smallint not null,
  primary key (chain_id, block_number, log_index)
) partition by list (chain_id);
create index evm_logs_address_idx on evm_logs (chain_id, address, block_number);

create table evm_tx_senders (
  chain_id         integer not null,
  transaction_hash bytea not null,
  block_number     bigint not null,
  sender           bytea not null,
  primary key (chain_id, transaction_hash)
);

create table fill_jobs (
  id             bigserial primary key,
  chain_id       integer not null,
  dataset        text not null,
  selector_id    bigint not null,
  blocks         int8range not null,
  purpose        text not null,
  application_id text not null,
  status         text not null,
  attempts       integer not null default 0,
  next_run_at    timestamptz not null default now(),
  lease_until    timestamptz,
  worker_id      text,
  last_error     jsonb,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now()
);
create unique index fill_jobs_active on fill_jobs (chain_id, dataset, selector_id, blocks)
  where status in ('pending', 'running');
create index fill_jobs_status_next_run_idx on fill_jobs (status, next_run_at);

create table chain_heads (
  chain_id    integer primary key,
  latest      bigint not null,
  latest_hash bytea not null,
  safe        bigint not null,
  finalized   bigint not null,
  updated_at  timestamptz not null
);

create table subscriptions (
  chain_id       integer not null,
  dataset        text not null,
  selector_id    bigint not null,
  application_id text not null,
  source         text not null,
  last_query_at  timestamptz not null,
  expires_at     timestamptz not null,
  primary key (chain_id, dataset, selector_id, application_id)
);

create table follow_progress (
  chain_id    integer not null,
  dataset     text not null,
  selector_id bigint not null,
  from_block  bigint not null,
  head_block  bigint not null,
  head_hash   bytea not null,
  promoted_to bigint not null,
  primary key (chain_id, dataset, selector_id)
);

create table usage_daily (
  day            date not null,
  application_id text not null,
  chain_id       integer not null,
  dataset        text not null,
  queries        bigint not null default 0,
  rows_returned  bigint not null default 0,
  blocks_served  bigint not null default 0,
  blocks_filled  bigint not null default 0,
  provider_calls bigint not null default 0,
  primary key (day, application_id, chain_id, dataset)
);
