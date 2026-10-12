-- Highest block this selector's follow rows were raised to `finalized`.
alter table follow_progress add column finalized_to bigint;
update follow_progress set finalized_to = from_block - 1;
alter table follow_progress alter column finalized_to set not null;

-- A halted chain stays halted across restarts until an operator resumes it.
alter table chains
  add column follow_halted_at timestamptz,
  add column follow_halt_reason text;
