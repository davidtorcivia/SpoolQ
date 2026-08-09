<!-- Source: spec/state-machine.json; SHA-256: e83f85741adae71434408593f076b82bac478ee42b87a9abc5d59bd38ef053fc -->

# SteadQ/1 State Machine (Generated)

## Transitions

| Operation | Source | Destination | Gen | Attempt | Token | Reason | Required syncs | Linearization | Before failure | After failure | Resolution | Notes |
|-----------|--------|-------------|-----|---------|-------|--------|----------------|---------------|----------------|---------------|------------|-------|
| enqueue_immediate | hidden | ready | zero | zero | none | none | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | probe destination: observed = committed, absent = not committed | none |
| enqueue_delayed | hidden | delayed | zero | zero | none | none | file_fsync, destination_dir_fsync | publish_noreplace | not_committed | outcome_unknown | probe destination: observed = committed, absent = not committed | none |
| promote | delayed | ready | increment | unchanged | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both: destination observed = committed, source only = not committed | none |
| claim | ready | leased | increment | increment | new | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both directories | none |
| exhausted_ready_cleanup | ready | dead | increment | unchanged | none | attempts_exhausted | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| renew | leased | leased | increment | unchanged | same | none | same_or_destination_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe destination: new generation observed = renewed, old gen observed = lease lost | none |
| acknowledge | leased | receipt | increment | unchanged | same | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe receipt buckets by exact name | none |
| retry_now | leased | ready | increment | unchanged | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| retry_later | leased | delayed | increment | unchanged | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| bury | leased | dead | increment | unchanged | none | application_defined | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | none |
| reap_expired_to_ready | leased | ready | increment | unchanged | none | none | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | attempt < maximum_attempts |
| reap_expired_to_dead | leased | dead | increment | unchanged | none | attempts_exhausted | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | attempt >= maximum_attempts |
| quarantine | active | quarantine | increment | unchanged | none | corruption | destination_dir_fsync, source_dir_fsync | rename_noreplace | not_committed | outcome_unknown | probe both | raw bytes preserved |

## Exceptional mutations

| Operation | Class | Linearization | Required syncs | Before failure | After failure | Description |
|-----------|-------|---------------|----------------|----------------|---------------|-------------|
| receipt_compaction | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname |
| wall_watermark_advancement | replacing_move | rename_replace | file_fsync, same_or_destination_dir_fsync | not_committed | outcome_unknown | Monotone wall-watermark record replaced under exclusive OFD lock |

## Administrative re-entry (creates new identity)

- **requeue_dead** (from dead): Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance (creates new identity: true)
- **requeue_quarantine** (from quarantine): Verified resubmission after full structural and payload verification: creates new job identity (creates new identity: true)
