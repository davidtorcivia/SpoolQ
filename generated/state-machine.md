<!-- Source: spec/state-machine.json; SHA-256: ef2e7b2b1da9c377a7dc8737dd1ba8adb6275ef674315cac43c85df18ff8be3f -->

# SteadQ/1 State Machine (Generated)

## Transitions

| Operation | Source | Destination | Gen | Attempt | Token | Reason | No-overwrite |
|-----------|--------|-------------|-----|---------|-------|--------|--------------|
| enqueue_immediate | hidden | ready | zero | zero | none | none | True |
| enqueue_delayed | hidden | delayed | zero | zero | none | none | True |
| promote | delayed | ready | increment | unchanged | none | none | True |
| claim | ready | leased | increment | increment | new | none | True |
| exhausted_ready_cleanup | ready | dead | increment | unchanged | none | attempts_exhausted | True |
| renew | leased | leased | increment | unchanged | same | none | True |
| acknowledge | leased | receipt | increment | unchanged | same | none | True |
| retry_now | leased | ready | increment | unchanged | none | none | True |
| retry_later | leased | delayed | increment | unchanged | none | none | True |
| bury | leased | dead | increment | unchanged | none | application_defined | True |
| reap_expired_to_ready | leased | ready | increment | unchanged | none | none | True |
| reap_expired_to_dead | leased | dead | increment | unchanged | none | attempts_exhausted | True |
| quarantine | active | quarantine | increment | unchanged | none | corruption | True |

## Replacing-rename exceptions

**receipt_compaction**: Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname
**wall_watermark_advancement**: Monotone wall-watermark record replaced under exclusive OFD lock

## Administrative re-entry (creates new identity)

**requeue_dead** (from dead): Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance
**requeue_quarantine** (from quarantine): Verified resubmission after full structural and payload verification: creates new job identity
