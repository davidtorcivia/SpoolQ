# SpoolQ/1 State Machine (Generated)

## Transitions

| Operation | Source | Destination | Gen | Attempt | Token | No-overwrite |
|-----------|--------|-------------|-----|---------|-------|--------------|
| enqueue_immediate | hidden | ready | zero | zero | none | True |
| enqueue_delayed | hidden | delayed | zero | zero | none | True |
| promote | delayed | ready | increment | unchanged | none | True |
| claim | ready | leased | increment | increment | new | True |
| exhausted_ready_cleanup | ready | dead | increment | unchanged | none | True |
| renew | leased | leased | increment | unchanged | same | True |
| acknowledge | leased | receipt | increment | unchanged | same | True |
| retry_now | leased | ready | increment | unchanged | none | True |
| retry_later | leased | delayed | increment | unchanged | none | True |
| bury | leased | dead | increment | unchanged | none | True |
| reap_expired_to_ready | leased | ready | increment | unchanged | none | True |
| reap_expired_to_dead | leased | dead | increment | unchanged | none | True |
| quarantine | active | quarantine | increment | unchanged | none | True |

## Replacing-rename exceptions

**receipt_compaction**: Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname
**wall_watermark_advancement**: Monotone wall-watermark record replaced under exclusive OFD lock

## Administrative re-entry (creates new identity)

**requeue_dead** (from dead): Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance
**requeue_quarantine** (from quarantine): Verified resubmission after full structural and payload verification: creates new job identity
