# SteadQ/1 Binary Format Reference

## FORMAT Record (160 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | magic = `SPQFMT1\0` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 4 | flags = 0 |
| 16 | 16 | queue_id |
| 32 | 8 | created_at_unix_ns |
| 40 | 4 | shard_count |
| 44 | 4 | reserved, zero |
| 48 | 8 | lease_bucket_width_ns |
| 56 | 8 | delayed_bucket_width_ns |
| 64 | 8 | terminal_bucket_width_ns |
| 72 | 8 | max_payload_length |
| 80 | 1 | digest_algorithm = 1 (SHA-256) |
| 81 | 1 | name_tag_bits = 64 |
| 82 | 6 | reserved, zero |
| 88 | 8 | required_feature_bits = 0 |
| 96 | 8 | optional_feature_bits = 0 |
| 104 | 24 | reserved, zero |
| 128 | 32 | format_digest |

All integers use network byte order.

## Fixed Job Header (128 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | magic = `SPQJOB1\0` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 4 | extension_header_length |
| 16 | 8 | payload_length |
| 24 | 4 | flags = 0 |
| 28 | 1 | digest_algorithm = 1 |
| 29 | 3 | reserved, zero |
| 32 | 16 | job_id |
| 48 | 4 | maximum_attempts |
| 52 | 4 | reserved, zero |
| 56 | 8 | created_at_unix_ns |
| 64 | 32 | payload_digest |
| 96 | 32 | envelope_digest |

## Compact Receipt (128 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | magic = `SPQRCPT\0` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 16 | job_id |
| 28 | 32 | envelope_digest |
| 60 | 4 | final_attempt |
| 64 | 16 | lease_token |
| 80 | 8 | receipt_bucket_start_unix_ns |
| 88 | 8 | original_payload_length |
| 96 | 32 | receipt_digest |

## Wall Watermark Record (64 bytes)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | magic = `SPQWMR1\0` |
| 8 | 2 | format major = 1 |
| 10 | 2 | format minor = 0 |
| 12 | 4 | reserved, zero |
| 16 | 8 | highest_observed_wall_bucket |
| 24 | 8 | record_sequence |
| 32 | 32 | record_digest |

## Digest Formulas

```
format_digest = SHA256("SteadQ-1-format\0" || bytes[0:128])
payload_digest = SHA256(payload)
envelope_digest = SHA256("SteadQ-1-envelope\0" || header_with_zero_env_digest || extension)
receipt_digest = SHA256("SteadQ-1-receipt\0" || bytes[0:96])
watermark_digest = SHA256("SteadQ-1-wall-watermark\0" || bytes[0:32])
name_tag = first_8_bytes(SHA256("SteadQ-1-name\0" || queue_id || context))
shard_hash = SHA256("SteadQ-1-shard\0" || queue_id || job_id)
```
