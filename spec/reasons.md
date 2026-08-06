# SpoolQ/1 Reason Registries

## Dead Reasons

| Code | Name |
|------|------|
| 0x0000 | unspecified |
| 0x0001 | consumer_rejected |
| 0x0002 | unsupported_content_type |
| 0x0003 | administrative_bury |
| 0x0004 | attempts_exhausted |
| 0x0100-0x7fff | application-defined |
| 0x8000-0xffff | private use |

## Quarantine Reasons

| Code | Name |
|------|------|
| 0x0001 | envelope_corrupt |
| 0x0002 | payload_corrupt |
| 0x0003 | filename_parse_failed |
| 0x0004 | filename_tag_failed |
| 0x0005 | filename_header_mismatch |
| 0x0006 | unsupported_required_feature |
| 0x0007 | duplicate_state_conflict |
| 0x0008 | non_regular_file |
| 0x0009 | unexpected_hard_link |
| 0x000a | cross_device_object |
| 0x000b | impossible_state_transition |
| 0x0100-0xffff | implementation/private detail |

Corruption reasons are not mixed with ordinary dead-letter policy.
