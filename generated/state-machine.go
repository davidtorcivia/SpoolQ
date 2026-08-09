// Auto-generated from spec/state-machine.json. Do not edit by hand.
// Source SHA-256: 8eba1a6b7a3d72aca483b07f52bbb1c97bee9828a0b338cdbdaf02dbfdf1ba92

package steadq

const ProtocolIRIdentity = "steadq-state-machine"
const ProtocolIRVersion uint32 = 3

type OptionalString struct {
	Value   string
	Present bool
}

type TransitionDef struct {
	Operation                  string
	Source                     string
	SourceObjectKind           string
	Destination                string
	DestinationObjectKind      string
	GenerationChange           string
	AttemptChange              string
	TokenChange                string
	ReasonClass                OptionalString
	ClockRequirement           string
	RequiredSyncs              []string
	Linearization              string
	BeforeLinearizationFailure string
	AfterLinearizationFailure  string
	ResolverProbeTopology      string
	Qualification              string
}

type ExceptionDef struct {
	Name                       string
	Description                string
	SourceObjectKind           string
	DestinationObjectKind      string
	ClockRequirement           string
	MutationClass              string
	Linearization              string
	RequiredSyncs              []string
	BeforeLinearizationFailure string
	AfterLinearizationFailure  string
}

type UnlinkDef struct {
	Name                       string
	Description                string
	Source                     string
	SourceObjectKind           string
	SourceAuthentication       string
	ClockRequirement           string
	Qualification              string
	MutationClass              string
	Linearization              string
	RequiredSyncs              []string
	BeforeLinearizationFailure string
	AfterLinearizationFailure  string
	ResolverProbeTopology      string
}

type ReentryDef struct {
	Name               string
	Source             string
	Description        string
	CreatesNewIdentity bool
}

var Transitions = []TransitionDef{
	{Operation: "enqueue_immediate", Source: "hidden", SourceObjectKind: "full_job", Destination: "ready", DestinationObjectKind: "full_job", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"file_fsync", "destination_dir_fsync"}, Linearization: "publish_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "destination_only", Qualification: "none"},
	{Operation: "enqueue_delayed", Source: "hidden", SourceObjectKind: "full_job", Destination: "delayed", DestinationObjectKind: "full_job", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"file_fsync", "destination_dir_fsync"}, Linearization: "publish_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "destination_only", Qualification: "none"},
	{Operation: "promote", Source: "delayed", SourceObjectKind: "full_job", Destination: "ready", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "claim", Source: "ready", SourceObjectKind: "full_job", Destination: "leased", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "increment", TokenChange: "new", ReasonClass: OptionalString{}, ClockRequirement: "boottime_and_authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "exhausted_ready_cleanup", Source: "ready", SourceObjectKind: "full_job", Destination: "dead", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "attempts_exhausted", Present: true}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "renew", Source: "leased", SourceObjectKind: "full_job", Destination: "leased", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", ReasonClass: OptionalString{}, ClockRequirement: "boottime_and_authenticated_wall_floor", RequiredSyncs: []string{"same_or_destination_dir_fsync", "source_dir_fsync_if_distinct"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "acknowledge", Source: "leased", SourceObjectKind: "full_job", Destination: "receipt", DestinationObjectKind: "full_receipt", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "receipt_candidates_and_source", Qualification: "none"},
	{Operation: "retry_now", Source: "leased", SourceObjectKind: "full_job", Destination: "ready", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "none", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "retry_later", Source: "leased", SourceObjectKind: "full_job", Destination: "delayed", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "bury", Source: "leased", SourceObjectKind: "full_job", Destination: "dead", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "application_defined", Present: true}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "none"},
	{Operation: "reap_expired_to_ready", Source: "leased", SourceObjectKind: "full_job", Destination: "ready", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "lease_expiration_evidence", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "attempts_remaining"},
	{Operation: "reap_expired_to_dead", Source: "leased", SourceObjectKind: "full_job", Destination: "dead", DestinationObjectKind: "full_job", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "attempts_exhausted", Present: true}, ClockRequirement: "lease_expiration_evidence_and_authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "attempts_exhausted"},
	{Operation: "quarantine", Source: "active", SourceObjectKind: "raw_object", Destination: "quarantine", DestinationObjectKind: "raw_object", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "corruption", Present: true}, ClockRequirement: "none", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_and_destination", Qualification: "raw_bytes_preserved"},
}

var Exceptions = []ExceptionDef{
	{Name: "receipt_compaction", Description: "Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname", SourceObjectKind: "full_receipt", DestinationObjectKind: "compact_receipt", ClockRequirement: "none", MutationClass: "replacing_move", Linearization: "rename_replace", RequiredSyncs: []string{"file_fsync", "same_or_destination_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown"},
	{Name: "wall_watermark_advancement", Description: "Monotone wall-watermark record replaced under exclusive OFD lock", SourceObjectKind: "watermark_record", DestinationObjectKind: "watermark_record", ClockRequirement: "authenticated_wall_floor", MutationClass: "replacing_move", Linearization: "rename_replace", RequiredSyncs: []string{"file_fsync", "same_or_destination_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown"},
}

var Unlinks = []UnlinkDef{
	{Name: "full_receipt_retention_deletion", Description: "Authenticated retention deletion of an eligible full receipt", Source: "receipt", SourceObjectKind: "full_receipt", SourceAuthentication: "strict_receipt", ClockRequirement: "authenticated_wall_floor", Qualification: "receipt_bucket_end_plus_retention_not_after_wall_floor", MutationClass: "unlink", Linearization: "unlink", RequiredSyncs: []string{"source_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_presence"},
	{Name: "compact_receipt_retention_deletion", Description: "Authenticated retention deletion of an eligible compact receipt", Source: "receipt", SourceObjectKind: "compact_receipt", SourceAuthentication: "strict_receipt", ClockRequirement: "authenticated_wall_floor", Qualification: "receipt_bucket_end_plus_retention_not_after_wall_floor", MutationClass: "unlink", Linearization: "unlink", RequiredSyncs: []string{"source_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolverProbeTopology: "source_presence"},
}

var Reentry = []ReentryDef{
	{Name: "requeue_dead", Source: "dead", Description: "Verified resubmission: creates new job identity, copies payload and safe metadata, adds old job_id as provenance", CreatesNewIdentity: true},
	{Name: "requeue_quarantine", Source: "quarantine", Description: "Verified resubmission after full structural and payload verification: creates new job identity", CreatesNewIdentity: true},
}

func IsLegalTransition(source, destination string) bool {
	for _, t := range Transitions {
		if t.Source == source && t.Destination == destination {
			return true
		}
	}
	return false
}
