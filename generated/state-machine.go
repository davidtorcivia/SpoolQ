// Auto-generated from spec/state-machine.json. Do not edit by hand.
// Source SHA-256: 51784a8a723ff47b4cdc878dd1eecb55dd23ed71b2111ebfa661768a8e96b478

package steadq

type OptionalString struct {
	Value   string
	Present bool
}

type TransitionDef struct {
	Operation                  string
	Source                     string
	Destination                string
	GenerationChange           string
	AttemptChange              string
	TokenChange                string
	ReasonClass                OptionalString
	ClockRequirement           string
	RequiredSyncs              []string
	Linearization              string
	BeforeLinearizationFailure string
	AfterLinearizationFailure  string
	ResolutionBehavior         string
	Notes                      OptionalString
}

type ExceptionDef struct {
	Name                       string
	Description                string
	ClockRequirement           string
	MutationClass              string
	Linearization              string
	RequiredSyncs              []string
	BeforeLinearizationFailure string
	AfterLinearizationFailure  string
}

type ReentryDef struct {
	Name               string
	Source             string
	Description        string
	CreatesNewIdentity bool
}

var Transitions = []TransitionDef{
	{Operation: "enqueue_immediate", Source: "hidden", Destination: "ready", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"file_fsync", "destination_dir_fsync"}, Linearization: "publish_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe destination: observed = committed, absent = not committed", Notes: OptionalString{}},
	{Operation: "enqueue_delayed", Source: "hidden", Destination: "delayed", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"file_fsync", "destination_dir_fsync"}, Linearization: "publish_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe destination: observed = committed, absent = not committed", Notes: OptionalString{}},
	{Operation: "promote", Source: "delayed", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both: destination observed = committed, source only = not committed", Notes: OptionalString{}},
	{Operation: "claim", Source: "ready", Destination: "leased", GenerationChange: "increment", AttemptChange: "increment", TokenChange: "new", ReasonClass: OptionalString{}, ClockRequirement: "boottime_and_authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both directories", Notes: OptionalString{}},
	{Operation: "exhausted_ready_cleanup", Source: "ready", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "attempts_exhausted", Present: true}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{}},
	{Operation: "renew", Source: "leased", Destination: "leased", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", ReasonClass: OptionalString{}, ClockRequirement: "boottime_and_authenticated_wall_floor", RequiredSyncs: []string{"same_or_destination_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe destination: new generation observed = renewed, old gen observed = lease lost", Notes: OptionalString{}},
	{Operation: "acknowledge", Source: "leased", Destination: "receipt", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe receipt buckets by exact name", Notes: OptionalString{}},
	{Operation: "retry_now", Source: "leased", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "none", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{}},
	{Operation: "retry_later", Source: "leased", Destination: "delayed", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{}},
	{Operation: "bury", Source: "leased", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "application_defined", Present: true}, ClockRequirement: "authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{}},
	{Operation: "reap_expired_to_ready", Source: "leased", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{}, ClockRequirement: "lease_expiration_evidence", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{Value: "attempt < maximum_attempts", Present: true}},
	{Operation: "reap_expired_to_dead", Source: "leased", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "attempts_exhausted", Present: true}, ClockRequirement: "lease_expiration_evidence_and_authenticated_wall_floor", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{Value: "attempt >= maximum_attempts", Present: true}},
	{Operation: "quarantine", Source: "active", Destination: "quarantine", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", ReasonClass: OptionalString{Value: "corruption", Present: true}, ClockRequirement: "none", RequiredSyncs: []string{"destination_dir_fsync", "source_dir_fsync"}, Linearization: "rename_noreplace", BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown", ResolutionBehavior: "probe both", Notes: OptionalString{Value: "raw bytes preserved", Present: true}},
}

var Exceptions = []ExceptionDef{
	{Name: "receipt_compaction", Description: "Terminal full-job receipt replaced by byte-deterministic compact receipt at same pathname", ClockRequirement: "none", MutationClass: "replacing_move", Linearization: "rename_replace", RequiredSyncs: []string{"file_fsync", "same_or_destination_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown"},
	{Name: "wall_watermark_advancement", Description: "Monotone wall-watermark record replaced under exclusive OFD lock", ClockRequirement: "authenticated_wall_floor", MutationClass: "replacing_move", Linearization: "rename_replace", RequiredSyncs: []string{"file_fsync", "same_or_destination_dir_fsync"}, BeforeLinearizationFailure: "not_committed", AfterLinearizationFailure: "outcome_unknown"},
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
