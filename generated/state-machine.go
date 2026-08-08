// Auto-generated from spec/state-machine.json. Do not edit by hand.
// Source SHA-256: 18e8cb0389bd8eee97a94b5afbb9d7ad466426c20f1665deb6a277f291d0ecb1

package steadq

type TransitionDef struct {
	Operation       string
	Source           string
	Destination      string
	GenerationChange string
	AttemptChange    string
	TokenChange      string
	NoOverwrite      bool
}

var Transitions = []TransitionDef{
	{Operation: "enqueue_immediate", Source: "hidden", Destination: "ready", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", NoOverwrite: true},
	{Operation: "enqueue_delayed", Source: "hidden", Destination: "delayed", GenerationChange: "zero", AttemptChange: "zero", TokenChange: "none", NoOverwrite: true},
	{Operation: "promote", Source: "delayed", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "claim", Source: "ready", Destination: "leased", GenerationChange: "increment", AttemptChange: "increment", TokenChange: "new", NoOverwrite: true},
	{Operation: "exhausted_ready_cleanup", Source: "ready", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "renew", Source: "leased", Destination: "leased", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", NoOverwrite: true},
	{Operation: "acknowledge", Source: "leased", Destination: "receipt", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "same", NoOverwrite: true},
	{Operation: "retry_now", Source: "leased", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "retry_later", Source: "leased", Destination: "delayed", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "bury", Source: "leased", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "reap_expired_to_ready", Source: "leased", Destination: "ready", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "reap_expired_to_dead", Source: "leased", Destination: "dead", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
	{Operation: "quarantine", Source: "active", Destination: "quarantine", GenerationChange: "increment", AttemptChange: "unchanged", TokenChange: "none", NoOverwrite: true},
}

func IsLegalTransition(source, destination string) bool {
	for _, t := range Transitions {
		if t.Source == source && t.Destination == destination {
			return true
		}
	}
	return false
}
