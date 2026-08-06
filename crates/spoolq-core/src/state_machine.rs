// Auto-generated from spec/state-machine.json. Do not edit by hand.

pub struct TransitionDef {
    pub operation: &'static str,
    pub source: &'static str,
    pub destination: &'static str,
    pub generation_change: GenerationChange,
    pub attempt_change: AttemptChange,
    pub token_change: TokenChange,
    pub no_overwrite: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenerationChange {
    Zero,
    Increment,
    IncrementOrSame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptChange {
    Zero,
    Increment,
    Unchanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenChange {
    None,
    New,
    Same,
}

pub const TRANSITIONS: &[TransitionDef] = &[
    TransitionDef {
        operation: "enqueue_immediate",
        source: "hidden",
        destination: "ready",
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "enqueue_delayed",
        source: "hidden",
        destination: "delayed",
        generation_change: GenerationChange::Zero,
        attempt_change: AttemptChange::Zero,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "promote",
        source: "delayed",
        destination: "ready",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "claim",
        source: "ready",
        destination: "leased",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Increment,
        token_change: TokenChange::New,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "exhausted_ready_cleanup",
        source: "ready",
        destination: "dead",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "renew",
        source: "leased",
        destination: "leased",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "acknowledge",
        source: "leased",
        destination: "receipt",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::Same,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "retry_now",
        source: "leased",
        destination: "ready",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "retry_later",
        source: "leased",
        destination: "delayed",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "bury",
        source: "leased",
        destination: "dead",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "reap_expired_to_ready",
        source: "leased",
        destination: "ready",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "reap_expired_to_dead",
        source: "leased",
        destination: "dead",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
    TransitionDef {
        operation: "quarantine",
        source: "active",
        destination: "quarantine",
        generation_change: GenerationChange::Increment,
        attempt_change: AttemptChange::Unchanged,
        token_change: TokenChange::None,
        no_overwrite: true,
    },
];

/// Check if a transition from source to destination is legal.
pub fn is_legal_transition(source: &str, destination: &str) -> bool {
    TRANSITIONS
        .iter()
        .any(|t| t.source == source && t.destination == destination)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legal_transitions() {
        assert!(is_legal_transition("hidden", "ready"));
        assert!(is_legal_transition("hidden", "delayed"));
        assert!(is_legal_transition("delayed", "ready"));
        assert!(is_legal_transition("ready", "leased"));
        assert!(is_legal_transition("ready", "dead"));
        assert!(is_legal_transition("leased", "leased"));
        assert!(is_legal_transition("leased", "receipt"));
        assert!(is_legal_transition("leased", "ready"));
        assert!(is_legal_transition("leased", "delayed"));
        assert!(is_legal_transition("leased", "dead"));
        assert!(is_legal_transition("active", "quarantine"));
    }

    #[test]
    fn illegal_transitions() {
        assert!(!is_legal_transition("receipt", "ready"));
        assert!(!is_legal_transition("dead", "ready"));
        assert!(!is_legal_transition("quarantine", "ready"));
        assert!(!is_legal_transition("ready", "ready"));
        assert!(!is_legal_transition("hidden", "leased"));
        assert!(!is_legal_transition("ready", "receipt"));
    }

    #[test]
    fn transition_count() {
        assert_eq!(TRANSITIONS.len(), 13);
    }

    #[test]
    fn all_transitions_use_no_overwrite() {
        for t in TRANSITIONS {
            assert!(
                t.no_overwrite,
                "transition {} must use no-overwrite",
                t.operation
            );
        }
    }

    #[test]
    fn claim_increments_attempt() {
        let claim = TRANSITIONS.iter().find(|t| t.operation == "claim").unwrap();
        assert_eq!(claim.attempt_change, AttemptChange::Increment);
        assert_eq!(claim.generation_change, GenerationChange::Increment);
        assert_eq!(claim.token_change, TokenChange::New);
    }

    #[test]
    fn ack_does_not_change_attempt() {
        let ack = TRANSITIONS
            .iter()
            .find(|t| t.operation == "acknowledge")
            .unwrap();
        assert_eq!(ack.attempt_change, AttemptChange::Unchanged);
    }

    #[test]
    fn renew_preserves_token() {
        let renew = TRANSITIONS.iter().find(|t| t.operation == "renew").unwrap();
        assert_eq!(renew.token_change, TokenChange::Same);
        assert_eq!(renew.attempt_change, AttemptChange::Unchanged);
    }
}
