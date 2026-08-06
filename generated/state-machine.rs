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

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum GenerationChange { Zero, Increment, IncrementOrSame }

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AttemptChange { Zero, Increment, Unchanged }

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TokenChange { None, New, Same }

pub const TRANSITIONS: &[TransitionDef] = &[
    TransitionDef { operation: "enqueue_immediate", source: "hidden", destination: "ready", generation_change: GenerationChange::Zero, attempt_change: AttemptChange::Zero, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "enqueue_delayed", source: "hidden", destination: "delayed", generation_change: GenerationChange::Zero, attempt_change: AttemptChange::Zero, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "promote", source: "delayed", destination: "ready", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "claim", source: "ready", destination: "leased", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Increment, token_change: TokenChange::New, no_overwrite: true },
    TransitionDef { operation: "exhausted_ready_cleanup", source: "ready", destination: "dead", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "renew", source: "leased", destination: "leased", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::Same, no_overwrite: true },
    TransitionDef { operation: "acknowledge", source: "leased", destination: "receipt", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::Same, no_overwrite: true },
    TransitionDef { operation: "retry_now", source: "leased", destination: "ready", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "retry_later", source: "leased", destination: "delayed", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "bury", source: "leased", destination: "dead", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "reap_expired_to_ready", source: "leased", destination: "ready", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "reap_expired_to_dead", source: "leased", destination: "dead", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
    TransitionDef { operation: "quarantine", source: "active", destination: "quarantine", generation_change: GenerationChange::Increment, attempt_change: AttemptChange::Unchanged, token_change: TokenChange::None, no_overwrite: true },
];

/// Check if a transition from source to destination is legal.
pub fn is_legal_transition(source: &str, destination: &str) -> bool {
    TRANSITIONS.iter().any(|t| t.source == source && t.destination == destination)
}
