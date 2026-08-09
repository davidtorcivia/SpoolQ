------------------------------- MODULE SteadQ -------------------------------
(************************************************************************)
(* SteadQ/1 formal model. Models the queue state machine, crash        *)
(* semantics, durability barriers, and recovery.                       *)
(*                                                                      *)
(* Bounded configuration: 2 jobs, 2 workers, and MaxAttempts = 2.      *)
(* Crash remains available whenever its action predicate is enabled.    *)
(************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, SteadQProtocol

(* Model parameters *)
CONSTANTS 
    Jobs,            (* set of job ids *)
    Workers,         (* set of worker ids *)
    MaxAttempts,     (* maximum attempts per job *)
    Nil

VARIABLES
    state,           (* [job -> State] *)
    generation,      (* [job -> Nat] *)
    attempt,         (* [job -> Nat] *)
    fileSynced,      (* [job -> Bool] file content durable *)
    destSynced,      (* [job -> Bool] destination dir synced *)
    srcSynced,       (* [job -> Bool] source dir synced *)
    poisoned,        (* set of poisoned handles *)
    token            (* [job -> Workers \cup {Nil}] *)

Vars == <<state, generation, attempt, fileSynced, destSynced, srcSynced, poisoned, token>>

TypeInvariant ==
    /\ state \in [Jobs -> {StateHidden, StateReady, StateLeased, StateDelayed,
                            StateDead, StateReceipt, StateQuarantine}]
    /\ generation \in [Jobs -> Nat]
    /\ attempt \in [Jobs -> 0..MaxAttempts]
    /\ fileSynced \in [Jobs -> BOOLEAN]
    /\ destSynced \in [Jobs -> BOOLEAN]
    /\ srcSynced \in [Jobs -> BOOLEAN]
    /\ poisoned \in SUBSET (Workers \cup {<<"reaper">>})
    /\ token \in [Jobs -> (Workers \cup {Nil})]

(* Initial state: all jobs hidden, not synced *)
Init ==
    /\ state = [j \in Jobs |-> StateHidden]
    /\ generation = [j \in Jobs |-> 0]
    /\ attempt = [j \in Jobs |-> 0]
    /\ fileSynced = [j \in Jobs |-> FALSE]
    /\ destSynced = [j \in Jobs |-> FALSE]
    /\ srcSynced = [j \in Jobs |-> FALSE]
    /\ poisoned = {}
    /\ token = [j \in Jobs |-> Nil]

(* ---- Actions ---- *)

(* Enqueue: hidden -> ready, file content is durable before publish *)
Enqueue(j) ==
    /\ state[j] = StateHidden
    /\ state' = [state EXCEPT ![j] = StateReady]
    /\ generation' = generation
    /\ attempt' = attempt
    /\ fileSynced' = [fileSynced EXCEPT ![j] = TRUE]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = srcSynced
    /\ poisoned' = poisoned
    /\ token' = [token EXCEPT ![j] = Nil]

(* File sync: make file content durable *)
FileSync(j) ==
    /\ state[j] \in {StateReady, StateLeased, StateDelayed, StateDead, StateReceipt}
    /\ fileSynced' = [fileSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, destSynced, srcSynced, poisoned, token>>

(* Destination dir sync *)
DestDirSync(j) ==
    /\ destSynced' = [destSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, fileSynced, srcSynced, poisoned, token>>

(* Source dir sync *)
SrcDirSync(j) ==
    /\ srcSynced' = [srcSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, fileSynced, destSynced, poisoned, token>>

(* Claim: ready -> leased, issues per-worker token *)
Claim(w, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ state[j] = StateReady
    /\ attempt[j] < MaxAttempts
    /\ state' = [state EXCEPT ![j] = StateLeased]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ attempt' = [attempt EXCEPT ![j] = attempt[j] + 1]
    /\ fileSynced' = [fileSynced EXCEPT ![j] = TRUE]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = w]
    /\ poisoned' = poisoned

(* Acknowledge: leased -> receipt, requires matching token *)
Ack(w, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ state[j] = StateLeased
    /\ token[j] = w
    /\ state' = [state EXCEPT ![j] = StateReceipt]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned>>

(* Retry: leased -> ready, requires matching token *)
RetryNow(w, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ state[j] = StateLeased
    /\ token[j] = w
    /\ state' = [state EXCEPT ![j] = StateReady]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned>>

(* Bury: leased -> dead, requires matching token *)
Bury(w, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ state[j] = StateLeased
    /\ token[j] = w
    /\ state' = [state EXCEPT ![j] = StateDead]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned>>

(* Reap expired: leased -> ready or dead *)
ReapExpired(j) ==
    /\ state[j] = StateLeased
    /\ IF attempt[j] >= MaxAttempts
       THEN /\ state' = [state EXCEPT ![j] = StateDead]
       ELSE /\ state' = [state EXCEPT ![j] = StateReady]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned>>

(* Poison handle *)
PoisonHandle(h) ==
    /\ poisoned' = poisoned \cup {h}
    /\ UNCHANGED <<state, generation, attempt, fileSynced, destSynced, srcSynced, token>>

(* Crash: reset volatile sync states, preserve file durability, clear stale lease token if claim never completed *)
Crash ==
    /\ fileSynced' = [j \in Jobs |->
         IF state[j] \in {StateReceipt, StateDead, StateQuarantine}
         THEN TRUE ELSE fileSynced[j]]
    /\ destSynced' = [j \in Jobs |-> FALSE]
    /\ srcSynced' = [j \in Jobs |-> FALSE]
    /\ poisoned' = {}
    /\ state' = [j \in Jobs |->
         IF state[j] = StateLeased /\ fileSynced[j]
         THEN StateLeased
         ELSE IF state[j] = StateLeased /\ ~fileSynced[j]
         THEN StateReady
         ELSE IF state[j] = StateReady /\ ~destSynced[j] /\ ~fileSynced[j]
         THEN StateHidden
         ELSE state[j]]
    /\ token' = [j \in Jobs |->
         IF state[j] = StateLeased /\ ~fileSynced[j]
         THEN Nil
         ELSE token[j]]
    /\ UNCHANGED <<generation, attempt>>

(* Next-state relation *)
Next ==
    \/ \E j \in Jobs : Enqueue(j)
    \/ \E j \in Jobs : FileSync(j)
    \/ \E j \in Jobs : DestDirSync(j)
    \/ \E j \in Jobs : SrcDirSync(j)
    \/ \E w \in Workers, j \in Jobs : Claim(w, j)
    \/ \E w \in Workers, j \in Jobs : Ack(w, j)
    \/ \E w \in Workers, j \in Jobs : RetryNow(w, j)
    \/ \E w \in Workers, j \in Jobs : Bury(w, j)
    \/ \E j \in Jobs : ReapExpired(j)
    \/ \E h \in (Workers \cup {<<"reaper">>}) : PoisonHandle(h)
    \/ Crash

Spec == Init /\ [][Next]_Vars

(* ---- Invariants ---- *)

(* Visible modeled objects retain the abstract file-durability witness. *)
CompleteVisibleEnvelope ==
    \A j \in Jobs :
        state[j] \in {StateReady, StateLeased, StateDelayed, StateDead, StateReceipt}
        => fileSynced[j]

(* The worker-valued token abstraction is present for every modeled lease. *)
LeaseHasToken ==
    \A j \in Jobs :
        state[j] = StateLeased => token[j] # Nil

(* Modeled attempts never exceed the configured bound. *)
AttemptWithinLimit ==
    \A j \in Jobs :
        attempt[j] =< MaxAttempts

(* Receipt jobs cannot execute another acknowledgment action. *)
ReceiptIsTerminal ==
    \A j \in Jobs :
        state[j] = StateReceipt => \A w \in Workers : ~ENABLED Ack(w, j)

(* Every modeled delivery has a positive attempt. *)
DeliveredAttemptIsPositive ==
    \A j \in Jobs :
        state[j] = StateLeased => attempt[j] >= 1

(* ---- End invariants ---- *)

(* No liveness property is encoded in this model. *)

(* Worker identity stands in for a token. This abstraction cannot establish
   stale-capability exclusion when one worker can hold multiple lease tokens. *)

=============================================================================
