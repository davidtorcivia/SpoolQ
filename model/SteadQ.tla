------------------------------- MODULE SteadQ -------------------------------
(************************************************************************)
(* SteadQ/1 formal model. Models the queue state machine, crash        *)
(* semantics, durability barriers, and recovery.                       *)
(*                                                                      *)
(* Bounded configuration: 2 jobs, 1 worker, 2 tokens, attempts/generation. *)
(* Crash remains available whenever its action predicate is enabled.    *)
(************************************************************************)

EXTENDS Naturals, Sequences, FiniteSets, TLC, SteadQProtocol

(* Model parameters *)
CONSTANTS 
    Jobs,            (* set of job ids *)
    Workers,         (* set of worker ids *)
    LeaseTokens,     (* finite set of lease capability ids *)
    MaxAttempts,     (* maximum attempts per job *)
    MaxGeneration,   (* maximum generation explored per job *)
    Nil

VARIABLES
    state,           (* [job -> State] *)
    generation,      (* [job -> Nat] *)
    attempt,         (* [job -> Nat] *)
    fileSynced,      (* [job -> Bool] file content durable *)
    destSynced,      (* [job -> Bool] destination dir synced *)
    srcSynced,       (* [job -> Bool] source dir synced *)
    poisoned,        (* set of poisoned handles *)
    token,           (* [job -> LeaseTokens \cup {Nil}] *)
    issuedTokens,    (* lease capabilities issued by completed claims *)
    receiptSeen      (* set of jobs that reached receipt *)

Vars == <<state, generation, attempt, fileSynced, destSynced, srcSynced,
          poisoned, token, issuedTokens, receiptSeen>>

TypeInvariant ==
    /\ state \in [Jobs -> {StateHidden, StateReady, StateLeased, StateDelayed,
                            StateDead, StateReceipt, StateQuarantine}]
    /\ generation \in [Jobs -> 0..MaxGeneration]
    /\ attempt \in [Jobs -> Nat]
    /\ fileSynced \in [Jobs -> BOOLEAN]
    /\ destSynced \in [Jobs -> BOOLEAN]
    /\ srcSynced \in [Jobs -> BOOLEAN]
    /\ poisoned \in SUBSET (Workers \cup {<<"reaper">>})
    /\ token \in [Jobs -> (LeaseTokens \cup {Nil})]
    /\ issuedTokens \in SUBSET LeaseTokens
    /\ receiptSeen \in SUBSET Jobs

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
    /\ issuedTokens = {}
    /\ receiptSeen = {}

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
    /\ UNCHANGED <<issuedTokens, receiptSeen>>

(* File sync: make file content durable *)
FileSync(j) ==
    /\ state[j] \in {StateReady, StateLeased, StateDelayed, StateDead, StateReceipt}
    /\ fileSynced' = [fileSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, destSynced, srcSynced,
                    poisoned, token, issuedTokens, receiptSeen>>

(* Destination dir sync *)
DestDirSync(j) ==
    /\ destSynced' = [destSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, fileSynced, srcSynced,
                    poisoned, token, issuedTokens, receiptSeen>>

(* Source dir sync *)
SrcDirSync(j) ==
    /\ srcSynced' = [srcSynced EXCEPT ![j] = TRUE]
    /\ UNCHANGED <<state, generation, attempt, fileSynced, destSynced,
                    poisoned, token, issuedTokens, receiptSeen>>

(* Claim: ready -> leased, issues a fresh capability independent of worker id. *)
Claim(w, t, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ t \in LeaseTokens
    /\ t \notin issuedTokens
    /\ state[j] = StateReady
    /\ attempt[j] < MaxAttempts
    /\ generation[j] < MaxGeneration
    /\ state' = [state EXCEPT ![j] = StateLeased]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ attempt' = [attempt EXCEPT ![j] = attempt[j] + 1]
    /\ fileSynced' = [fileSynced EXCEPT ![j] = TRUE]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = t]
    /\ issuedTokens' = issuedTokens \cup {t}
    /\ poisoned' = poisoned
    /\ receiptSeen' = receiptSeen

(* Renew: leased -> leased, preserving the exact capability. *)
Renew(w, t, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ t \in LeaseTokens
    /\ state[j] = StateLeased
    /\ token[j] = t
    /\ generation[j] < MaxGeneration
    /\ state' = state
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ UNCHANGED <<attempt, fileSynced, poisoned, token, issuedTokens, receiptSeen>>

(* Acknowledge: leased -> receipt, requires the exact capability. *)
Ack(w, t, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ t \in LeaseTokens
    /\ state[j] = StateLeased
    /\ token[j] = t
    /\ generation[j] < MaxGeneration
    /\ state' = [state EXCEPT ![j] = StateReceipt]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ receiptSeen' = receiptSeen \cup {j}
    /\ UNCHANGED <<attempt, fileSynced, poisoned, issuedTokens>>

(* Retry: leased -> ready, requires the exact capability. *)
RetryNow(w, t, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ t \in LeaseTokens
    /\ state[j] = StateLeased
    /\ token[j] = t
    /\ generation[j] < MaxGeneration
    /\ state' = [state EXCEPT ![j] = StateReady]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned, issuedTokens, receiptSeen>>

(* Bury: leased -> dead, requires the exact capability. *)
Bury(w, t, j) ==
    /\ w \in Workers
    /\ w \notin poisoned
    /\ t \in LeaseTokens
    /\ state[j] = StateLeased
    /\ token[j] = t
    /\ generation[j] < MaxGeneration
    /\ state' = [state EXCEPT ![j] = StateDead]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned, issuedTokens, receiptSeen>>

(* Reap expired: leased -> ready or dead *)
ReapExpired(j) ==
    /\ state[j] = StateLeased
    /\ generation[j] < MaxGeneration
    /\ IF attempt[j] >= MaxAttempts
       THEN /\ state' = [state EXCEPT ![j] = StateDead]
       ELSE /\ state' = [state EXCEPT ![j] = StateReady]
    /\ generation' = [generation EXCEPT ![j] = generation[j] + 1]
    /\ destSynced' = [destSynced EXCEPT ![j] = FALSE]
    /\ srcSynced' = [srcSynced EXCEPT ![j] = FALSE]
    /\ token' = [token EXCEPT ![j] = Nil]
    /\ UNCHANGED <<attempt, fileSynced, poisoned, issuedTokens, receiptSeen>>

(* Poison handle *)
PoisonHandle(h) ==
    /\ poisoned' = poisoned \cup {h}
    /\ UNCHANGED <<state, generation, attempt, fileSynced, destSynced,
                    srcSynced, token, issuedTokens, receiptSeen>>

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
    /\ UNCHANGED <<generation, attempt, issuedTokens, receiptSeen>>

(* Next-state relation *)
Next ==
    \/ \E j \in Jobs : Enqueue(j)
    \/ \E j \in Jobs : FileSync(j)
    \/ \E j \in Jobs : DestDirSync(j)
    \/ \E j \in Jobs : SrcDirSync(j)
    \/ \E w \in Workers, t \in LeaseTokens, j \in Jobs : Claim(w, t, j)
    \/ \E w \in Workers, t \in LeaseTokens, j \in Jobs : Renew(w, t, j)
    \/ \E w \in Workers, t \in LeaseTokens, j \in Jobs : Ack(w, t, j)
    \/ \E w \in Workers, t \in LeaseTokens, j \in Jobs : RetryNow(w, t, j)
    \/ \E w \in Workers, t \in LeaseTokens, j \in Jobs : Bury(w, t, j)
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

(* Every modeled lease carries an issued capability. *)
LeaseHasToken ==
    \A j \in Jobs :
        state[j] = StateLeased => token[j] \in issuedTokens

(* A non-null capability exists only while its job is leased. *)
TokenAuthorityRequiresLease ==
    \A j \in Jobs : token[j] # Nil => state[j] = StateLeased

(* Two active leases never share one capability. *)
ActiveLeaseTokensAreUnique ==
    \A left, right \in Jobs :
        /\ state[left] = StateLeased
        /\ state[right] = StateLeased
        /\ token[left] = token[right]
        => left = right

LeaseMutation(w, t, j) ==
    \/ Renew(w, t, j)
    \/ Ack(w, t, j)
    \/ RetryNow(w, t, j)
    \/ Bury(w, t, j)

(* An issued capability cannot mutate a job for which it is not current. *)
StaleTokenCannotMutate ==
    \A t \in issuedTokens, w \in Workers, j \in Jobs :
        t # token[j] => ~ENABLED LeaseMutation(w, t, j)

(* Modeled attempts never exceed the configured bound. *)
AttemptWithinLimit ==
    \A j \in Jobs :
        attempt[j] =< MaxAttempts

(* Every job that reached receipt remains in receipt. *)
ReceiptRemainsTerminal ==
    \A j \in receiptSeen : state[j] = StateReceipt

(* Every modeled delivery has a positive attempt. *)
DeliveredAttemptIsPositive ==
    \A j \in Jobs :
        state[j] = StateLeased => attempt[j] >= 1

(* ---- End invariants ---- *)

(* No liveness property is encoded in this model. *)

=============================================================================
