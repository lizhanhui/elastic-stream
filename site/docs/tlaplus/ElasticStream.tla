------------------------- MODULE ElasticStream -------------------------
EXTENDS MessagePassing, Naturals, FiniteSets, FiniteSetsExt, Sequences, SequencesExt, Integers, TLC

(*
This specification formally describes the ElasticStream protocol. 
The scope of the specification is the lifetime of a single stream.
*)

\* Input parameters
CONSTANTS RangeServers,                 \* The RangeServers available e.g. { r1, r2, r3, r4 }
          Clients,                      \* The clients e.g. {c1, c2}
          WriteQuorum,                  \* The number of copies
          AckQuorum,                    \* The acknowledgement number for the replicas
          SendLimit,                    \* The data items to send. Limited to a very small number of data items
                                        \* in order to keep the state space small. E.g 1 or 2
          InflightLimit                 \* Limit the number of unacknowledged add entry requests by the client
                                        \* which can reduce to state space significantly

\* Model values
CONSTANTS Nil,
          NoSuchEntry,
          Unknown,
          OK,
          NeedMoreResponses,
          STATUS_OPEN,
          STATUS_IN_RECOVERY,
          STATUS_CLOSED,
          CLIENT_WITHDRAWN,
          RECOVERY_ABORTED

\* Stream state in the pm store
VARIABLES pd_status,              \* the stream status
          pd_ranges,              \* the ranges of the stream
          pd_last_entry,          \* the endpoint of the stream (set on closing)
          pd_version              \* stream metadata version (incremented on each update)

\* RangeServer state
VARIABLES r_entries,                \* the entries stored in each range server
          r_fenced,                 \* the fenced status of the stream on each range server (TRUE/FALSE)
          r_lac                     \* the last add confirmed of the stream on each range server

\* the state of the clients
VARIABLES clients                   \* the set of clients

ASSUME SendLimit \in Nat
ASSUME SendLimit > 0
ASSUME WriteQuorum \in Nat
ASSUME WriteQuorum > 0
ASSUME AckQuorum \in Nat
ASSUME AckQuorum > 0
ASSUME WriteQuorum >= AckQuorum

range_server_vars == << r_fenced, r_entries, r_lac >>
pd_vars == << pd_status, pd_ranges, pd_last_entry, pd_version >>
vars == << range_server_vars, clients, pd_vars, messages >>

(***************************************************************************)
(* Recovery Phases                                                         *)
(***************************************************************************)
NotStarted == 0
FencingPhase == 1
ReadWritePhase == 2

(***************************************************************************)
(* Records                                                                 *)
(***************************************************************************)

EntryIds ==
    1..SendLimit

NilEntry == [id |-> 0, data |-> Nil]

Entry ==
    [id: EntryIds, data: EntryIds \union {Nil}]

Range ==
    [id: Nat, ensemble: SUBSET RangeServers, first_entry_id: Nat]

PendingAddOp ==
    [entry: Entry, range_id: Nat, ensemble: SUBSET RangeServers]

ClientStatuses ==
    {Nil, STATUS_OPEN, STATUS_IN_RECOVERY, STATUS_CLOSED, CLIENT_WITHDRAWN}
    
ReadResponses ==
    { OK, NoSuchEntry, Unknown } 

ClientState ==
    [id: Clients,
     pd_version: Nat \union {Nil},              \* The metadata version this client has
     status: ClientStatuses,                    \* The possible statuses of a client
     ranges: [Nat -> Range],                    \* The ranges of the stream known by the client
     curr_range: Range \union {Nil},            \* The current range known by a client
     pending_add_ops: SUBSET PendingAddOp,      \* The pending add operations of a client
     lap: Nat,                                  \* The Last Add Pushed of a client
     lac: Nat,                                  \* The Last Add Confirmed of a client
     confirmed: [EntryIds -> SUBSET RangeServers],   \* The range servers that have confirmed each entry id
     fenced: SUBSET RangeServers,                    \* The range servers that have confirmed they are fenced to this client

     recovery_ensemble: SUBSET RangeServers,         \* The ensemble of the last range at the beginning of recovery
                                                \* where all read recovery requests are sent
     curr_read_entry: Entry \union {Nil},       \* The entry currently being read (during recovery)
     read_responses: SUBSET ReadResponses,      \* The recovery read responses of the current entry
     recovery_phase: 0..ReadWritePhase,         \* The recovery phases
     last_recoverable_entry: Nat \union {Nil}]  \* The last recoverable entry set to the lowest negative
                                                \* recovery read - 1 

InitClient(cid) ==
    [id                     |-> cid,
     pd_version           |-> Nil,
     status                 |-> Nil,
     curr_range          |-> Nil,
     ranges              |-> <<>>,
     pending_add_ops        |-> {},
     lap                    |-> 0,
     lac                    |-> 0,
     confirmed              |-> [id \in EntryIds |-> {}],
     fenced                 |-> {},
     recovery_ensemble      |-> {},
     curr_read_entry        |-> Nil,
     read_responses         |-> {},
     recovery_phase         |-> 0,
     last_recoverable_entry |-> Nil]

(***************************************************************************)
(* Range Helpers                                                        *)
(***************************************************************************)

RangeOf(range_id, ranges) ==
    ranges[range_id]

RangeOfEntryId(entry_id, ranges) ==
    IF Len(ranges) = 1
    THEN ranges[1]
    ELSE IF Last(ranges).first_entry_id <= entry_id
         THEN Last(ranges)
         ELSE
            LET r_id == CHOOSE f1 \in DOMAIN ranges :
                            \E f2 \in DOMAIN ranges :
                                /\ ranges[f1].id = ranges[f2].id-1
                                /\ ranges[f1].first_entry_id <= entry_id
                                /\ ranges[f2].first_entry_id > entry_id
            IN ranges[r_id]

ValidEnsemble(ensemble, include_range_servers, exclude_range_servers) ==
    /\ Cardinality(ensemble) = WriteQuorum
    /\ ensemble \intersect exclude_range_servers = {}
    /\ include_range_servers \intersect ensemble = include_range_servers
    /\ \A i \in DOMAIN pd_ranges : ensemble # pd_ranges[i].ensemble

EnsembleAvailable(include_range_servers, exclude_range_servers) ==
    \E ensemble \in SUBSET RangeServers :
        ValidEnsemble(ensemble, include_range_servers, exclude_range_servers)

SelectEnsemble(include_range_servers, exclude_range_servers) ==
    CHOOSE ensemble \in SUBSET RangeServers :
        ValidEnsemble(ensemble, include_range_servers, exclude_range_servers)
        
HasQuorumCoverage(s, cohort_size) ==
    Cardinality(s) >= ((cohort_size - AckQuorum) + 1)
    
(***************************************************************************
ACTION: Create stream                                                   
****************************************************************************)

ClientCreatesStream(cid) ==
    /\ pd_status = Nil
    /\ clients[cid].pd_version = Nil
    /\ LET range == [id |-> 1, ensemble |-> SelectEnsemble({},{}), first_entry_id |-> 1]
       IN
        /\ clients' = [clients EXCEPT ![cid] = 
                                [@ EXCEPT !.status = STATUS_OPEN,
                                          !.pd_version = 1,
                                          !.ranges = Append(pd_ranges, range),
                                          !.curr_range = range]]
        /\ pd_status' = STATUS_OPEN
        /\ pd_version' = 1
        /\ pd_ranges' = Append(pd_ranges, range)
    /\ UNCHANGED << range_server_vars, pd_last_entry, messages >>

(**************************************************************************
ACTION: Send Add Entry Requests                                                   
***************************************************************************)

GetAddEntryRequests(c, entry, ensemble, recovery) ==
    { [type     |-> AddEntryRequestMessage,
       range_server   |-> b,
       cid      |-> c.id,
       entry    |-> entry,
       lac      |-> c.lac,
       recovery |-> recovery] : b \in ensemble }

SendAddEntryRequests(c, entry) ==
    /\ SendMessagesToEnsemble(GetAddEntryRequests(c,
                                                  entry,
                                                  c.curr_range.ensemble,
                                                  FALSE))
    /\ clients' = [clients EXCEPT ![c.id] =  
                                [c EXCEPT !.lap = entry.id,
                                          !.pending_add_ops = @ \union 
                                               {[entry       |-> entry,
                                                 range_id |-> c.curr_range.id,
                                                 ensemble    |-> c.curr_range.ensemble]}]]

ClientSendsAddEntryRequests(cid) ==
    LET c == clients[cid]
    IN
        /\ c.status = STATUS_OPEN
        /\ c.lap - c.lac <= InflightLimit - 1 
        /\ LET entry_data == c.lap + 1
           IN
            /\ entry_data <= SendLimit
            /\ SendAddEntryRequests(c, [id   |-> entry_data, data |-> entry_data])
        /\ UNCHANGED << range_server_vars, pd_vars >>

(**************************************************************************
ACTION: A range server receives an AddEntryRequestMessage, sends a confirm.             
***************************************************************************)

GetAddEntryResponse(add_entry_msg, success) ==
    [type     |-> AddEntryResponseMessage,
     range_server   |-> add_entry_msg.range_server,
     cid      |-> add_entry_msg.cid,
     entry    |-> add_entry_msg.entry,
     recovery |-> add_entry_msg.recovery,
     success  |-> success]

RangeServerSendsAddConfirmedResponse ==
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, AddEntryRequestMessage)
        /\ IsEarliestMsg(msg)
        /\ \/ r_fenced[msg.range_server] = FALSE
           \/ msg.recovery = TRUE
        /\ r_entries' = [r_entries EXCEPT ![msg.range_server] = @ \union {msg.entry}]
        /\ r_lac' = [r_lac EXCEPT ![msg.range_server] = msg.lac]
        /\ ProcessedOneAndSendAnother(msg, GetAddEntryResponse(msg, TRUE))
        /\ UNCHANGED << r_fenced, clients, pd_vars >>

(***************************************************************************
ACTION: A range server receives an AddEntryRequestMessage, sends a fenced response.                                                                                      
****************************************************************************)

RangeServerSendsAddFencedResponse ==
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, AddEntryRequestMessage)
        /\ msg.recovery = FALSE
        /\ r_fenced[msg.range_server] = TRUE
        /\ IsEarliestMsg(msg)
        /\ ProcessedOneAndSendAnother(msg, GetAddEntryResponse(msg, FALSE))
        /\ UNCHANGED << range_server_vars, clients, pd_vars >>

(***************************************************************************
ACTION: A client receive a success AddEntryResponseMessage              
****************************************************************************)

AddToConfirmed(c, entry_id, range_server) ==
    [c.confirmed EXCEPT ![entry_id] = @ \union {range_server}]

\* Only remove if it has reached the Ack Quorum and <= LAC
MayBeRemoveFromPending(c, confirmed, lac) ==
    { op \in c.pending_add_ops :
        \/ Cardinality(confirmed[op.entry.id]) < AckQuorum
        \/ op.entry.id > lac
    }

MaxContiguousConfirmedEntry(confirmed) ==
    IF \E id \in DOMAIN confirmed : \A id1 \in 1..id :
                                        Cardinality(confirmed[id1]) >= AckQuorum
    THEN Max({id \in DOMAIN confirmed :
                \A id1 \in 1..id :
                    Cardinality(confirmed[id1]) >= AckQuorum
             })
    ELSE 0

ReceiveAddConfirmedResponse(c, is_recovery) ==
    \E msg \in DOMAIN messages :
        /\ msg.cid = c.id
        /\ ReceivableMessageOfType(messages, msg, AddEntryResponseMessage)
        /\ IsEarliestMsg(msg)
        /\ msg.recovery = is_recovery
        /\ msg.success = TRUE
        /\ msg.range_server \in c.curr_range.ensemble
        /\ LET confirmed == AddToConfirmed(c, msg.entry.id, msg.range_server)
           IN LET lac == MaxContiguousConfirmedEntry(confirmed)
              IN
                clients' = [clients EXCEPT ![c.id] = 
                                        [c EXCEPT !.confirmed = confirmed,
                                                  !.lac = IF lac > @ THEN lac ELSE @,
                                                  !.pending_add_ops = MayBeRemoveFromPending(c, confirmed, lac)]]
        /\ MessageProcessed(msg)


ClientReceivesAddConfirmedResponse(cid) ==
    /\ clients[cid].status = STATUS_OPEN
    /\ ReceiveAddConfirmedResponse(clients[cid], FALSE)
    /\ UNCHANGED << range_server_vars, pd_vars >>

(***************************************************************************
ACTION: A client receives a fenced AddEntryResponseMessage              
****************************************************************************)

ClientReceivesAddFencedResponse(cid) ==
    /\ clients[cid].status = STATUS_OPEN
    /\ \E msg \in DOMAIN messages :
        /\ msg.cid = cid
        /\ ReceivableMessageOfType(messages, msg, AddEntryResponseMessage)
        /\ IsEarliestMsg(msg)
        /\ msg.recovery = FALSE
        /\ msg.success = FALSE
        /\ msg.range_server \in clients[cid].curr_range.ensemble
        /\ clients' = [clients EXCEPT ![cid] = [@ EXCEPT !.status = CLIENT_WITHDRAWN]]
        /\ MessageProcessed(msg)
        /\ UNCHANGED << range_server_vars, pd_vars >>

(***************************************************************************
ACTION: A client performs an ensemble change.                           
****************************************************************************)

NoPendingResends(c) ==
    ~\E op \in c.pending_add_ops :
        /\ \/ op.range_id # c.curr_range.id
           \/ op.ensemble # c.curr_range.ensemble


UpdatedRanges(c, first_entry_id, new_ensemble) ==
    IF first_entry_id = c.curr_range.first_entry_id
    THEN [index \in DOMAIN c.ranges |-> IF c.ranges[index] = Last(c.ranges)
                                              THEN [c.ranges[index] EXCEPT !.ensemble = new_ensemble]
                                              ELSE c.ranges[index]]
    ELSE Append(c.ranges, [id             |-> Len(c.ranges)+1,
                              ensemble       |-> new_ensemble,
                              first_entry_id |-> first_entry_id])

ChangeEnsemble(c, recovery) ==
    /\ NoPendingResends(c)
    /\ \/ recovery
       \/ ~recovery /\ c.pd_version = pd_version
    /\ \E failed_range_servers \in SUBSET c.curr_range.ensemble :
        /\ \A range_server \in failed_range_servers : WriteTimeoutForRangeServer(messages, c.id, range_server, recovery)
        /\ EnsembleAvailable(c.curr_range.ensemble \ failed_range_servers, failed_range_servers)
        /\ LET first_entry_id == c.lac + 1
           IN
              /\ LET new_ensemble   == SelectEnsemble(c.curr_range.ensemble \ failed_range_servers, failed_range_servers)
                     updated_ranges == UpdatedRanges(c, first_entry_id, new_ensemble)
                 IN
                    \* only update the metadata if not in stream recovery
                    /\ IF recovery 
                       THEN UNCHANGED << pd_version, pd_ranges >>
                       ELSE /\ pd_ranges' = updated_ranges
                            /\ pd_version' = pd_version + 1
                    /\ clients' = [clients EXCEPT ![c.id] =  
                                        [c EXCEPT !.pd_version  = IF recovery THEN @ ELSE pd_version + 1,
                                                  !.confirmed     = [id \in DOMAIN c.confirmed |->
                                                                       IF id >= first_entry_id
                                                                       THEN c.confirmed[id] \ failed_range_servers
                                                                       ELSE c.confirmed[id]],
                                                  !.ranges     = updated_ranges,
                                                  !.curr_range = Last(updated_ranges)]]
                    /\ ClearWriteTimeout(c.id, failed_range_servers, recovery)

ClientChangesEnsemble(cid) ==
    /\ clients[cid].status = STATUS_OPEN
    /\ pd_status = STATUS_OPEN
    /\ ChangeEnsemble(clients[cid], FALSE)
    /\ UNCHANGED <<  range_server_vars, pd_status, pd_last_entry >>

(***************************************************************************
ACTION: Client resends a Pending Add Op                                 
****************************************************************************)

NeedsResend(op, curr_range) ==
    \/ op.range_id # curr_range.id
    \/ op.ensemble # curr_range.ensemble

\* update the pending add op ensemble
SetNewEnsemble(c, pending_op) ==
    {
        IF op = pending_op
        THEN [entry       |-> op.entry,
              range_id |-> c.curr_range.id,
              ensemble    |-> c.curr_range.ensemble]
        ELSE op : op \in c.pending_add_ops
    }

ResendPendingAddOp(c, is_recovery) ==
    /\ \E op \in c.pending_add_ops :
        /\ NeedsResend(op, c.curr_range)
        /\ ~\E op2 \in c.pending_add_ops :
            /\ op2.range_id = op.range_id
            /\ op2.ensemble = op.ensemble
            /\ op2.entry.id < op.entry.id
        /\ LET new_range_servers == c.curr_range.ensemble \ op.ensemble
           IN
              /\ SendMessagesToEnsemble(GetAddEntryRequests(c,
                                                            op.entry,
                                                            new_range_servers,
                                                            is_recovery))
              /\ clients' = [clients EXCEPT ![c.id] = 
                                [c EXCEPT !.pending_add_ops = SetNewEnsemble(c, op)]]

ClientResendsPendingAddOp(cid) ==
    /\ clients[cid].status = STATUS_OPEN
    /\ ResendPendingAddOp(clients[cid], FALSE)
    /\ UNCHANGED << range_server_vars, pd_vars >>

(***************************************************************************
ACTION: A client closes its own stream.
***************************************************************************)

ClientClosesStreamSuccess(cid) ==
    /\ clients[cid].pd_version = pd_version
    /\ clients[cid].status = STATUS_OPEN
    /\ pd_status = STATUS_OPEN
    /\ clients' = [clients EXCEPT ![cid] = 
                        [@ EXCEPT !.pd_version = @ + 1,
                                  !.status = STATUS_CLOSED]]
    /\ pd_status' = STATUS_CLOSED
    /\ pd_last_entry' = clients[cid].lac
    /\ pd_version' = pd_version + 1
    /\ UNCHANGED << range_server_vars, pd_ranges, messages >>

(***************************************************************************
ACTION: A client fails to close its own stream.  
****************************************************************************)

ClientClosesStreamFail(cid) ==
    /\ clients[cid].status = STATUS_OPEN
    /\ pd_status # STATUS_OPEN
    /\ clients' = [clients EXCEPT ![cid] = [@ EXCEPT !.status = CLIENT_WITHDRAWN,
                                                     !.pd_version = Nil]]
    /\ UNCHANGED << range_server_vars, pd_vars, messages >>

(***************************************************************************
ACTION: A client starts recovery.                                                       
****************************************************************************)

GetFencedReadLacRequests(c, ensemble) ==
    { [type   |-> FenceRequestMessage,
       range_server |-> range_server,
       cid    |-> c.id] : range_server \in ensemble }

(***************************************************************************
ACTION: A range server receives a fencing LAC request, sends a response.           
****************************************************************************)

GetFencingReadLacResponseMessage(msg) ==
    [type   |-> FenceResponseMessage,
     range_server |-> msg.range_server,
     cid    |-> msg.cid,
     lac    |-> r_lac[msg.range_server]]

RangeServerSendsFencingReadLacResponse ==
    \E msg \in DOMAIN messages :
        /\ ReceivableMessageOfType(messages, msg, FenceRequestMessage)
        /\ r_fenced' = [r_fenced EXCEPT ![msg.range_server] = TRUE]
        /\ ProcessedOneAndSendAnother(msg, GetFencingReadLacResponseMessage(msg))
        /\ UNCHANGED << r_entries, r_lac, clients, pd_vars >>
 
(***************************************************************************)
(* Initial and Next state                                                  *)
(***************************************************************************)

Init ==
    /\ messages = [msg \in {} |-> 0]
    /\ pd_status = Nil
    /\ pd_last_entry = 0
    /\ pd_ranges = <<>>
    /\ pd_version = 0
    /\ r_fenced = [b \in RangeServers |-> FALSE]
    /\ r_entries = [b \in RangeServers |-> {}]
    /\ r_lac = [b \in RangeServers |-> 0]
    /\ clients = [cid \in Clients |-> InitClient(cid)]

Next ==
    \* RangeServers
    \/ RangeServerSendsAddConfirmedResponse
    \/ RangeServerSendsAddFencedResponse
    \/ RangeServerSendsFencingReadLacResponse
    \/ \E cid \in Clients :
        \* original client
        \/ ClientCreatesStream(cid)
        \/ ClientSendsAddEntryRequests(cid)
        \/ ClientReceivesAddConfirmedResponse(cid)
        \/ ClientReceivesAddFencedResponse(cid)
        \/ ClientChangesEnsemble(cid)
        \/ ClientResendsPendingAddOp(cid)
        \/ ClientClosesStreamSuccess(cid)
        \/ ClientClosesStreamFail(cid)



(***************************************************************************
Invariant: TypeOK                                                       

Check that the variables hold the correct data types                    
****************************************************************************)

TypeOK ==
    /\ pd_status \in { Nil, STATUS_OPEN, STATUS_IN_RECOVERY, STATUS_CLOSED }
    /\ pd_last_entry \in Nat
    /\ pd_version \in Nat
    /\ r_fenced \in [RangeServers -> BOOLEAN]
    /\ r_entries \in [RangeServers -> SUBSET Entry]
    /\ r_lac \in [RangeServers -> Nat]
\*    /\ clients \in [Clients -> ClientState]

(***************************************************************************
Invariant: No Divergence Between Clients And MetaData                                                       
****************************************************************************)

NoDivergenceBetweenClientAndMetaData ==
    IF pd_status # STATUS_CLOSED
    THEN TRUE
    ELSE \A c \in DOMAIN clients :
            \/ clients[c].status = Nil
            \/ /\ clients[c].status # Nil
               /\ \A id \in 1..clients[c].lac : id <= pd_last_entry

(***************************************************************************
Invariant: All confirmed entries are readable                           
****************************************************************************)
AllAckedEntriesAreReadable ==
    \A cid \in Clients :
        IF /\ clients[cid].status \in {STATUS_OPEN, STATUS_CLOSED}
           /\ clients[cid].lac > 0
        THEN \A entry_id \in 1..clients[cid].lac :
                \E b \in RangeOfEntryId(entry_id, pd_ranges).ensemble : 
                    \E entry \in r_entries[b] :
                        entry.id = entry_id
        ELSE TRUE

(***************************************************************************
Invariant: No dirty reads                        
****************************************************************************)
NoDirtyReads ==
    \A b \in RangeServers :
        \/ r_lac[b] = 0 \* we only care about range server with LAC > 0
        \/ /\ r_lac[b] > 0
           /\ LET ensemble == RangeOfEntryId(r_lac[b], pd_ranges).ensemble
              IN
                \/ b \notin ensemble \* we only care about range servers in the ensemble
                \/ /\ b \in ensemble
                   /\ Quantify(ensemble, 
                        LAMBDA bk : \E e \in r_entries[bk] : e.id = r_lac[b]) >= AckQuorum         

(***************************************************************************
Invariant: All committed entries reach Ack Quorum                       
****************************************************************************)
EntryIdReachesAckQuorum(ensemble, entry_id) ==
    Quantify(ensemble, LAMBDA b : \E e \in r_entries[b] : e.id = entry_id) >= AckQuorum
\*    Cardinality({ b \in ensemble : \E e \in r_entries[b] : e.id = entry_id }) >= AckQuorum

AllCommittedEntriesReachAckQuorum ==
    IF pd_status # STATUS_CLOSED
    THEN TRUE
    ELSE IF pd_last_entry > 0
         THEN \A id \in 1..pd_last_entry :
                LET range == RangeOfEntryId(id, pd_ranges)
                IN EntryIdReachesAckQuorum(range.ensemble, id)
         ELSE TRUE

(***************************************************************************
Invariant: Read order matches write order                                                        
***************************************************************************)
NoOutOfOrderEntries ==
    \A b \in RangeServers :
        \A entry \in r_entries[b] :
            entry.id = entry.data

(************************************************************
Spec and Liveness                                                      
************************************************************)

StreamIsClosed ==
    /\ pd_status = STATUS_CLOSED
    /\ \E c \in clients : c.status = STATUS_CLOSED

Liveness ==
    /\ WF_vars(Next)
    /\ []<>StreamIsClosed

Spec == Init /\ [][Next]_vars


====