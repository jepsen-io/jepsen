---- MODULE aerospike ----

EXTENDS TLC, Sequences, Naturals, FiniteSets

CONSTANT Roster            \* User-defined set of nodes
CONSTANT ReplicationFactor \* How many nodes should be replicas?
CONSTANT SuccessionOrder   \* A function of servers to precedence order

(* Message types *)
CONSTANTS Heartbeat


------------
\* Global variables

\* Network: a multiset of messages
VARIABLES net

--------------
\* Server variables
VARIABLES clusters \* The set of servers this server thinks are alive
\* VARIABLE successionList Deterministic, hashed list chosen by cluster

serverVars == <<clusters>>

\* All variables
vars == <<net, serverVars>>


-------------------
\* Helpers
Range(t) == { t[x] : x \in DOMAIN t }

\* Dissociate a key from a function m
Dissoc(m, k) == [i \in (DOMAIN m \ {k}) |-> m[i]]

\* First n records from sequence s
Take(n, s) == [i \in {j \in DOMAIN s : j =< n} |-> s[i]]

--------------------
\* Network transitions

\* Add a message to the network and increment its count
AddMessage(network, message) == IF message \in DOMAIN network
                                THEN [network EXCEPT ![message] = @ + 1]
                                ELSE network @@ (message :> 1)
                                
\* Remove a message from the network by decrementing its count
RemoveMessage(network, message) == IF ~(message \in DOMAIN network)
                                   THEN network
                                   ELSE IF network[message] = 1
                                   THEN Dissoc(network, message) \* Delete
                                   ELSE [network EXCEPT ![message] = @ - 1] \* Decrement
 
\* Send a message into the net
Send(message) == net' = AddMessage(net, message)
 
\* Forget a message (for once we've processed it)
Discard(message) == net' = RemoveMessage(net, message)
 
\* Discard request, send response
Reply(request, response) == net' = AddMessage(RemoveMessage(net, request), response)


-------------------------
\* Membership stuff

\* Succession lists are deterministic functions of clusters. In Aerospike, they're hashed.
\* For now, we'll just sort using SuccessionOrder.
SuccessionListComparator(a, b) == SuccessionOrder[a] < SuccessionOrder[b]
SuccessionList(cluster) == SortSeq(cluster, SuccessionListComparator)

\* The Roster Replicas are not, as one might think, the set of replicas on the Roster.
\* Rather, they're the first replication-factor replicas on the roster, in the order
\* given by the succession list for a cluster which is exactly equal to the roster.
RosterReplicas == Take(ReplicationFactor, SuccessionList(Roster))

-------------------------
\* Server predicates


\* The Roster Master is the first node in the roster
RosterMaster == SuccessionList(Roster)[1]

\* Does the given set of servers have all, any, or the master node from the roster?
HasAllRosterReplicas(cluster)   == Roster \subseteq cluster
HasARosterReplica(cluster)      == \E s \in clusters, r \in Range(RosterReplicas) : s = r 
HasRosterMaster(cluster)        == \E s \in clusters : s = RosterMaster

\* Do we have a strict majority or exactly half of the roster?
HasMajority(cluster) == Cardinality(cluster) * 2 > Cardinality(Roster)
HasHalf(cluster)     == Cardinality(cluster) * 2 = Cardinality(Roster)

\* Does the given server think a quorum is available?
HasQuorum(s) == LET cluster == clusters[s]
                IN \*\/ HasAllRosterReplicas(cluster)
                   \/ /\ HasMajority(cluster)
                      /\ HasARosterReplica(cluster)
                   \/ /\ HasHalf(cluster)
                      /\ HasRosterMaster(cluster)

-------------------------
\* State initialization

\* No messages to start
InitNet == net = [m \in {} |-> 0]
 
\* Every server starts thinking only itself is alive
InitClusters == clusters = [server \in Roster |-> {server}]

Init == /\ InitNet
        /\ InitClusters


---------------------------
\* State transitions

\* Network can drop or duplicate a message
NetDrop(m) == /\ Discard(m)
              /\ UNCHANGED <<serverVars>>
NetDuplicate(m) == /\ Send(m)
                   /\ UNCHANGED <<serverVars>>

\* A server can send a heartbeat to another server
SendHeartbeat(s, dest) == /\ Send([mtype |-> Heartbeat,
                                   msource |-> s,
                                   mdest |-> dest])
                          /\ UNCHANGED <<serverVars>>
                          
\* Server s handles a heartbeat message from source
HandleHeartbeat(s, source, m) == /\ clusters' = [clusters EXCEPT ![s] = @ \union {source}]
                                /\ Discard(m)

\* A message arrives at a server
Receive(m) == LET source  == m.msource
                  dest    == m.mdest
              IN /\ m.mtype = Heartbeat
                 /\ HandleHeartbeat(dest, source, m)

Next == \/ \E m \in DOMAIN net : NetDrop(m)
        \/ \E m \in DOMAIN net : NetDuplicate(m)
        \/ \E s,dest \in Roster : SendHeartbeat(s, dest)
        \/ \E m \in DOMAIN net : Receive(m)

\* Start with init and transition with Next
Spec == Init /\ [][Next]_vars


--------------------------
\* Invariants


SomeHasQuorum == \E s \in Roster : HasQuorum(s)

====