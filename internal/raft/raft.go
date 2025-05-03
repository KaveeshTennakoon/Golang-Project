package raft

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"mini_etcd/config"
	"mini_etcd/internal/transport"
)

// ------------------------------------------------------------
// Raft node state & constructor
// ------------------------------------------------------------

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string { return [...]string{"Follower", "Candidate", "Leader"}[s] }

type Node struct {
	mu sync.RWMutex

	// identity & topology
	id    string
	peers map[string]string // peerID -> addr

	// persistent state and caching variables
	currentTerm int
	votedFor    string
	db          *bolt.DB
	log         StableLog
	store       StableStore

	// volatile state
	commitIndex int
	lastApplied int

	// leader-only state
	nextIndex  map[string]int
	matchIndex map[string]int

	// runtime plumbing
	state          State
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	stopCh         chan struct{}

	// transport
	trans *transport.HTTPTransport
}

func NewNode(id string, peers map[string]string, applyCh chan ApplyMsg, db *bolt.DB) *Node {
	n := &Node{
		id:         id,
		peers:      peers,
		db:         db,
		log:        NewBoltLog(db),
		store:      NewBoltStore(db),
		state:      Follower,
		applyCh:    applyCh,
		stopCh:     make(chan struct{}),
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}

	n.currentTerm = n.store.Term()
	n.votedFor = n.store.VotedFor()
	n.lastApplied = n.store.LastApplied()

	n.resetElectionTimer()
	n.trans = transport.New(n.handleInbound)
	return n
}

// ------------------------------------------------------------
// Public API
// ------------------------------------------------------------

func (n *Node) Serve(addr string) error {
	// Create a more resilient HTTP server with timeouts
	svr := &http.Server{
		Addr:    addr,
		Handler: n.trans,
		// Add timeouts to prevent hanging connections
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go n.ticker()
	return svr.ListenAndServe()
}

func (n *Node) Start() {
	go n.ticker()
}

func (n *Node) Stop() { close(n.stopCh) }

// Propose replicates a command **only the leader**.
func (n *Node) Propose(cmd any) (idx int, ok bool) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return -1, false
	}
	idx = n.log.Append(LogEntry{Term: n.currentTerm, Command: cmd})

	// ---------- single-node fast commit ----------------
	if len(n.peers) == 0 {
		n.commitIndex = idx
		n.applyCommitted() // apply locally
		n.maybePrune()

		n.mu.Unlock()

		return idx, true
	}
	// ---------------------------------------------------

	n.mu.Unlock()

	go n.broadcastAppendEntries()
	return idx, true
}

// ------------------------------------------------------------
// Ticker goroutine – drives elections & heart-beats
// ------------------------------------------------------------

func (n *Node) ticker() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()

			if !isLeader { // only followers / candidates start elections
				// run election logic in its *own* goroutine so the ticker
				// continues to service heart-beats and term updates.
				go n.startElection()
			}
		case <-n.heartbeatTimerC():
			n.mu.RLock()
			leader := n.state == Leader
			n.mu.RUnlock()
			if leader {
				n.broadcastAppendEntries()
			}
			n.heartbeatTimer.Reset(config.HeartbeatInterval)
		}
	}
}

func (n *Node) startHeartbeatTimer() {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
	} else {
		n.heartbeatTimer.Reset(config.HeartbeatInterval)
	}
}

func (n *Node) heartbeatTimerC() <-chan time.Time {
	if n.heartbeatTimer == nil {
		n.heartbeatTimer = time.NewTimer(config.HeartbeatInterval)
		if !n.heartbeatTimer.Stop() {
			<-n.heartbeatTimer.C
		}
	}
	return n.heartbeatTimer.C
}

func (n *Node) resetElectionTimer() {
	d := time.Duration(rand.Intn(int(config.ElectionTimeoutMax-config.ElectionTimeoutMin))) + config.ElectionTimeoutMin
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(d)
	} else {
		n.electionTimer.Reset(d)
	}
}

// ------------------------------------------------------------
// Elections
// ------------------------------------------------------------

func (n *Node) startElection() {
	// Step up to candidate & bump term
	n.mu.Lock()

	// Only start an election if we're a follower or candidate
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	DebugLog("Node %s starting election for term %d", n.id, n.currentTerm+1)
	n.state = Candidate

	n.currentTerm++
	n.store.SetTerm(n.currentTerm)

	// Vote for self
	n.votedFor = n.id
	n.store.SetVotedFor(n.id)

	term := n.currentTerm
	lastIdx, lastTerm := n.log.LastIndexTerm()

	// Reset election timer with a new random timeout
	n.resetElectionTimer()

	// Copy peers map so we can iterate after releasing the lock
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}

	// Calculate the number of votes needed for majority
	totalNodes := len(n.peers) + 1 // +1 for self
	votesNeeded := totalNodes/2 + 1

	DebugLog("Node %s needs %d votes to win election (total nodes: %d)", n.id, votesNeeded, totalNodes)

	// If we're a single node, become leader immediately
	if len(n.peers) == 0 {
		DebugLog("Node %s is the only node, becoming leader immediately", n.id)
		n.becomeLeader()
		n.mu.Unlock()
		return
	}

	n.mu.Unlock()

	// Count votes (starting with our self-vote)
	var votes int32 = 1

	// Use a channel to signal when we've received enough votes
	votesChan := make(chan bool, len(peerAddrs))

	// Request votes from all peers
	for pid, paddr := range peerAddrs {
		go func(id, addr string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}

			var reply RequestVoteReply
			DebugLog("Node %s requesting vote from %s for term %d", n.id, id, term)

			if err := n.trans.Call(addr, transport.RPCRequestVote, &args, &reply); err != nil {
				DebugLog("Node %s error requesting vote from %s: %v", n.id, id, err)
				votesChan <- false
				return
			}

			// If we see a higher term, become a follower
			if reply.Term > term {
				DebugLog("Node %s saw higher term %d from %s, becoming follower", n.id, reply.Term, id)
				n.mu.Lock()
				n.becomeFollower(reply.Term)
				n.mu.Unlock()
				votesChan <- false
				return
			}

			// If we got a vote, increment the counter
			if reply.VoteGranted && reply.Term == term {
				DebugLog("Node %s received vote from %s for term %d", n.id, id, term)
				if atomic.AddInt32(&votes, 1) >= int32(votesNeeded) {
					DebugLog("Node %s received majority of votes (%d/%d) for term %d", n.id, votes, totalNodes, term)
					n.mu.Lock()
					if n.state == Candidate && n.currentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
				}
			} else {
				DebugLog("Node %s vote denied by %s for term %d", n.id, id, term)
			}

			votesChan <- true
		}(pid, paddr)
	}

	// Wait for all vote requests to complete
	for i := 0; i < len(peerAddrs); i++ {
		<-votesChan
	}

	// Final check if we've become leader
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state == Candidate && n.currentTerm == term {
		DebugLog("Node %s election for term %d completed without majority, remaining candidate", n.id, term)
	}
}

// ------------------------------------------------------------
// Leader transition helpers
// ------------------------------------------------------------

func (n *Node) becomeFollower(term int) {
	n.state = Follower

	n.currentTerm = term
	n.store.SetTerm(term)

	n.votedFor = ""
	n.store.SetVotedFor("")

	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	DebugLog("Node %s becoming leader for term %d", n.id, n.currentTerm)
	n.state = Leader

	// Initialize nextIndex and matchIndex for all peers
	lastIdx := n.log.LastIndex() + 1
	for id := range n.peers {
		if id == n.id {
			continue
		}
		n.nextIndex[id] = lastIdx
		n.matchIndex[id] = 0
	}

	// Append a no-op entry to establish leadership
	n.log.Append(LogEntry{Term: n.currentTerm, Command: nil})

	// Reset election timer to prevent timeouts while leader
	n.resetElectionTimer()

	// Start heartbeat timer
	n.startHeartbeatTimer()

	// Send initial heartbeats to establish authority
	go n.broadcastAppendEntries()
}

func (n *Node) broadcastAppendEntries() {
	// capture a *snapshot* of leader’s state under read-lock
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	term := n.currentTerm
	commitIdx := n.commitIndex
	nextIdxSnap := make(map[string]int, len(n.nextIndex))
	for k, v := range n.nextIndex {
		nextIdxSnap[k] = v
	}
	peerAddrs := make(map[string]string, len(n.peers))
	for id, addr := range n.peers {
		if id != n.id {
			peerAddrs[id] = addr
		}
	}
	n.mu.RUnlock()

	// If no peers, nothing to broadcast
	if len(peerAddrs) == 0 {
		return
	}

	for pid, addr := range peerAddrs {
		ni := nextIdxSnap[pid]
		go func(id, paddr string, next int) {
			prevIdx := next - 1
			prevTerm := 0
			if prevIdx > 0 {
				if e, ok := n.log.At(prevIdx); ok {
					prevTerm = e.Term
				}
			}
			// gather entries [next .. last]
			entries := make([]LogEntry, 0)
			lastIdx := n.log.LastIndex()
			for i := next; i <= lastIdx; i++ {
				if e, ok := n.log.At(i); ok {
					entries = append(entries, e)
				}
			}
			args := AppendEntriesArgs{Term: term, LeaderID: n.id, PrevLogIndex: prevIdx, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: commitIdx}
			var reply AppendEntriesReply
			if err := n.trans.Call(paddr, transport.RPCAppendEntries, &args, &reply); err != nil {
				return // network error – ignore, follower will timeout
			}
			n.handleAppendEntriesReply(id, &reply)
		}(pid, addr, ni)
	}
}

func (n *Node) handleAppendEntriesReply(peerID string, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}
	if !reply.Success {
		if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
		return
	}

	// --- success path --------------------------------------------------
	// Update nextIndex and matchIndex for this peer
	lastLogIndex := n.log.LastIndex()
	n.nextIndex[peerID] = lastLogIndex + 1
	n.matchIndex[peerID] = lastLogIndex

	// Check if we can advance commitIndex
	advanced := false
	for i := n.commitIndex + 1; i <= lastLogIndex; i++ {
		// Only count entries from current term for safety
		entry, ok := n.log.At(i)
		if !ok || entry.Term != n.currentTerm {
			continue
		}

		replicated := 1 // self
		for id := range n.peers {
			if id != n.id && n.matchIndex[id] >= i {
				replicated++
			}
		}
		// Need majority of all nodes (including self)
		if replicated > (len(n.peers)+1)/2 {
			n.commitIndex = i
			advanced = true
		}
	}

	// tell followers the new commitIndex
	if advanced {
		go n.broadcastAppendEntries()
		n.maybePrune()
	}

	// apply to local state machine
	n.applyCommitted()
}

func (n *Node) onRequestVote(args *RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If the request term is less than our current term, reject it
	if args.Term < n.currentTerm {
		DebugLog("Node %s rejecting vote for %s: term %d < current term %d", n.id, args.CandidateID, args.Term, n.currentTerm)
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	// If we see a higher term, become a follower
	if args.Term > n.currentTerm {
		DebugLog("Node %s becoming follower: saw term %d > current term %d", n.id, args.Term, n.currentTerm)
		n.becomeFollower(args.Term)
	}

	// Check if candidate's log is at least as up-to-date as ours
	li, lt := n.log.LastIndexTerm()
	upToDate := args.LastLogTerm > lt || (args.LastLogTerm == lt && args.LastLogIndex >= li)

	// Grant vote if we haven't voted for anyone else in this term and the candidate's log is up-to-date
	grant := false
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		DebugLog("Node %s granting vote to %s for term %d", n.id, args.CandidateID, args.Term)
		grant = true

		n.votedFor = args.CandidateID
		n.store.SetVotedFor(args.CandidateID)

		// Reset election timer since we just voted
		n.resetElectionTimer()
	} else {
		if n.votedFor != "" && n.votedFor != args.CandidateID {
			DebugLog("Node %s rejecting vote for %s: already voted for %s in term %d", n.id, args.CandidateID, n.votedFor, n.currentTerm)
		}
		if !upToDate {
			DebugLog("Node %s rejecting vote for %s: log not up-to-date (candidate: lastTerm=%d, lastIdx=%d; me: lastTerm=%d, lastIdx=%d)",
				n.id, args.CandidateID, args.LastLogTerm, args.LastLogIndex, lt, li)
		}
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: grant}
}

func (n *Node) onAppendEntries(args *AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If the request term is less than our current term, reject it
	if args.Term < n.currentTerm {
		DebugLog("Node %s rejecting AppendEntries: term %d < current term %d", n.id, args.Term, n.currentTerm)
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// If we see a higher term, become a follower
	if args.Term > n.currentTerm {
		DebugLog("Node %s becoming follower: saw term %d > current term %d", n.id, args.Term, n.currentTerm)
		n.becomeFollower(args.Term)
	}

	// If we're a leader and we get an AppendEntries from another node with the same term,
	// step down (there can't be two leaders in the same term)
	if n.state == Leader && args.Term == n.currentTerm && args.LeaderID != n.id {
		DebugLog("Node %s stepping down: another leader %s exists in term %d", n.id, args.LeaderID, args.Term)
		n.becomeFollower(args.Term)
	}

	// Reset election timer as we've heard from the leader
	n.resetElectionTimer()

	// Log consistency check
	if args.PrevLogIndex > n.log.LastIndex() {
		DebugLog("Node %s rejecting AppendEntries: prevLogIndex %d > lastIndex %d", n.id, args.PrevLogIndex, n.log.LastIndex())
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if args.PrevLogIndex > 0 {
		if e, ok := n.log.At(args.PrevLogIndex); !ok || e.Term != args.PrevLogTerm {
			DebugLog("Node %s rejecting AppendEntries: term mismatch at prevLogIndex %d", n.id, args.PrevLogIndex)
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	// Append new entries (truncate conflicts first)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if e, ok := n.log.At(idx); !ok || e.Term != entry.Term {
			// Truncate suffix
			err := n.log.TruncateSuffix(idx)
			if err != nil {
				panic(err)
			}
			n.log.Append(entry)
		}
	}

	// Update commit index if leader's is higher
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min_(args.LeaderCommit, n.log.LastIndex())
		DebugLog("Node %s updated commitIndex to %d", n.id, n.commitIndex)
	}

	// Apply committed entries to state machine
	n.applyCommitted()
	n.maybePrune()

	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) Trans() http.Handler { return n.trans }

func min_(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		if e, ok := n.log.At(n.lastApplied); ok {
			n.applyCh <- ApplyMsg{CommandValid: true, Command: e.Command,
				CommandIndex: n.lastApplied}
			n.store.SetLastApplied(n.lastApplied)
		}
	}
}

func (n *Node) maybePrune() {
	if n.commitIndex-n.log.FirstIndex() > config.PruneEvery {
		cutoff := n.commitIndex - config.RetainTail
		if cutoff > n.log.FirstIndex() {
			n.log.TruncateBefore(cutoff)
		}
	}
}

// ------------------------------------------------------------
// Inbound RPC handlers (HTTP callbacks)
// ------------------------------------------------------------

func (n *Node) handleInbound(method transport.RPC, body io.Reader, w http.ResponseWriter) {
	switch method {
	case transport.RPCRequestVote:
		var args RequestVoteArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onRequestVote(&args))
	case transport.RPCAppendEntries:
		var args AppendEntriesArgs
		_ = json.NewDecoder(body).Decode(&args)
		transport.ReplyJSON(w, n.onAppendEntries(&args))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// ------------------------------------------------------------
// *Testing helpers* – read-only accessors use RLock
// ------------------------------------------------------------

func (n *Node) State() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) ID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

func (n *Node) LastApplied() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.lastApplied
}

func (n *Node) Log() StableLog { return n.log }

func (n *Node) GetDB() *bolt.DB { return n.db }

func (n *Node) Peers() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.peers
}

func (n *Node) PeersCopy() map[string]string {
	out := make(map[string]string, len(n.peers))
	for k, v := range n.peers {
		out[k] = v
	}
	return out
}

func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }
