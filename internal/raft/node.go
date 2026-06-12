package raft

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
	"errors"
	"aegian/proto"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

var (
	ErrNotLeader = errors.New("not the leader")
	ErrTimeout   = errors.New("commit timed out")
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

const noVote int32 = -1

const (
	minElectionTimeout = 1000 * time.Millisecond
	maxElectionTimeout = 2000 * time.Millisecond
	tickInterval       = 50 * time.Millisecond
	heartbeatInterval  = 300 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	delta := maxElectionTimeout - minElectionTimeout
	return minElectionTimeout + time.Duration(rand.Int63n(int64(delta)))
}

type Node struct {
	mu sync.Mutex

	id    int32
	peers []proto.RaftClient

	store *storage

	currentTerm int32
	votedFor    int32
	role        Role

	lastHeard       time.Time
	electionTimeout time.Duration

	log         []*proto.LogEntry
	commitIndex int32             
	lastApplied int32             
	kv          map[string]string 

	nextIndex  []int32 
	matchIndex []int32 

	leaderID int32
	commitWaiters map[int32][]chan struct{}
}

func NewNode(id int32, peers []proto.RaftClient) *Node {
	store, err := newStorage(id)
	if err != nil {
		log.Fatalf("node %d: could not open storage: %v", id, err)
	}

	n := &Node{
		id:              id,
		peers:           peers,
		currentTerm:     0,
		votedFor:        noVote,
		role:            Follower,
		lastHeard:       time.Now(),
		electionTimeout: randomElectionTimeout(),

		log:         []*proto.LogEntry{{Term: 0}},
		commitIndex: 0,
		lastApplied: 0,
		kv:          make(map[string]string),
		store:       store,
		commitWaiters: make(map[int32][]chan struct{}),
	}

	term, votedFor, savedLog, ok, err := store.load()
	if err != nil {
		log.Fatalf("node %d: could not load storage: %v", id, err)
	}
	if ok {
		n.currentTerm = term
		n.votedFor = votedFor
		if len(savedLog) > 0 {
			n.log = savedLog
		}
		log.Printf("node %d: RESTORED from disk — term %d, votedFor %d, log up to index %d",
			id, n.currentTerm, n.votedFor, n.lastLogIndex())

		n.commitIndex = n.lastLogIndex()
		n.applyCommitted()
	}
	n.notifyCommitWaiters()

	return n
}

func (n *Node) lastLogIndex() int32 {
	return int32(len(n.log) - 1)
}

func (n *Node) lastLogTerm() int32 {
	return n.log[len(n.log)-1].Term
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		n.applyCommand(entry.Command)
	}
	n.notifyCommitWaiters()
}

func (n *Node) applyCommand(cmd string) {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}
	switch parts[0] {
	case "PUT":
		if len(parts) == 3 {
			n.kv[parts[1]] = parts[2]
			log.Printf("node %d: applied PUT %s=%s", n.id, parts[1], parts[2])
		}
	case "DEL":
		if len(parts) == 2 {
			delete(n.kv, parts[1])
			log.Printf("node %d: applied DEL %s", n.id, parts[1])
		}
	}
}

func (n *Node) Get(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	v, ok := n.kv[key]
	return v, ok
}

func (n *Node) Run() {
	n.mu.Lock()
	log.Printf("node %d: started as %s (term %d)", n.id, n.role, n.currentTerm)
	n.mu.Unlock()

	for {
		time.Sleep(tickInterval)

		n.mu.Lock()
		if n.role != Leader && time.Since(n.lastHeard) >= n.electionTimeout {
			n.becomeCandidate()
		}
		n.mu.Unlock()
	}
}

func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.lastHeard = time.Now()
	n.electionTimeout = randomElectionTimeout()
	n.persist() 
	log.Printf("node %d: election timeout — becoming Candidate for term %d", n.id, n.currentTerm)
	go n.startElection()
}

func (n *Node) startElection() {
	n.mu.Lock()
	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	n.mu.Unlock()

	req := &proto.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}

	majority := (len(n.peers)+1)/2 + 1
	votes := 1

	for _, peer := range n.peers {
		go func(p proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			reply, err := p.RequestVote(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.GetTerm() > n.currentTerm {
				n.becomeFollower(reply.GetTerm())
				return
			}
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.GetVoteGranted() {
				votes++
				log.Printf("node %d: got vote (%d/%d) for term %d", n.id, votes, len(n.peers)+1, term)
				if votes >= majority {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeLeader() {
	if n.role == Leader {
		return
	}
	n.role = Leader
	n.leaderID = n.id

	n.nextIndex = make([]int32, len(n.peers))
	n.matchIndex = make([]int32, len(n.peers))
	for i := range n.peers {
		n.nextIndex[i] = n.lastLogIndex() + 1
		n.matchIndex[i] = 0
	}

	log.Printf("node %d: WON election for term %d — becoming LEADER", n.id, n.currentTerm)
	go n.runHeartbeats(n.currentTerm)
}

func (n *Node) becomeFollower(term int32) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = noVote
	n.lastHeard = time.Now()
	n.persist() 
}

func (n *Node) runHeartbeats(term int32) {
	for {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		n.replicate(term)
		time.Sleep(heartbeatInterval)
	}
}

func (n *Node) replicate(term int32) {
	for i := range n.peers {
		go n.replicateToPeer(i, term)
	}
}

//leader node checks for the replication
func (n *Node) replicateToPeer(i int, term int32) {
	n.mu.Lock()
	if n.role != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	prevLogIndex := n.nextIndex[i] - 1
	prevLogTerm := n.log[prevLogIndex].Term

	var entries []*proto.LogEntry
	for j := n.nextIndex[i]; j <= n.lastLogIndex(); j++ {
		entries = append(entries, n.log[j])
	}

	req := &proto.AppendEntriesRequest{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	peer := n.peers[i]
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	reply, err := peer.AppendEntries(ctx, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.GetTerm() > n.currentTerm {
		n.becomeFollower(reply.GetTerm())
		return
	}
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.GetSuccess() {
		n.matchIndex[i] = prevLogIndex + int32(len(entries))
		n.nextIndex[i] = n.matchIndex[i] + 1
		n.advanceCommitIndex()
	} else if n.nextIndex[i] > 1 {
		n.nextIndex[i]--
	}
}

func (n *Node) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		return &proto.RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	if req.GetTerm() > n.currentTerm {
		n.becomeFollower(req.GetTerm())
	}

	myLastIndex := n.lastLogIndex()
	myLastTerm := n.lastLogTerm()
	logOk := req.GetLastLogTerm() > myLastTerm ||
		(req.GetLastLogTerm() == myLastTerm && req.GetLastLogIndex() >= myLastIndex)

	voteGranted := false
	if (n.votedFor == noVote || n.votedFor == req.GetCandidateId()) && logOk {
		n.votedFor = req.GetCandidateId()
		n.lastHeard = time.Now()
		voteGranted = true
		n.persist() 
		log.Printf("node %d: voted for node %d in term %d", n.id, req.GetCandidateId(), n.currentTerm)
	}

	return &proto.RequestVoteReply{Term: n.currentTerm, VoteGranted: voteGranted}
}

func (n *Node) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.GetTerm() < n.currentTerm {
		return &proto.AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if req.GetTerm() > n.currentTerm {
		n.becomeFollower(req.GetTerm())
	}
	n.role = Follower
	n.lastHeard = time.Now()
	n.leaderID = req.GetLeaderId()

	prevIdx := req.GetPrevLogIndex()
	if prevIdx > n.lastLogIndex() || n.log[prevIdx].Term != req.GetPrevLogTerm() {
		return &proto.AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	for i, entry := range req.GetEntries() {
		idx := prevIdx + 1 + int32(i)
		if idx <= n.lastLogIndex() {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx]
				n.log = append(n.log, entry)
			}
		} else {
			n.log = append(n.log, entry)
		}
	}

	if len(req.GetEntries()) > 0 {
		n.persist()
		log.Printf("node %d: appended %d entries from leader %d (log now index %d)",
			n.id, len(req.GetEntries()), req.GetLeaderId(), n.lastLogIndex())
	}
	
	if req.GetLeaderCommit() > n.commitIndex {
		last := n.lastLogIndex()
		if req.GetLeaderCommit() < last {
			n.commitIndex = req.GetLeaderCommit()
		} else {
			n.commitIndex = last
		}
		n.applyCommitted()
	}

	return &proto.AppendEntriesReply{Term: n.currentTerm, Success: true}
}

func (n *Node) Propose(command string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return false
	}

	entry := &proto.LogEntry{
		Term:    n.currentTerm,
		Command: command,
	}
	n.log = append(n.log, entry)
	n.persist()

	log.Printf("node %d: LEADER appended [%s] at index %d (term %d)",
		n.id, command, n.lastLogIndex(), n.currentTerm)
	return true
}

func itoa(i int) string {
	return strconv.Itoa(i)
}

func (n *Node) advanceCommitIndex() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break
		}

		count := 1
		for i := range n.peers {
			if n.matchIndex[i] >= idx {
				count++
			}
		}

		if count >= (len(n.peers)+1)/2+1 {
			n.commitIndex = idx
			log.Printf("node %d: LEADER advanced commitIndex to %d", n.id, n.commitIndex)
			n.applyCommitted()
			break
		}
	}
}

func (n *Node) persist() {
	if err := n.store.save(n.currentTerm, n.votedFor, n.log); err != nil {
		log.Printf("node %d: PERSIST FAILED: %v", n.id, err)
	}
}

func (n *Node) Close() {
	if n.store != nil {
		n.store.close()
	}
}

func (n *Node) notifyCommitWaiters() {
	for idx, chans := range n.commitWaiters {
		if idx <= n.commitIndex {
			for _, ch := range chans {
				close(ch)
			}
			delete(n.commitWaiters, idx)
		}
	}
}

func (n *Node) ProposeAndWait(command string, timeout time.Duration) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	entry := &proto.LogEntry{Term: n.currentTerm, Command: command}
	n.log = append(n.log, entry)
	n.persist()
	index := n.lastLogIndex()

	log.Printf("node %d: LEADER appended [%s] at index %d (term %d)", n.id, command, index, n.currentTerm)

	ch := make(chan struct{})
	n.commitWaiters[index] = append(n.commitWaiters[index], ch)
	n.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return ErrTimeout
	}
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

func (n *Node) LeaderID() int32 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}