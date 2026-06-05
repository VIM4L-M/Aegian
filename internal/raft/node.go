package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"aegian/proto"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
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

	currentTerm int32
	votedFor    int32
	role        Role

	lastHeard       time.Time
	electionTimeout time.Duration
}

func NewNode(id int32, peers []proto.RaftClient) *Node {
	return &Node{
		id:              id,
		peers:           peers,
		currentTerm:     0,
		votedFor:        noVote,
		role:            Follower,
		lastHeard:       time.Now(),
		electionTimeout: randomElectionTimeout(),
	}
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

// becomeCandidate must be called with n.mu held.
func (n *Node) becomeCandidate() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.id
	n.lastHeard = time.Now()
	n.electionTimeout = randomElectionTimeout()
	log.Printf("node %d: election timeout — becoming Candidate for term %d", n.id, n.currentTerm)
	go n.startElection()
}

func (n *Node) startElection() {
	n.mu.Lock()
	term := n.currentTerm
	n.mu.Unlock()

	req := &proto.RequestVoteRequest{
		Term:        term,
		CandidateId: n.id,
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

// becomeLeader must be called with n.mu held.
func (n *Node) becomeLeader() {
	if n.role == Leader {
		return
	}
	n.role = Leader
	log.Printf("node %d: WON election for term %d — becoming LEADER", n.id, n.currentTerm)
	go n.runHeartbeats(n.currentTerm)
}

// becomeFollower must be called with n.mu held.
func (n *Node) becomeFollower(term int32) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = noVote
	n.lastHeard = time.Now()
}

func (n *Node) runHeartbeats(term int32) {
	for {
		n.mu.Lock()
		if n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		n.sendHeartbeats(term)
		time.Sleep(heartbeatInterval)
	}
}

func (n *Node) sendHeartbeats(term int32) {
	req := &proto.AppendEntriesRequest{
		Term:     term,
		LeaderId: n.id,
	}

	for _, peer := range n.peers {
		go func(p proto.RaftClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			reply, err := p.AppendEntries(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.GetTerm() > n.currentTerm {
				n.becomeFollower(reply.GetTerm())
			}
		}(peer)
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

	voteGranted := false
	if n.votedFor == noVote || n.votedFor == req.GetCandidateId() {
		n.votedFor = req.GetCandidateId()
		n.lastHeard = time.Now()
		voteGranted = true
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

	return &proto.AppendEntriesReply{Term: n.currentTerm, Success: true}
}