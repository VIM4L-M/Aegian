package server

import (
	"context"
	"log"

	"aegian/proto"
)

// Server implements the gRPC RaftServer interface.
// For Stage 0 the handlers just log and reply — no real Raft logic yet.
type Server struct {
	proto.UnimplementedRaftServer // required: forward-compatibility for new RPCs
	ID int32
}

func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteReply, error) {
	log.Printf("node %d: got RequestVote from node %d (term %d)", s.ID, req.GetCandidateId(), req.GetTerm())
	return &proto.RequestVoteReply{Term: req.GetTerm(), VoteGranted: true}, nil
}

func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesReply, error) {
	log.Printf("node %d: got AppendEntries from node %d (term %d)", s.ID, req.GetLeaderId(), req.GetTerm())
	return &proto.AppendEntriesReply{Term: req.GetTerm(), Success: true}, nil
}