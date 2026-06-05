package server

import (
	"context"

	"aegian/internal/raft"
	"aegian/proto"
)

type Server struct {
	proto.UnimplementedRaftServer
	Node *raft.Node
}

func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteReply, error) {
	return s.Node.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesReply, error) {
	return s.Node.HandleAppendEntries(req), nil
}