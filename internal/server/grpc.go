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
func (s *Server) ClientCommand(ctx context.Context, req *proto.ClientCommandRequest) (*proto.ClientCommandReply, error) {
	err := s.Node.HandleClientCommand(req.GetCommand())
	switch err {
	case nil:
		return &proto.ClientCommandReply{Success: true, Message: "OK"}, nil
	case raft.ErrNotLeader:
		return &proto.ClientCommandReply{Success: false, Message: "not the leader"}, nil
	case raft.ErrTimeout:
		return &proto.ClientCommandReply{Success: false, Message: "commit timed out"}, nil
	default:
		return &proto.ClientCommandReply{Success: false, Message: err.Error()}, nil
	}
}
