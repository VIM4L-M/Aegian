package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"aegian/internal/server"
	"aegian/proto"
)

func main() {
	id := flag.Int("node-id", 1, "this node's ID")
	port := flag.String("grpc-port", "50071", "gRPC port to listen on")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("node %d: failed to listen on :%s: %v", *id, *port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterRaftServer(grpcServer, &server.Server{ID: int32(*id)})

	go func() {
		log.Printf("node %d: listening on :%s", *id, *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("node %d: serve failed: %v", *id, err)
		}
	}()

	time.Sleep(3 * time.Second)
	for _, addr := range strings.Split(*peers, ",") {
		if addr == "" {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("node %d: could not create client for %s: %v", *id, addr, err)
			continue
		}
		client := proto.NewRaftClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := client.RequestVote(ctx, &proto.RequestVoteRequest{Term: 1, CandidateId: int32(*id)})
		cancel()
		conn.Close()
		if err != nil {
			log.Printf("node %d: ping to %s failed: %v", *id, addr, err)
			continue
		}
		log.Printf("node %d: pinged %s, vote_granted=%v", *id, addr, reply.GetVoteGranted())
	}

	select {}
}