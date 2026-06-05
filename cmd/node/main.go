package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"aegian/internal/raft"
	"aegian/internal/server"
	"aegian/proto"
)

func main() {
	id := flag.Int("node-id", 1, "this node's ID")
	port := flag.String("grpc-port", "50071", "gRPC port to listen on")
	peers := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()

	var peerClients []proto.RaftClient
	for _, addr := range strings.Split(*peers, ",") {
		if addr == "" {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("node %d: could not create client for %s: %v", *id, addr, err)
		}
		peerClients = append(peerClients, proto.NewRaftClient(conn))
	}

	node := raft.NewNode(int32(*id), peerClients)
	srv := &server.Server{Node: node}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("node %d: failed to listen on :%s: %v", *id, *port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterRaftServer(grpcServer, srv)

	go func() {
		log.Printf("node %d: listening on :%s", *id, *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("node %d: serve failed: %v", *id, err)
		}
	}()

	go node.Run()

	select {}
}