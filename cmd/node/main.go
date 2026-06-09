package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"strconv"

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
	fresh := flag.Bool("fresh", false, "wipe saved state and start clean")
	flag.Parse()

	if *fresh {
		dbPath := filepath.Join("data", "node-"+strconv.Itoa(*id)+".db")
		if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
			log.Printf("node %d: could not wipe state: %v", *id, err)
		} else {
			log.Printf("node %d: --fresh — wiped saved state", *id)
		}
	}

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

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Printf("node %d: shutting down…", *id)
	grpcServer.GracefulStop()
	node.Close()
	log.Printf("node %d: storage closed, bye", *id)
}