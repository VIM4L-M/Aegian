package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"aegian/internal/raft"
	"aegian/internal/server"
	"aegian/proto"
)

func main() {
	id := flag.Int("node-id", 1, "this node's ID")
	port := flag.String("grpc-port", "50071", "gRPC port to listen on")
	httpPort := flag.String("http-port", "3001", "HTTP port for the client API")
	peersWithID := flag.String("peers", "", "comma-separated id=grpcaddr pairs of OTHER nodes")
	peerHTTPFlag := flag.String("peer-http", "", "comma-separated id=httpaddr pairs")
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

	peerHTTP := parsePeerHTTP(*peerHTTPFlag)

	var peerClients []proto.RaftClient
	peerClientsByID := make(map[int32]proto.RaftClient)

	for _, pair := range strings.Split(*peersWithID, ",") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}
		peerID, err := strconv.Atoi(kv[0])
		if err != nil {
			continue
		}
		addr := kv[1]
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("node %d: could not create client for %s: %v", *id, addr, err)
		}
		client := proto.NewRaftClient(conn)
		peerClients = append(peerClients, client)
		peerClientsByID[int32(peerID)] = client
	}

	node := raft.NewNode(int32(*id), peerClients, peerClientsByID)
	srv := &server.Server{Node: node}

	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("node %d: failed to listen on :%s: %v", *id, *port, err)
	}
	grpcServer := grpc.NewServer()
	proto.RegisterRaftServer(grpcServer, srv)

	go func() {
		log.Printf("node %d: gRPC listening on :%s", *id, *port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("node %d: serve failed: %v", *id, err)
		}
	}()

	httpServer := server.NewHTTPServer(node, peerHTTP)
	go func() {
		log.Printf("node %d: HTTP API listening on :%s", *id, *httpPort)
		if err := httpServer.Start(":" + *httpPort); err != nil {
			log.Fatalf("node %d: HTTP serve failed: %v", *id, err)
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

func parsePeerHTTP(s string) map[int32]string {
	result := make(map[int32]string)
	for _, pair := range strings.Split(s, ",") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}
		idNum, err := strconv.Atoi(kv[0])
		if err != nil {
			continue
		}
		result[int32(idNum)] = kv[1]
	}
	return result
}