package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"context"
	"aegian/internal/raft"
	"aegian/proto"
)

type HTTPServer struct {
	node      *raft.Node
	peerHTTP  map[int32]string
}

func NewHTTPServer(node *raft.Node, peerHTTP map[int32]string) *HTTPServer {
	return &HTTPServer{node: node, peerHTTP: peerHTTP}
}

func (h *HTTPServer) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/aegian/", h.handleKV)
	return http.ListenAndServe(addr, mux)
}

func (h *HTTPServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/aegian/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, key)
	case http.MethodPut:
		h.handlePut(w, r, key)
	case http.MethodDelete:
		h.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *HTTPServer) handleGet(w http.ResponseWriter, key string) {
	value, ok := h.node.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	fmt.Fprintln(w, value)
}

func (h *HTTPServer) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}
	value := strings.TrimSpace(string(body))
	if value == "" {
		http.Error(w, "missing value", http.StatusBadRequest)
		return
	}

	command := "PUT " + key + " " + value
	h.applyWrite(w, command)
}

func (h *HTTPServer) handleDelete(w http.ResponseWriter, key string) {
	command := "DEL " + key
	h.applyWrite(w, command)
}

func (h *HTTPServer) forwardToLeader(w http.ResponseWriter, command string) {
	leader, ok := h.node.LeaderClient()
	if !ok {
		http.Error(w, "no leader available, try again", http.StatusServiceUnavailable)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reply, err := leader.ClientCommand(ctx, &proto.ClientCommandRequest{Command: command})
	if err != nil {
		http.Error(w, "could not reach leader, try again", http.StatusServiceUnavailable)
		return
	}

	if reply.GetSuccess() {
		fmt.Fprintln(w, "OK")
	} else {
		http.Error(w, reply.GetMessage(), http.StatusServiceUnavailable)
	}
}

func (h *HTTPServer) applyWrite(w http.ResponseWriter, command string) {
	err := h.node.ProposeAndWait(command, 3*time.Second)
	switch err {
	case nil:
		fmt.Fprintln(w, "OK")
		return
	case raft.ErrTimeout:
		http.Error(w, "commit timed out, try again", http.StatusServiceUnavailable)
		return
	case raft.ErrNotLeader:
		h.forwardToLeader(w, command)
		return
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}