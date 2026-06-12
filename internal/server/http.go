package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"aegian/internal/raft"
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

func (h *HTTPServer) applyWrite(w http.ResponseWriter, command string) {
	err := h.node.ProposeAndWait(command, 3*time.Second)
	switch err {
	case nil:
		fmt.Fprintln(w, "OK")
	case raft.ErrNotLeader:
		leaderID := h.node.LeaderID()
		addr, found := h.peerHTTP[leaderID]
		if !found || leaderID == 0 {
			http.Error(w, "no leader available, try again", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "not the leader — try node %d at %s\n", leaderID, addr)
		w.WriteHeader(http.StatusMisdirectedRequest)
	case raft.ErrTimeout:
		http.Error(w, "commit timed out, try again", http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}