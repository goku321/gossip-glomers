package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
)

type server struct {
	node             *maelstrom.Node
	neighbourNodes   []string
	msgIDReceived    []float64
	msgIDMutex       *sync.RWMutex
	msgReceiveCh     chan msgBroadcast
	receivedMessages map[float64]struct{}
}

type msgBroadcast struct {
	src  string
	body map[string]any
}

func newServer(node *maelstrom.Node) *server {
	return &server{node: node, msgIDReceived: []float64{}, msgIDMutex: &sync.RWMutex{}, msgReceiveCh: make(chan msgBroadcast), receivedMessages: map[float64]struct{}{}}
}

type messageType string

func (mType messageType) toString() string {
	return string(mType)
}

const (
	BROADCAST messageType = "broadcast"
	READ      messageType = "read"
	TOPOLOGY  messageType = "topology"
	INIT      messageType = "init"
)

func (s *server) broadcastToNodes(src string, body map[string]any) error {
	for _, nodeID := range s.node.NodeIDs() {
		s.neighbourNodes = append(s.neighbourNodes, nodeID)
	}
	return s.broadcast(src, body)
}

func (s *server) broadcast(src string, body map[string]any) error {
	for _, destNodeID := range s.neighbourNodes {
		if s.node.ID() != destNodeID && src != destNodeID {
			go func() {
				if err := s.node.Send(destNodeID, body); err != nil {
					log.Fatal(err)
				}
			}()
		}
	}
	return nil
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgValue := body["message"].(float64)
	// Store only if it's not already present.
	s.msgIDMutex.Lock()
	if _, ok := s.receivedMessages[msgValue]; ok {
		s.msgIDMutex.Unlock()
		return nil
	}
	s.receivedMessages[msgValue] = struct{}{}
	s.msgIDMutex.Unlock()
	s.broadcastToNodes(msg.Src, body)

	reply := map[string]any{
		"type": "broadcast_ok",
	}
	return s.node.Reply(msg, reply)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var replyMessages []float64
	s.msgIDMutex.RLock()
	for msg := range s.receivedMessages {
		replyMessages = append(replyMessages, msg)
	}
	s.msgIDMutex.RUnlock()
	reply := map[string]any{
		"type":     "read_ok",
		"messages": replyMessages,
	}
	return s.node.Reply(msg, reply)
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	reply := map[string]any{
		"type": "topology_ok",
	}
	return s.node.Reply(msg, reply)
}

func main() {
	n := maelstrom.NewNode()
	s := newServer(n)
	n.Handle(BROADCAST.toString(), s.broadcastHandler)
	n.Handle(READ.toString(), s.readHandler)
	n.Handle(TOPOLOGY.toString(), s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
