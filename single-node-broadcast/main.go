package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type server struct {
	node *maelstrom.Node
	neighbourNodes []string
	msgIDReceived []float64
}

func newServer(node *maelstrom.Node) *server {
	node.NodeIDs()
	return &server{node: node, msgIDReceived: []float64{}}
}

type messageType string

func (mType messageType) toString() string {
	return string(mType)
}

const (
	BROADCAST messageType = "broadcast"
	READ messageType = "read"
	TOPOLOGY messageType = "topology"
)

func (s *server) initHandler(msg maelstrom.Message) error {
	for _, nodeID := range s.node.NodeIDs() {
		s.neighbourNodes = append(s.neighbourNodes, nodeID)
	}
	return s.node.Reply(msg, nil)
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.msgIDReceived = append(s.msgIDReceived, body["message"].(float64))

	reply := map[string]any{
		"type": "broadcast_ok",
	}
	s.node.NodeIDs()
	return s.node.Reply(msg, reply)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	reply := map[string]any{
		"type": "read_ok",
		"messages": s.msgIDReceived,
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
