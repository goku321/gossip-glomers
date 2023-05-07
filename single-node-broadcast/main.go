package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type server struct {
	node *maelstrom.Node
}

func newServer(node *maelstrom.Node) *server {
	return &server{node}
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

var msgIDReceived []float64

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	msgIDReceived = append(msgIDReceived, body["message"].(float64))

	reply := map[string]any{
		"type": "broadcast_ok",
	}
	return s.node.Reply(msg, reply)
}

func (s *server) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	reply := map[string]any{
		"type": "read_ok",
		"messages": msgIDReceived,
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
