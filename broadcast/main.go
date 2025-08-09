package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Message int `json:"message"`
}

type TopologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	node := maelstrom.NewNode()

	var ids []int

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcast BroadcastMessage
		if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
			return err
		}

		ids = append(ids, broadcast.Message)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")

		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body = map[string]any{
			"type":     "read_ok",
			"messages": ids,
		}

		return node.Reply(msg, body)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		var topology TopologyMessage
		if err := json.Unmarshal(msg.Body, &topology); err != nil {
			return err
		}

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "topology_ok"
		delete(body, "topology")

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
