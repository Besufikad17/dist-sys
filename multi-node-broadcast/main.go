package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Body struct {
	Type      string `json:"type"`
	MsgId     int    `json:"msg_id"`
	InReplyTo int    `json:"in_reply_to"`
}

type Reply struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

type ReadReply struct {
	Reply
	Messages []int `json:"messages"`
}

type BroadcastMessage struct {
	Body
	Message int `json:"message"`
}

type DeliverMessage struct {
	BroadcastMessage
}

type TopologyMessage struct {
	Body
	Topology map[string][]string `json:"topology"`
}

func main() {
	node := maelstrom.NewNode()

	var ids []int
	var topology TopologyMessage

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var broadcast BroadcastMessage
		if err := json.Unmarshal(msg.Body, &broadcast); err != nil {
			return err
		}

		ids = append(ids, int(broadcast.Message))

		reply := Reply{
			Type:      "broadcast_ok",
			InReplyTo: broadcast.Body.InReplyTo,
		}

		for _, neighbor := range node.NodeIDs() {
			req := DeliverMessage{
				BroadcastMessage: BroadcastMessage{
					Body: Body{
						Type:      "deliver",
						MsgId:     broadcast.MsgId,
						InReplyTo: broadcast.InReplyTo,
					},
					Message: broadcast.Message,
				},
			}

			if err := node.Send(neighbor, req); err != nil {
				return err
			}
		}

		return node.Reply(msg, reply)
	})

	node.Handle("deliver", func(msg maelstrom.Message) error {
		var deliverMessage DeliverMessage
		if err := json.Unmarshal(msg.Body, &deliverMessage); err != nil {
			return err
		}

		ids = append(ids, int(deliverMessage.Message))

		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		reply := ReadReply{
			Reply: Reply{
				Type:      "read_ok",
				InReplyTo: body.InReplyTo,
			},
			Messages: ids,
		}

		return node.Reply(msg, reply)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		if err := json.Unmarshal(msg.Body, &topology); err != nil {
			return err
		}

		reply := Reply{
			Type:      "topology_ok",
			InReplyTo: topology.Body.InReplyTo,
		}

		return node.Reply(msg, reply)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
