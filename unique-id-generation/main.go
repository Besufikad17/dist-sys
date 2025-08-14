package main

import (
	"encoding/json"
	"log"
	"math/rand"

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

type GenerateReply struct {
	Reply
	Id int `json:"id"`
}

func main() {
	node := maelstrom.NewNode()

	node.Handle("generate", func(msg maelstrom.Message) error {
		var body Body
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		reply := GenerateReply{
			Reply: Reply{
				Type:      "generate_ok",
				InReplyTo: body.InReplyTo,
			},
			Id: rand.Int(),
		}

		return node.Reply(msg, reply)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
