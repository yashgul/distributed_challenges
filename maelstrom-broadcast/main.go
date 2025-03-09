package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	log.Println("Initializing node...")

	message_ids := make(map[any]bool)

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		log.Println("Handling broadcast request")

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// store the message ID
		message_ids[body["message"]] = true

		response := map[string]any{"type": "broadcast_ok"}
		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		log.Println("Handling read request")

		// Convert stored message IDs into a slice
		message_ids_arr := make([]any, 0, len(message_ids))
		for k := range message_ids {
			message_ids_arr = append(message_ids_arr, k)
		}

		response := map[string]any{
			"type":     "read_ok",
			"messages": message_ids_arr,
		}
		return n.Reply(msg, response)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		log.Println("Handling topology update")

		response := map[string]any{"type": "topology_ok"}
		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
