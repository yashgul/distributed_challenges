package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	log.Println("Initializing node...")

	var message_ids sync.Map
	var unsent_message_ids sync.Map
	nodes := []string{"n0", "n1", "n2", "n3", "n4"}

	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		log.Println("Handling broadcast request")

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		log.Println(body)

		// store the message ID
		message_ids.Store(body["message"], true)
		unsent_message_ids.Store(body["message"], true)

		if val, ok := body["isRebroadcast"]; !ok || val == false {
			log.Println("Send re-broadcast message to all nodes")

			response := map[string]any{"type": "broadcast_ok"}
			n.Reply(msg, response)

			for _, next_node := range nodes {
				unsent_message_ids.Range(func(unsent_message_id, _ interface{}) bool {
					rebroadcast_request := map[string]any{"type": "broadcast", "message": unsent_message_id, "isRebroadcast": true}
					n.Send(next_node, rebroadcast_request)
					return true
				})
			}

			unsent_message_ids = sync.Map{}

		}

		return nil

	})

	n.Handle("read", func(msg maelstrom.Message) error {
		log.Println("Handling read request")

		// Convert stored message IDs into a slice
		var message_ids_arr []any
		message_ids.Range(func(k, v interface{}) bool {
			message_ids_arr = append(message_ids_arr, k)
			return true
		})

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
