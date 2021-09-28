/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// TODO: Write proper comments.

package main

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/messagepb"
	"google.golang.org/protobuf/proto"
)

type ChatApp struct {
	messages []string
	reqStore modules.RequestStore
}

func NewChatApp(reqStore modules.RequestStore) *ChatApp {
	return &ChatApp{
		messages: make([]string, 0),
		reqStore: reqStore,
	}
}

func (chat *ChatApp) Apply(batch *messagepb.Batch) error {

	// For each request in the batch
	for _, reqRef := range batch.Requests {

		// Extract request data from the request store and construct a printable chat message.
		reqData, err := chat.reqStore.GetRequest(reqRef)
		if err != nil {
			return err
		}
		chatMessage := fmt.Sprintf("Client %d: %s", reqRef.ClientId, string(reqData))

		// Append the received chat message to the chat history.
		chat.messages = append(chat.messages, chatMessage)

		// Print received chat message.
		fmt.Println(chatMessage)
	}
	return nil
}

func (chat *ChatApp) Snapshot() ([]byte, error) {
	state := &AppState{
		Messages: chat.messages,
	}
	return proto.Marshal(state)
}

func (chat *ChatApp) RestoreState(snapshot []byte) error {
	state := &AppState{}
	if err := proto.Unmarshal(snapshot, state); err != nil {
		return err
	}

	// Restore internal state
	chat.messages = state.Messages

	// Print new state
	fmt.Println("\n CHAT STATE RESTORED. SHOWING ALL CHAT HISTORY FROM THE BEGINNING.\n")
	for _, message := range chat.messages {
		fmt.Println(message)
	}

	return nil
}
