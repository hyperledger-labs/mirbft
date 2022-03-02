/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// ********************************************************************************
//       Chat demo application for demonstrating the usage of MirBFT             //
//                            (application logic)                                //
// ********************************************************************************

package main

import (
	"fmt"
	"github.com/hyperledger-labs/mirbft/pkg/modules"
	"github.com/hyperledger-labs/mirbft/pkg/pb/requestpb"
	"google.golang.org/protobuf/proto"
)

// ChatApp and its methods implement the application logic of the small chat demo application
// showcasing the usage of the MirBFT library.
// An initialized instance of this struct needs to be passed to the mirbft.NewNode() method of all nodes
// for the system to run the chat demo app.
type ChatApp struct {

	// The only state of the application is the chat message history,
	// to which each delivered request appends one message.
	messages []string

	// The request store module (also passed to the MirBFT library at startup)
	// is used for accessing the request payloads containing the chat message data.
	reqStore modules.RequestStore
}

// NewChatApp returns a new instance of the chat demo application.
// The reqStore must be the same request store that is passed to the mirbft.NewNode() function as a module.
func NewChatApp(reqStore modules.RequestStore) *ChatApp {
	return &ChatApp{
		messages: make([]string, 0),
		reqStore: reqStore,
	}
}

// Apply applies a batch of requests to the state of the application.
// In our case, it simply extends the message history
// by appending the payload of each received request as a new chat message.
// Each appended message is also printed to stdout.
func (chat *ChatApp) Apply(batch *requestpb.Batch) error {

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

// Snapshot returns a binary representation of the application state.
// The returned value can be passed to RestoreState().
// At the time of writing this comment, the MirBFT library does not support state transfer
// and Snapshot is never actually called.
// We include its implementation for completeness.
func (chat *ChatApp) Snapshot() ([]byte, error) {

	// We use protocol buffers to serialize the application state.
	state := &AppState{
		Messages: chat.messages,
	}
	return proto.Marshal(state)
}

// RestoreState restores the application's state to the one represented by the passed argument.
// The argument is a binary representation of the application state returned from Snapshot().
// After the chat history is restored, RestoreState prints the whole chat history to stdout.
func (chat *ChatApp) RestoreState(snapshot []byte) error {

	// Unmarshal the protobuf message from its binary form.
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
