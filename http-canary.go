// Copyright 2020 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

// Serves Health Checks
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/blues/note-go/note"
)

// Canary handler
func inboundWebCanaryHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Exit if someone is probing us
	if httpReq.Method == "GET" {
		return
	}

	// Get the body if supplied
	eventJSON, err := ioutil.ReadAll(httpReq.Body)
	if err != nil {
		eventJSON = []byte("{}")
	}

	// Unmarshal to an event
	var e note.Event
	err = json.Unmarshal(eventJSON, &e)
	if err != nil {
		return
	}

	// Ignore non-data events
	if e.NotefileID != "_temp.qo" {
		return
	}

	// Determine the various latencies
	capturedTime := e.When
	receivedTime := int64(e.Received)
	routedTime := time.Now().UTC().Unix()
	body := *e.Body
	seqNo := body["count"].(int64)
	sessionID := e.SessionUID

	// Determine the difference between note creation time and the time of receipt
	fmt.Printf("\ncaptured: %d\n", capturedTime)
	fmt.Printf("\nreceived: %d\n", receivedTime)
	fmt.Printf("\nrouted: %d\n", routedTime)
	fmt.Printf("\nseqno: %d\n", seqNo)
	fmt.Printf("\nsessionID: %s\n", sessionID)

}
