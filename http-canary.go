// Copyright 2020 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

// Serves Health Checks
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/blues/note-go/note"
)

// Retained between canary notifications
type lastEvent struct {
	continuous   bool
	sessionID    string
	seqNo        int64
	capturedTime int64
	receivedTime int64
	routedTime   int64
}

var lastDeviceEvent map[string]lastEvent

// Canary handler
func inboundWebCanaryHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Instantiate the map
	if lastDeviceEvent == nil {
		lastDeviceEvent = map[string]lastEvent{}
	}

	// Exit if someone is probing us
	if httpReq.Method == "GET" {
		return
	}

	// Get the body if supplied
	eventJSON, err := io.ReadAll(httpReq.Body)
	if err != nil {
		eventJSON = []byte("{}")
	}

	// Unmarshal to an event
	var e note.Event
	err = json.Unmarshal(eventJSON, &e)
	if err != nil {
		return
	}

	// Remember info about the last session
	if e.NotefileID == "_session.qo" {
		last, present := lastDeviceEvent[e.DeviceUID]
		if present && e.Body != nil {
			body := *e.Body
			last.continuous = strings.Contains(body["why"].(string), "continuous")
		}
		lastDeviceEvent[e.DeviceUID] = last
		return
	}

	// Ignore non-data events
	if e.NotefileID != "_temp.qo" {
		return
	}

	// Determine the various latencies
	var this lastEvent
	this.sessionID = e.SessionUID
	this.capturedTime = e.When
	this.receivedTime = int64(e.Received)
	this.routedTime = time.Now().UTC().Unix()
	if e.Body != nil {
		body := *e.Body
		this.seqNo = int64(body["count"].(float64))
	}

	// Alert
	errstr := ""
	last, present := lastDeviceEvent[e.DeviceUID]
	if present {
		if this.continuous && this.sessionID != last.sessionID {
			errstr = "canary: continuous session dropped and reconnected"
		} else if this.seqNo != last.seqNo+1 {
			errstr = fmt.Sprintf("canary: sequence out of order (expected %d but received %d)", last.seqNo+1, this.seqNo)
		} else if (this.receivedTime - this.capturedTime) > 120 {
			errstr = fmt.Sprintf("canary: event took %d secs to get from notecard to notehub\n", this.receivedTime-this.capturedTime)
		} else if (this.routedTime - this.receivedTime) > 15 {
			errstr = fmt.Sprintf("canary: event took %d secs to be routed once it was received by notehub\n", this.routedTime-this.receivedTime)
		} else if (this.receivedTime - last.receivedTime) > 5*60 {
			errstr = fmt.Sprintf("canary: %d minutes between events received by notehub\n", (this.routedTime-this.receivedTime)/60)
		}
	}
	lastDeviceEvent[e.DeviceUID] = this

	// Send message
	if errstr != "" {
		fmt.Printf("%s\n", errstr)
	}

	// Determine the difference between note creation time and the time of receipt
	fmt.Printf("captured: %d\n", this.capturedTime)
	fmt.Printf("received: %d\n", this.receivedTime)
	fmt.Printf("routed: %d\n", this.routedTime)
	fmt.Printf("seqno: %d\n", this.seqNo)
	fmt.Printf("sessionID: %s\n", this.sessionID)
	fmt.Printf("\n")

}
