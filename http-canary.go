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
	"sync"
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
	warnings     int64
}

var lastDeviceEventLock sync.Mutex
var lastDeviceEvent map[string]lastEvent

// Canary handler
func inboundWebCanaryHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Instantiate the map
	lastDeviceEventLock.Lock()
	if lastDeviceEvent == nil {
		lastDeviceEvent = map[string]lastEvent{}
	}
	lastDeviceEventLock.Unlock()

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
		lastDeviceEventLock.Lock()
		last, present := lastDeviceEvent[e.DeviceUID]
		if present && e.Body != nil {
			body := *e.Body
			last.continuous = strings.Contains(body["why"].(string), "continuous")
		}
		lastDeviceEvent[e.DeviceUID] = last
		lastDeviceEventLock.Unlock()
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
	lastDeviceEventLock.Lock()
	errstr := ""
	last, present := lastDeviceEvent[e.DeviceUID]
	if present {
		if this.continuous && this.sessionID != last.sessionID {
			errstr = "continuous session dropped and reconnected: " + this.sessionID
		} else if this.seqNo != last.seqNo+1 {
			errstr = fmt.Sprintf("sequence out of order (expected %d but received %d): %s", last.seqNo+1, this.seqNo, e.EventUID)
		} else if (this.receivedTime - this.capturedTime) > 120 {
			errstr = fmt.Sprintf("event took %d secs to get from notecard to notehub: %s", this.receivedTime-this.capturedTime, e.EventUID)
		} else if (this.routedTime - this.receivedTime) > 10 {
			errstr = fmt.Sprintf("event took %d secs to be routed once it was received by notehub: %s", this.routedTime-this.receivedTime, e.EventUID)
		} else if (this.receivedTime - last.receivedTime) > 5*60 {
			errstr = fmt.Sprintf("%d minutes between events received by notehub: %s", (this.routedTime-this.receivedTime)/60, e.EventUID)
		}
	}
	lastDeviceEvent[e.DeviceUID] = this
	lastDeviceEventLock.Unlock()

	// Send message
	if errstr != "" {
		canaryMessage(e.DeviceUID, errstr)
	}

}

// Canary handler
func canarySweepDevices() {

	// Instantiate the map
	lastDeviceEventLock.Lock()
	if lastDeviceEvent == nil {
		lastDeviceEvent = map[string]lastEvent{}
	}
	lastDeviceEventLock.Unlock()

	// Look at the map to see if there's anything due

	lastDeviceEventLock.Lock()
	now := time.Now().UTC().Unix()
	for deviceUID, last := range lastDeviceEvent {
		if now-last.receivedTime >= 6*60 {
			last.warnings++
			lastDeviceEvent[deviceUID] = last
			if last.warnings < 10 {
				canaryMessage(deviceUID, fmt.Sprintf("no routed events received in %d minutes (last event received %s)", (now-last.receivedTime)/60,
					time.Unix(last.receivedTime, 0).UTC().Format("01-02 15:04:05")))
			} else if last.warnings == 10 {
				canaryMessage(deviceUID, "LAST WARNING before silence!")
			}
		}
	}
	lastDeviceEventLock.Unlock()

}

// Output a canary message
func canaryMessage(deviceUID string, message string) {
	slackSendMessage(fmt.Sprintf("canary: %s %s", deviceUID, message))
}
