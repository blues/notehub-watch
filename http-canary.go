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
type deviceContext struct {
	sn         string
	continuous bool
	warnings   int64
}
type lastEvent struct {
	sessionID    string
	seqNo        int64
	capturedTime int64
	receivedTime int64
	routedTime   int64
}

var canaryLock sync.Mutex
var last map[string]lastEvent
var device map[string]deviceContext

// Canary handler
func inboundWebCanaryHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Exit
	if Config.CanaryDisabled {
		return
	}

	// Instantiate the map
	canaryLock.Lock()
	if last == nil {
		last = map[string]lastEvent{}
	}
	if device == nil {
		device = map[string]deviceContext{}
	}
	canaryLock.Unlock()

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
		canaryLock.Lock()
		d, present := device[e.DeviceUID]
		if present && e.Body != nil {
			body := *e.Body
			d.continuous = strings.Contains(body["why"].(string), "continuous")
		}
		d.sn = e.DeviceSN
		device[e.DeviceUID] = d
		canaryLock.Unlock()
		return
	}

	// Ignore non-data events
	if e.NotefileID != "_temp.qo" {
		return
	}

	// Determine the various latencies
	var t lastEvent
	t.sessionID = e.SessionUID
	t.capturedTime = e.When
	t.receivedTime = int64(e.Received)
	t.routedTime = time.Now().UTC().Unix()
	if e.Body != nil {
		body := *e.Body
		t.seqNo = int64(body["count"].(float64))
	}

	// Alert
	canaryLock.Lock()
	errstr := ""
	d, present := device[e.DeviceUID]
	if present {
		d.sn = e.DeviceSN
		device[e.DeviceUID] = d

		var secsCapturedToReceived, secsReceivedToReceived int64
		secsCapturedToReceived = 120
		secsReceivedToReceived = 5 * 60
		if strings.HasPrefix(d.sn, "ntn") {
			// For NTN, the packet interval is 15m
			secsCapturedToReceived = 20 * 60
			secsReceivedToReceived = 25 * 60
		}

		l := last[e.DeviceUID]
		if d.continuous && t.sessionID != l.sessionID {
			errstr = "continuous session dropped and reconnected: " + t.sessionID
		} else if t.seqNo != l.seqNo+1 {
			errstr = fmt.Sprintf("sequence out of order (expected %d but received %d): %s", l.seqNo+1, t.seqNo, e.EventUID)
		} else if (t.receivedTime - t.capturedTime) > secsCapturedToReceived {
			errstr = fmt.Sprintf("event took %d secs to get from notecard to notehub: %s", t.receivedTime-t.capturedTime, e.EventUID)
		} else if (t.routedTime - t.receivedTime) > 10 {
			errstr = fmt.Sprintf("event took %d secs to be routed once it was received by notehub: %s", t.routedTime-t.receivedTime, e.EventUID)
		} else if (t.receivedTime - l.receivedTime) > secsReceivedToReceived {
			errstr = fmt.Sprintf("%d minutes between events received by notehub: %s", (t.routedTime-t.receivedTime)/60, e.EventUID)
		}
	}
	last[e.DeviceUID] = t
	canaryLock.Unlock()

	// Send message
	if errstr != "" {
		canaryMessage(e.DeviceUID, e.DeviceSN, errstr)
	}

}

// Canary handler
func canarySweepDevices() {

	// Exit if disabled
	if Config.CanaryDisabled {
		return
	}

	// Instantiate the map
	canaryLock.Lock()
	if last == nil {
		last = map[string]lastEvent{}
	}
	if device == nil {
		device = map[string]deviceContext{}
	}
	// Make a copy of these structures so we don't hold the mutex for very long
	deviceCopy := device
	lastCopy := last
	canaryLock.Unlock()

	// Look at the map to see if there's anything due
	now := time.Now().UTC().Unix()
	for deviceUID, d := range deviceCopy {
		l := lastCopy[deviceUID]

		var receivedInterval int64
		receivedInterval = 6 * 60
		if strings.HasPrefix(d.sn, "ntn") {
			// For NTN, the packet interval is 15m
			receivedInterval = 20 * 60
		}

		if now-l.receivedTime >= receivedInterval {
			d.warnings++
			deviceCopy[deviceUID] = d
			canaryLock.Lock()
			device[deviceUID] = d
			canaryLock.Unlock()
			if d.warnings < 10 {
				canaryMessage(deviceUID, d.sn, fmt.Sprintf("no routed events received in %d minutes (last event received %s)", (now-l.receivedTime)/60,
					time.Unix(l.receivedTime, 0).UTC().Format("01-02 15:04:05")))
			} else if d.warnings == 10 {
				canaryMessage(deviceUID, d.sn, "LAST WARNING before silence!")
			}
		}
	}

}

// Output a canary message
func canaryMessage(deviceUID string, sn string, message string) {
	slackSendMessage(fmt.Sprintf("canary: %s %s %s", sn, deviceUID, message))
}
