// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Watcher show command
func watcherShow(server string, showWhat string) (result string) {

	if server == "" {
		return "" +
			"show prod <what>\n" +
			"show staging <what>\n" +
			"show <yourserver> <what>\n" +
			""
	}

	if server == "r" {
		return watcherShowServer("api.ray.blues.tools", showWhat)
	}

	if server == "p" || server == "prod" || server == "production" {
		return watcherShowServer("api.notefile.net", showWhat)
	}

	if server == "s" || server == "staging" {
		return watcherShowServer("api.staging.blues.tools", showWhat)
	}

	return watcherShowServer("api."+server, showWhat)

}

// Show something about the server
func watcherShowServer(server string, showWhat string) (response string) {

	// Get the list of handlers on the server
	handlerNodeIDs, handlerAddrs, errstr := watcherGetHandlers(server)
	if errstr != "" {
		return errstr
	}

	// Show the handlers
	for i, addr := range handlerAddrs {
		response += fmt.Sprintf("%s\n", handlerNodeIDs[i])
		r, errstr := watcherShowHandler(addr, showWhat)
		if errstr != "" {
			return response + errstr
		}
		response += r
	}

	// Done
	return response
}

// Get the list of handlers
func watcherGetHandlers(server string) (handlerNodeIDs []string, handlerAddrs []string, response string) {

	rsp, err := http.Get(server + "/ping")
	if err != nil {
		response = err.Error()
		return
	}
	defer rsp.Body.Close()

	rspJSON, err := io.ReadAll(rsp.Body)
	if err != nil {
		response = err.Error()
		return
	}

	var pr PingRequest
	err = json.Unmarshal(rspJSON, &pr)
	if err != nil {
		response = err.Error()
		return
	}

	for _, h := range *pr.AppHandlers {
		handlerNodeIDs = append(handlerNodeIDs, h.NodeID)
		handlerAddrs = append(handlerAddrs, h.Ipv4)
	}

	return

}

// Show something about a handler
func watcherShowHandler(addr string, showWhat string) (response string, errstr string) {

	// If showing nothing, done
	if showWhat == "" {
		return
	}

	// Get the data
	url := fmt.Sprintf("%s/ping?show=\"%s\"", addr, showWhat)
	rsp, err := http.Get(url)
	if err != nil {
		errstr = err.Error()
		return
	}
	defer rsp.Body.Close()

	rspJSON, err := io.ReadAll(rsp.Body)
	if err != nil {
		errstr = err.Error()
		return
	}

	var pr PingRequest
	err = json.Unmarshal(rspJSON, &pr)
	if err != nil {
		errstr = err.Error()
		return
	}

	// Return it
	switch showWhat {

	case "goroutines":
		response = pr.GoroutineStatus
		return

	case "heap":
		response = pr.HeapStatus
		return

	case "lb":
		if pr.LBStatus == nil {
			response = "no load balancer information available"
			return
		}
		rspJSON, err := json.Marshal(*pr.LBStatus)
		if err != nil {
			errstr = err.Error()
		} else {
			response = string(rspJSON)
		}
		return

	case "handlers":
		if pr.AppHandlers == nil {
			response = "no handler information available"
			return
		}
		rspJSON, err := json.Marshal(*pr.AppHandlers)
		if err != nil {
			errstr = err.Error()
		} else {
			response = string(rspJSON)
		}
		return

	}

	// Unknown object to show
	errstr = "unknown type: " + showWhat
	return
}
