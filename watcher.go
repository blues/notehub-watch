// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Watcher show command
func watcherShow(host string, showWhat string) (result string) {

	if host == "" {
		return "" +
			"/notehub <host>\n" +
			"/notehub <host> show <what>\n" +
			"<host> is prod, staging, or your local hostname\n" +
			"<what> is goroutines, heap, handlers\n" +
			""
	}

	targetHost := host

	// Production
	if host == "p" || host == "prod" || host == "production" {
		targetHost = "notefile.net"
	}

	// Staging
	if host == "s" || host == "staging" {
		targetHost = "staging.blues.tools"
	}

	// Localdev
	if !strings.Contains(host, ".") {
		targetHost = host + ".blues.tools"
	}

	// We must target the API service for this host
	if !strings.HasPrefix(targetHost, "api.") {
		targetHost = "api." + targetHost
	}

	return watcherShowHost(targetHost, showWhat)

}

// Show something about the host
func watcherShowHost(host string, showWhat string) (response string) {

	// If showing nothing, done
	if showWhat == "" {
		return sheetGetHostStats(host)
	}

	// Get the list of handlers on the host
	handlerNodeIDs, handlerAddrs, handlerTypes, errstr := watcherGetHandlers(host)
	if errstr != "" {
		return errstr
	}

	// Show the handlers
	for i, addr := range handlerAddrs {
		response += "\n"
		response += fmt.Sprintf("*NODE %s (%s)*\n", handlerNodeIDs[i], handlerTypes[i])
		r, errstr := watcherShowHandler(addr, handlerNodeIDs[i], showWhat)
		if errstr != "" {
			response += "  " + errstr + "\n"
		} else {
			response += r
		}
	}

	// Done
	return response
}

// Get the list of handlers
func watcherGetHandlers(host string) (handlerNodeIDs []string, handlerAddrs []string, handlerTypes []string, errstr string) {

	url := "https://" + host + "/ping?show=\"handlers\""
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		errstr = err.Error()
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}
	rsp, err := httpclient.Do(req)
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

	var pb PingBody
	err = json.Unmarshal(rspJSON, &pb)
	if err != nil {
		errstr = string(rspJSON)
		return
	}

	if pb.Body.AppHandlers == nil {
		errstr = "no handlers in " + string(rspJSON)
		return
	}
	for _, h := range *pb.Body.AppHandlers {
		handlerNodeIDs = append(handlerNodeIDs, h.NodeID)
		handlerTypes = append(handlerTypes, h.PrimaryService)
		addr := fmt.Sprintf("http://%s", host)
		handlerAddrs = append(handlerAddrs, addr)
	}

	return

}

func getHandlerInfo(addr string, nodeID string, showWhat string) (pb PingBody, errstr string) {

	// Get the data
	url := fmt.Sprintf("%s/ping?show=\"%s\"", addr, showWhat)
	if nodeID != "" {
		url = fmt.Sprintf("%s/ping?node=\"%s\"&show=\"%s\"", addr, nodeID, showWhat)
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		errstr = err.Error()
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}
	rsp, err := httpclient.Do(req)
	if err != nil {
		errstr = err.Error()
		return
	}
	defer rsp.Body.Close()

	// Read the body
	rspJSON, err := io.ReadAll(rsp.Body)
	if err != nil {
		errstr = err.Error()
		return
	}

	// Unmarshal it
	err = json.Unmarshal(rspJSON, &pb)
	if err != nil {
		errstr = string(rspJSON)
		return
	}

	// Done
	return

}

// Show something about a handler
func watcherShowHandler(addr string, nodeID string, showWhat string) (response string, errstr string) {

	// Get the info from the handler
	var pb PingBody
	pb, errstr = getHandlerInfo(addr, nodeID, showWhat)
	if errstr != "" {
		return
	}

	// Return it
	switch showWhat {

	case "goroutines":
		response = pb.Body.GoroutineStatus
		return

	case "heap":
		response = pb.Body.HeapStatus
		return

	case "handlers":
		if pb.Body.AppHandlers == nil {
			response = "no handler information available"
			return
		}
		rspJSON, err := json.MarshalIndent(*pb.Body.AppHandlers, "", "    ")
		if err != nil {
			errstr = err.Error()
		} else {
			response = string(rspJSON)
		}
		return

	case "lb":
		if pb.Body.LBStatus == nil {
			response = "no load balancer information available"
			return
		}
		rspJSON, err := json.MarshalIndent(*pb.Body.LBStatus, "", "    ")
		if err != nil {
			errstr = err.Error()
		} else {
			response = string(rspJSON)
		}
		return

	}

	// Unknown object to show
	errstr = "unknown 'show' type: " + showWhat
	return
}
