// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Watcher show command
func watcherShow(server string, showWhat string) (result string) {

	if server == "" {
		return "" +
			"/notehub prod show <what>\n" +
			"/notehub staging show <what>\n" +
			"/notehub <yourserver> show <what>\n" +
			""
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
	var errstr string
	var handlerNodeIDs, handlerAddrs []string

	if server == "api.r" {

		// Special case debugging for server "r" meaning Ray's localhost
		// Local dev doesn't support http, and staging/production don't support https
		handlerNodeIDs = []string{"Ray's Local Dev"}
		handlerAddrs = []string{"https://api.ray.blues.tools"}

	} else {

		// Get the list of handlers on the server
		handlerNodeIDs, handlerAddrs, errstr = watcherGetHandlers(server)
		if errstr != "" {
			return errstr
		}

	}

	// Show the handlers
	for i, addr := range handlerAddrs {
		response += fmt.Sprintf("%s\n", handlerNodeIDs[i])
		r, errstr := watcherShowHandler(addr, showWhat)
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
func watcherGetHandlers(server string) (handlerNodeIDs []string, handlerAddrs []string, errstr string) {

	rsp, err := http.Get("https://" + server + "/ping?show=\"handlers\"")
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
		addr := fmt.Sprintf("http://%s:%d", h.PublicIpv4, h.HTTPPort)
		handlerAddrs = append(handlerAddrs, addr)
	}

	return

}

func getHandlerInfo(addr string, showWhat string) (pb PingBody, errstr string) {

	// Get the data
	url := fmt.Sprintf("%s/ping?show=\"%s\"", addr, showWhat)

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
func watcherShowHandler(addr string, showWhat string) (response string, errstr string) {

	// If showing nothing, done
	if showWhat == "" {
		return
	}

	// Get the info from the handler
	var pb PingBody
	pb, errstr = getHandlerInfo(addr, showWhat)
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

	case "lb":
		if pb.Body.LBStatus == nil {
			response = "no load balancer information available"
			return
		}
		rspJSON, err := json.Marshal(*pb.Body.LBStatus)
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
