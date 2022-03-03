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

	if server == "api.local" {

		// Special case debugging for server "local" meaning Ray's localhost
		// Local dev doesn't support http, and staging/production don't support https
		handlerNodeIDs = []string{"Local Dev"}
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

	url := "https://" + server + "/ping?show=\"handlers\""
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
		return watcherGetHandlerStats(addr)
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
		rspJSON, err := json.MarshalIndent(*pb.Body.LBStatus, "", "    ")
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

// Get general load stats for a handler
func watcherGetHandlerStats(addr string) (response string, errstr string) {
	bol := "  "
	eol := "\n"

	// Get the info from the handler
	var pb PingBody
	pb, errstr = getHandlerInfo(addr, "lb")
	if errstr != "" {
		return
	}

	// Generate summary info
	if pb.Body.LBStatus != nil && len(*pb.Body.LBStatus) >= 1 {

		// Uptime
		response += bol
		uptimeSecs := time.Now().Unix() - (*pb.Body.LBStatus)[0].Started
		uptimeDays := uptimeSecs / (24 * 60 * 60)
		uptimeSecs -= uptimeDays * (24 * 60 * 60)
		uptimeHours := uptimeSecs / (60 * 60)
		uptimeSecs -= uptimeHours * (60 * 60)
		uptimeMins := uptimeSecs / 60
		uptimeSecs -= uptimeMins * 60
		response += fmt.Sprintf("Uptime: %dd:%dh:%dm", uptimeDays, uptimeHours, uptimeMins)
		response += eol

		// Handlers
		response += bol
		response += fmt.Sprintf("Handlers ephemeral:%d continuous:%d notification:%d discovery:%d",
			(*pb.Body.LBStatus)[0].EphemeralHandlers,
			(*pb.Body.LBStatus)[0].ContinuousHandlers,
			(*pb.Body.LBStatus)[0].NotificationHandlers,
			(*pb.Body.LBStatus)[0].DiscoveryHandlers)
		response += eol

	}

	// Generate aggregate info
	if pb.Body.LBStatus != nil && len(*pb.Body.LBStatus) >= 2 {

		// Extract all available stats, and convert them from absolute to
		// per-bucket relative
		stats := absoluteToRelative((*pb.Body.LBStatus)[1:])

		// Display the header
		bucketMins := (*pb.Body.LBStatus)[0].BucketMins
		response += bol
		for i := range stats {
			response += fmt.Sprintf("\t%dm", i*bucketMins)
		}
		response += eol

		// Handler stats
		response += bol
		response += "Handlers: "
		for _, stat := range stats {
			response += fmt.Sprintf("\t%d",
				stat.DiscoveryHandlers+stat.EphemeralHandlers+stat.ContinuousHandlers+stat.NotificationHandlers)
		}
		response += eol

		// Event stats
		response += bol
		response += "Events: "
		for _, stat := range stats {
			response += fmt.Sprintf("\t%d", stat.EventsRouted)
		}
		response += eol

		// Database stats
		response += bol
		response += "Databases r/w: "
		response += eol
		for k, _ := range stats[0].Databases {
			response += bol
			response += k
			for _, stat := range stats {
				response += fmt.Sprintf("\t%d/%d", stat.Databases[k].Reads, stat.Databases[k].Writes)
			}
			response += eol
		}

		// Cache stats
		response += bol
		response += "Cache invalidations/size/hwm: "
		response += eol
		for k, _ := range stats[0].Caches {
			response += bol
			response += k
			for _, stat := range stats {
				response += fmt.Sprintf("\t%d/%d/%d",
					stat.Caches[k].Invalidations, stat.Caches[k].Entries, stat.Caches[k].EntriesHWM)
			}
			response += eol
		}

		// Auth/API stats
		response += bol
		response += "Authorizations: "
		response += eol
		for k, _ := range stats[0].Authorizations {
			response += bol
			response += k
			for _, stat := range stats {
				response += fmt.Sprintf("\t%d", stat.Authorizations[k])
			}
			response += eol
		}

		// Errors stats
		response += bol
		response += "Errors: "
		response += eol
		for k, _ := range stats[0].Errors {
			response += bol
			response += k
			for _, stat := range stats {
				response += fmt.Sprintf("\t%d", stat.Errors[k])
			}
			response += eol
		}

	}

	// Done
	return
}

// Convert N absolute buckets to N-1 relative buckets by subtracting values
// from the NEXT bucket for each bucket.
func absoluteToRelative(stats []AppLBStat) (out []AppLBStat) {

	if len(stats) == 0 {
		stats = append(stats, AppLBStat{})
	}

	if stats[0].Databases == nil {
		stats[0].Databases = make(map[string]AppLBDatabase)
	}
	if stats[0].Caches == nil {
		stats[0].Caches = make(map[string]AppLBCache)
	}
	if stats[0].Authorizations == nil {
		stats[0].Authorizations = make(map[string]int)
	}
	if stats[0].Errors == nil {
		stats[0].Errors = make(map[string]int)
	}

	if len(stats) == 1 {
		return stats
	}

	for i := 0; i < len(stats)-1; i++ {

		stats[i].DiscoveryHandlers -= stats[i+1].DiscoveryHandlers
		stats[i].EphemeralHandlers -= stats[i+1].EphemeralHandlers
		stats[i].ContinuousHandlers -= stats[i+1].ContinuousHandlers
		stats[i].NotificationHandlers -= stats[i+1].NotificationHandlers
		stats[i].EventsEnqueued -= stats[i+1].EventsEnqueued
		stats[i].EventsDequeued -= stats[i+1].EventsDequeued
		stats[i].EventsRouted -= stats[i+1].EventsRouted

		if stats[i+1].Databases == nil {
			stats[i+1].Databases = make(map[string]AppLBDatabase)
		}
		for k, vcur := range stats[i].Databases {
			vprev, present := stats[i+1].Databases[k]
			if present {
				vcur.Reads -= vprev.Reads
				vcur.Writes -= vprev.Writes
				stats[i].Databases[k] = vcur
			}
		}

		if stats[i+1].Caches == nil {
			stats[i+1].Caches = make(map[string]AppLBCache)
		}
		for k, vcur := range stats[i].Caches {
			vprev, present := stats[i+1].Caches[k]
			if present {
				vcur.Invalidations -= vprev.Invalidations
				stats[i].Caches[k] = vcur
			}
		}

		if stats[i+1].Authorizations == nil {
			stats[i+1].Authorizations = make(map[string]int)
		}
		for k, vcur := range stats[i].Authorizations {
			vprev, present := stats[i+1].Authorizations[k]
			if present {
				vcur -= vprev
				stats[i].Authorizations[k] = vcur
			}
		}

		if stats[i+1].Errors == nil {
			stats[i+1].Errors = make(map[string]int)
		}
		for k, vcur := range stats[i].Errors {
			vprev, present := stats[i+1].Errors[k]
			if present {
				vcur -= vprev
				stats[i].Errors[k] = vcur
			}
		}

	}

	return stats[0 : len(stats)-1]

}
