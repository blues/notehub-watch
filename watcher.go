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
		server = "api.ray.blues.tools"
	}

	// Get the list of handlers on the server
	handlerNodeIDs, handlerAddrs, errstr = watcherGetHandlers(server)
	if errstr != "" {
		return errstr
	}

	// Show the handlers
	for i, addr := range handlerAddrs {
		response += fmt.Sprintf("NODE %s\n", handlerNodeIDs[i])
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
		addr := fmt.Sprintf("http://%s%s", server, h.PublicPath)
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

// Get general load stats for a handler
func watcherGetHandlerStats(addr string) (response string, errstr string) {
	indent := "  "
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
		uptimeSecs := time.Now().Unix() - (*pb.Body.LBStatus)[0].Started
		uptimeDays := uptimeSecs / (24 * 60 * 60)
		uptimeSecs -= uptimeDays * (24 * 60 * 60)
		uptimeHours := uptimeSecs / (60 * 60)
		uptimeSecs -= uptimeHours * (60 * 60)
		uptimeMins := uptimeSecs / 60
		uptimeSecs -= uptimeMins * 60
		response += indent + "RIGHT NOW" + eol
		response += indent + indent + "Uptime "
		response += fmt.Sprintf("%dd:%dh:%dm", uptimeDays, uptimeHours, uptimeMins)
		response += eol

		// Handlers
		response += indent + indent + "Handlers "
		response += fmt.Sprintf("continuous:%d ", (*pb.Body.LBStatus)[0].ContinuousHandlers)
		response += fmt.Sprintf("notification:%d ", (*pb.Body.LBStatus)[0].NotificationHandlers)
		response += fmt.Sprintf("ephemeral:%d ", (*pb.Body.LBStatus)[0].EphemeralHandlers)
		response += fmt.Sprintf("discovery:%d ", (*pb.Body.LBStatus)[0].DiscoveryHandlers)
		response += eol

	}

	// Generate aggregate info
	if pb.Body.LBStatus != nil && len(*pb.Body.LBStatus) >= 2 {

		// Extract all available stats, and convert them from absolute to
		// per-bucket relative
		stats := absoluteToRelative((*pb.Body.LBStatus)[1:])

		// Display the header
		bucketMins := (*pb.Body.LBStatus)[0].BucketMins
		response += indent + "PAST" + eol
		response += indent + indent + indent
		for i := range stats {
			response += fmt.Sprintf("%dm\t", int64(i+1)*bucketMins)
		}
		response += eol

		// Handler stats
		response += indent + indent + "Handlers" + eol
		response += indent + indent + indent
		for _, stat := range stats {
			response += fmt.Sprintf("%d\t",
				stat.DiscoveryHandlers+stat.EphemeralHandlers+stat.ContinuousHandlers+stat.NotificationHandlers)
		}
		response += eol

		// Event stats
		response += indent + indent + "Events Routed" + eol
		response += indent + indent + indent
		for _, stat := range stats {
			response += fmt.Sprintf("%d\t", stat.EventsRouted)
		}
		response += eol

		// Database stats
		response += indent + indent + "Databases (r+w rMs/wMs)" + eol
		for k, _ := range stats[0].Databases {
			response += indent + indent + indent + k + eol
			response += indent + indent + indent + indent
			for _, stat := range stats {
				response += fmt.Sprintf("%d+%d\t", stat.Databases[k].Reads, stat.Databases[k].Writes)
			}
			response += eol
			for _, stat := range stats {
				response += fmt.Sprintf("%d/%d\t", stat.Databases[k].ReadMs, stat.Databases[k].WriteMs)
			}
			response += eol
		}

		// Cache stats
		response += indent + indent + "Caches (invalidations/size)" + eol
		for k, _ := range stats[0].Caches {
			response += indent + indent + indent + k + eol
			response += indent + indent + indent + indent
			for _, stat := range stats {
				response += fmt.Sprintf("%d/%d\t", stat.Caches[k].Invalidations, stat.Caches[k].Entries)
			}
			response += eol
		}

		// Auth/API stats
		if len(stats[0].Authorizations) > 0 {
			response += indent + indent + "Authorizations (API)" + eol
			for k, _ := range stats[0].Authorizations {
				response += indent + indent + indent + k + eol
				response += indent + indent + indent + indent
				for _, stat := range stats {
					response += fmt.Sprintf("%d\t", stat.Authorizations[k])
				}
				response += eol
			}
		}

		// Fatals stats
		if len(stats[0].Fatals) > 0 {
			response += indent + indent + "Fatals (FATAL)" + eol
			for k, _ := range stats[0].Fatals {
				response += indent + indent + indent + k + eol
				response += indent + indent + indent + indent
				for _, stat := range stats {
					response += fmt.Sprintf("%d\t", stat.Fatals[k])
				}
				response += eol
			}
		}

	}

	// Done
	return
}

// Convert N absolute buckets to N-1 relative buckets by subtracting values
// from the next bucket from the value in each bucket.
func absoluteToRelative(stats []AppLBStat) (out []AppLBStat) {

	// Do prep work to make the code below flow more naturally without
	// getting access violations because of uninitialized maps
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
		stats[0].Authorizations = make(map[string]int64)
	}
	if stats[0].Fatals == nil {
		stats[0].Fatals = make(map[string]int64)
	}

	// Special-case returning a single stat just after server reboot
	if len(stats) == 1 {
		for k, vcur := range stats[0].Databases {
			if vcur.Reads > 0 {
				vcur.ReadMs = vcur.ReadMs / vcur.Reads
			}
			if vcur.Writes > 0 {
				vcur.WriteMs = vcur.WriteMs / vcur.Writes
			}
			stats[0].Databases[k] = vcur
		}
		return stats
	}

	// Iterate over all stats, converting from boot-absolute numbers
	// to numbers that are bucket-scoped relative to the prior bucket
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
				vcur.ReadMs -= vprev.ReadMs
				if vcur.Reads > 0 {
					vcur.ReadMs = vcur.ReadMs / vcur.Reads
				}
				vcur.Writes -= vprev.Writes
				vcur.WriteMs -= vprev.WriteMs
				if vcur.Writes > 0 {
					vcur.WriteMs = vcur.WriteMs / vcur.Writes
				}
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
			stats[i+1].Authorizations = make(map[string]int64)
		}
		for k, vcur := range stats[i].Authorizations {
			vprev, present := stats[i+1].Authorizations[k]
			if present {
				vcur -= vprev
				stats[i].Authorizations[k] = vcur
			}
		}

		if stats[i+1].Fatals == nil {
			stats[i+1].Fatals = make(map[string]int64)
		}
		for k, vcur := range stats[i].Fatals {
			vprev, present := stats[i+1].Fatals[k]
			if present {
				vcur -= vprev
				stats[i].Fatals[k] = vcur
			}
		}

	}

	return stats[0 : len(stats)-1]

}
