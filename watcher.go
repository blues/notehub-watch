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
func watcherShow(server string, showWhat string) (result string) {

	if server == "" {
		return "" +
			"/notehub prod show <what>\n" +
			"/notehub staging show <what>\n" +
			"/notehub <yourserver> show <what>\n" +
			""
	}

	targetServer := server

	// Production
	if server == "p" || server == "prod" || server == "production" {
		targetServer = "notefile.net"
	}

	// Staging
	if server == "s" || server == "staging" {
		targetServer = "staging.blues.tools"
	}

	// Localdev
	if !strings.Contains(server, ".") {
		targetServer = server + ".blues.tools"
	}

	// We must target the API service for this server
	if !strings.HasPrefix(targetServer, "api.") {
		targetServer = "api." + targetServer
	}

	return watcherShowServer(targetServer, showWhat)

}

// Show something about the server
func watcherShowServer(server string, showWhat string) (response string) {
	var errstr string
	var handlerNodeIDs, handlerAddrs []string

	// Get the list of handlers on the server
	handlerNodeIDs, handlerAddrs, errstr = watcherGetHandlers(server)
	if errstr != "" {
		return errstr
	}

	// Show the handlers
	for i, addr := range handlerAddrs {
		response += "\n"
		response += fmt.Sprintf("*NODE %s*\n", handlerNodeIDs[i])
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
		handlerNodeIDs = append(handlerNodeIDs, h.NodeID+":"+h.PrimaryService)
		addr := fmt.Sprintf("http://%s", server)
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

	// If showing nothing, done
	if showWhat == "" {
		return watcherGetHandlerStats(addr, nodeID)
	}

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

// Return a time header
func timeHeader(bucketMins int, buckets int) (response string) {
	response += fmt.Sprintf("%7s", "")
	for i := 0; i < buckets; i++ {
		response += fmt.Sprintf("%6dm", (i+1)*bucketMins)
	}
	response += "\n"
	return
}

// Get general load stats for a handler
func watcherGetHandlerStats(addr string, nodeID string) (response string, errstr string) {
	eol := "\n"
	code := "```"
	bold := "*"
	italic := "_"

	// Get the info from the handler
	var pb PingBody
	pb, errstr = getHandlerInfo(addr, nodeID, "lb")
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
		response += bold + "        Uptime:" + bold + " "
		response += fmt.Sprintf("%dd:%dh:%dm", uptimeDays, uptimeHours, uptimeMins)
		response += eol

		// Handlers
		response += bold + "        Handlers:" + bold + " "
		continuousActive := (*pb.Body.LBStatus)[0].ContinuousHandlersActivated -
			(*pb.Body.LBStatus)[0].ContinuousHandlersDeactivated
		notificationActive := (*pb.Body.LBStatus)[0].NotificationHandlersActivated -
			(*pb.Body.LBStatus)[0].NotificationHandlersDeactivated
		ephemeralActive := (*pb.Body.LBStatus)[0].EphemeralHandlersActivated -
			(*pb.Body.LBStatus)[0].EphemeralHandlersDeactivated
		discoveryActive := (*pb.Body.LBStatus)[0].DiscoveryHandlersActivated -
			(*pb.Body.LBStatus)[0].DiscoveryHandlersDeactivated
		totalActive := continuousActive + notificationActive + ephemeralActive + discoveryActive
		response += fmt.Sprintf("%d", totalActive)
		response += fmt.Sprintf(" (%d continuous", continuousActive)
		response += fmt.Sprintf(", %d notification", notificationActive)
		response += fmt.Sprintf(", %d ephemeral", ephemeralActive)
		response += fmt.Sprintf(", %d discovery)", discoveryActive)
		response += eol

	}

	// Generate aggregate info
	if pb.Body.LBStatus != nil && len(*pb.Body.LBStatus) >= 2 {

		// Extract all available stats, and convert them from absolute to
		// per-bucket relative
		stats := absoluteToRelative((*pb.Body.LBStatus)[1:])

		// Limit the number of buckets because of slack UI block width
		buckets := len(stats)
		if slackUsingBlocksForResponses() && buckets > 10 {
			buckets = 10
		}
		bucketMins := int((*pb.Body.LBStatus)[0].BucketMins)

		// Handler stats
		response += bold + italic + "Handlers" + italic + bold + eol
		response += code
		response += timeHeader(bucketMins, buckets)
		response += fmt.Sprintf("%7s", "contin")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.ContinuousHandlersActivated)
		}
		response += eol
		response += fmt.Sprintf("%7s", "notif")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.NotificationHandlersActivated)
		}
		response += eol
		response += fmt.Sprintf("%7s", "ephem")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.EphemeralHandlersActivated)
		}
		response += eol
		response += fmt.Sprintf("%7s", "disco")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.DiscoveryHandlersActivated)
		}
		response += code
		response += eol

		// Event stats
		response += bold + italic + "Events" + italic + bold + eol
		response += code
		response += timeHeader(bucketMins, buckets)
		response += fmt.Sprintf("%7s", "routed")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.EventsRouted)
		}
		response += code
		response += eol

		// Database stats
		response += bold + italic + "Databases" + italic + bold + eol
		for k := range stats[0].Databases {
			response += k + eol
			response += code
			response += timeHeader(bucketMins, buckets)
			response += fmt.Sprintf("%7s", "reads")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Databases[k].Reads)
			}
			response += eol
			response += fmt.Sprintf("%7s", "writes")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Databases[k].Writes)
			}
			response += eol
			response += fmt.Sprintf("%7s", "readMs")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Databases[k].ReadMs)
			}
			response += eol
			response += fmt.Sprintf("%7s", "writeMs")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Databases[k].WriteMs)
			}
			response += code
			response += eol
		}

		// Cache stats
		response += bold + italic + "Caches" + italic + bold + eol
		for k := range stats[0].Caches {
			response += k + " cache " + eol
			response += code
			response += timeHeader(bucketMins, buckets)
			response += fmt.Sprintf("%7s", "refresh")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Caches[k].Invalidations)
			}
			response += eol
			response += fmt.Sprintf("%7s", "entries")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				response += fmt.Sprintf("%7d", stat.Caches[k].Entries)
			}
			response += code
			response += eol
		}

		// Auth/API stats
		if len(stats[0].API) > 0 {
			response += bold + italic + "API" + italic + bold + eol
			for k := range stats[0].API {
				response += k + eol
				response += code
				response += timeHeader(bucketMins, buckets)
				response += fmt.Sprintf("%7s", "")
				for i, stat := range stats {
					if i >= buckets {
						break
					}
					response += fmt.Sprintf("%7d", stat.API[k])
				}
				response += code
				response += eol
			}
		}

		// Fatals stats
		if len(stats[0].Fatals) > 0 {
			response += bold + italic + "Fatals" + italic + bold + eol
			for k := range stats[0].Fatals {
				response += k + eol
				response += code
				response += timeHeader(bucketMins, buckets)
				response += fmt.Sprintf("%7s", "")
				for i, stat := range stats {
					if i >= buckets {
						break
					}
					response += fmt.Sprintf("%7d", stat.Fatals[k])
				}
				response += code
				response += eol
			}
		}

		// Memory stats
		response += bold + italic + "Memory (MiB)" + italic + bold + eol
		response += code
		response += timeHeader(bucketMins, buckets)
		response += fmt.Sprintf("%7s", "free")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.OSMemFree/(1024*1024))
		}
		response += eol
		response += fmt.Sprintf("%7s", "total")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			response += fmt.Sprintf("%7d", stat.OSMemTotal/(1024*1024))
		}
		response += code
		response += eol

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
	if stats[0].API == nil {
		stats[0].API = make(map[string]int64)
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

		stats[i].DiscoveryHandlersActivated -= stats[i+1].DiscoveryHandlersActivated
		stats[i].DiscoveryHandlersDeactivated = 0

		stats[i].ContinuousHandlersActivated -= stats[i+1].ContinuousHandlersActivated
		stats[i].ContinuousHandlersDeactivated = 0

		stats[i].NotificationHandlersActivated -= stats[i+1].NotificationHandlersActivated
		stats[i].NotificationHandlersDeactivated = 0

		stats[i].EphemeralHandlersActivated -= stats[i+1].EphemeralHandlersActivated
		stats[i].EphemeralHandlersDeactivated = 0

		stats[i].EventsEnqueued -= stats[i+1].EventsEnqueued
		stats[i].EventsDequeued = 0

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

		if stats[i+1].API == nil {
			stats[i+1].API = make(map[string]int64)
		}
		for k, vcur := range stats[i].API {
			vprev, present := stats[i+1].API[k]
			if present {
				vcur -= vprev
				stats[i].API[k] = vcur
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
