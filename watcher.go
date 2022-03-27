// Copyright 2022 Blues Inc.  All rights reserved.
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

// Trace
const watcherTrace = true

// Synchronous vs asynchronous sheet request handling, because we're getting "operation timeout"
const asyncSheetRequest = true

// Current "live" info
type serviceSummary struct {
	Started              int64
	ContinuousHandlers   int64
	NotificationHandlers int64
	EphemeralHandlers    int64
	DiscoveryHandlers    int64
	ServiceInstanceIDs   []string
	ServiceInstanceAddrs []string
}

// Watcher show command
func watcherShow(hostname string, showWhat string) (result string) {

	// Map name to address
	hostaddr := ""
	validHosts := ""
	for _, v := range Config.MonitoredHosts {
		if !v.Disabled {
			if hostname == v.Name {
				hostaddr = v.Addr
				break
			}
			if validHosts != "" {
				validHosts += " or "
			}
			validHosts += "'" + v.Name + "'"
		}
	}
	if hostaddr == "" {
		return "" +
			"/notehub <host>\n" +
			"/notehub <host> show <what>\n" +
			"<host> is " + validHosts + "\n" +
			"<what> is goroutines, heap, handlers\n"
	}

	// Show the host
	return watcherShowHost(hostname, hostaddr, showWhat)

}

// An async version of the sheet host stats procedure
func asyncSheetGetHostStats(hostname string, hostaddr string) {
	time.Sleep(1 * time.Second)
	slackSendMessage(sheetGetHostStats(hostname, hostaddr))
}

// Show something about the host
func watcherShowHost(hostname string, hostaddr string, showWhat string) (response string) {

	// If showing nothing, done
	if showWhat == "" {
		if asyncSheetRequest {
			go asyncSheetGetHostStats(hostname, hostaddr)
			return "one moment, please"
		}
		return sheetGetHostStats(hostname, hostaddr)
	}

	// Get the list of handlers on the host
	serviceInstanceIDs, serviceInstanceAddrs, err := watcherGetServiceInstances(hostaddr)
	if err != nil {
		return err.Error()
	}

	// Show the handlers
	for i, addr := range serviceInstanceAddrs {
		response += "\n"
		response += fmt.Sprintf("*NODE %s*\n", serviceInstanceIDs[i])
		r, errstr := watcherShowServiceInstance(addr, serviceInstanceIDs[i], showWhat)
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
func watcherGetServiceInstances(hostaddr string) (serviceInstanceIDs []string, serviceInstanceAddrs []string, err error) {

	url := "https://" + hostaddr + "/ping?show=\"handlers\""
	req, err2 := http.NewRequest("GET", url, nil)
	if err2 != nil {
		err = err2
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}
	rsp, err2 := httpclient.Do(req)
	if err2 != nil {
		err = err2
		return
	}
	defer rsp.Body.Close()

	var rspJSON []byte
	rspJSON, err = io.ReadAll(rsp.Body)
	if err != nil {
		return
	}

	var pb PingBody
	err = json.Unmarshal(rspJSON, &pb)
	if err != nil {
		return
	}

	if pb.Body.AppHandlers == nil {
		err = fmt.Errorf("no handlers in " + string(rspJSON))
		return
	}
	for _, h := range *pb.Body.AppHandlers {
		// Create the SIID out of the NodeID combined with the primary service.  This technique is mimicked
		// within the actual http-ping.go handling in notehub, and is required for unique addressing of
		// a service instance simply because on Local Dev we have a single NodeID that hosts all of the
		// different services that collect stats within their own process address spaces.
		serviceInstanceIDs = append(serviceInstanceIDs, h.NodeID+":"+h.PrimaryService)
		addr := fmt.Sprintf("http://%s", hostaddr)
		serviceInstanceAddrs = append(serviceInstanceAddrs, addr)
	}

	return

}

// Retrieve the ping info from a handler
func getServiceInstanceInfo(addr string, siid string, showWhat string) (pb PingBody, err error) {

	// Prefix in case it's missing
	if !strings.Contains(addr, "://") {
		addr = "https://" + addr
	}

	// Get the data
	url := fmt.Sprintf("%s/ping?show=\"%s\"", addr, showWhat)
	if siid != "" {
		url = fmt.Sprintf("%s/ping?node=\"%s\"&show=\"%s\"", addr, siid, showWhat)
	}

	req, err2 := http.NewRequest("GET", url, nil)
	if err2 != nil {
		err = err2
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}
	rsp, err2 := httpclient.Do(req)
	if err2 != nil {
		err = err2
		return
	}
	defer rsp.Body.Close()

	// Read the body
	rspJSON, err2 := io.ReadAll(rsp.Body)
	if err2 != nil {
		err = err2
		return
	}

	// Unmarshal it
	err = json.Unmarshal(rspJSON, &pb)
	if err != nil {
		return
	}

	// Done
	return

}

// Show something about a service instance
func watcherShowServiceInstance(addr string, siid string, showWhat string) (response string, errstr string) {

	// Get the info from the service instance
	pb, err := getServiceInstanceInfo(addr, siid, showWhat)
	if err != nil {
		errstr = err.Error()
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

// Convert N absolute buckets to N-1 relative buckets by subtracting values
// from the next bucket from the value in each bucket.
func ConvertStatsFromAbsoluteToRelative(startTime int64, bucketMins int64, stats []AppLBStat) (out []AppLBStat) {

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

		stats[i].Started = startTime
		stats[i].BucketMins = bucketMins

		stats[i].OSDiskRead -= stats[i+1].OSDiskRead
		stats[i].OSDiskWrite -= stats[i+1].OSDiskWrite

		stats[i].OSNetReceived -= stats[i+1].OSNetReceived
		stats[i].OSNetSent -= stats[i+1].OSNetSent

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

// Retrieve a sample of data from the specified host, returning a vector of available stats indexed by SIID
func watcherGetStats(hostaddr string) (ss serviceSummary, stats map[string][]AppLBStat, err error) {

	if watcherTrace {
		fmt.Printf("watcherGetStats: fetching stats for %s\n", hostaddr)
		defer fmt.Printf("watcherGetStats: completed\n")
	}

	// Instantiate the stats map
	stats = map[string][]AppLBStat{}

	// Get the list of service instances on the host
	ss.ServiceInstanceIDs, ss.ServiceInstanceAddrs, err = watcherGetServiceInstances(hostaddr)
	if err != nil {
		return
	}

	// Iterate over each service instance, gathering its stats
	for i, siid := range ss.ServiceInstanceIDs {

		// Get the info
		var pb PingBody
		pb, err = getServiceInstanceInfo(ss.ServiceInstanceAddrs[i], siid, "lb")
		if err != nil {
			return
		}

		// Update service summary
		if len(*pb.Body.LBStatus) > 0 {
			ss.Started = (*pb.Body.LBStatus)[0].Started
			ss.ContinuousHandlers += (*pb.Body.LBStatus)[0].ContinuousHandlersActivated -
				(*pb.Body.LBStatus)[0].ContinuousHandlersDeactivated
			ss.NotificationHandlers += (*pb.Body.LBStatus)[0].NotificationHandlersActivated -
				(*pb.Body.LBStatus)[0].NotificationHandlersDeactivated
			ss.EphemeralHandlers += (*pb.Body.LBStatus)[0].EphemeralHandlersActivated -
				(*pb.Body.LBStatus)[0].EphemeralHandlersDeactivated
			ss.DiscoveryHandlers += (*pb.Body.LBStatus)[0].DiscoveryHandlersActivated -
				(*pb.Body.LBStatus)[0].DiscoveryHandlersDeactivated
		}

		// If the server hasn't been up long enough to have stats.  Note that [0] is the
		// current stats, and we need at least two more to compute relative stats.
		if len(*pb.Body.LBStatus) < 3 {
			err = fmt.Errorf("server hasn't been up long enough to have useful stats")
			return
		}

		// Extract all available stats, and convert them from absolute to per-bucket relative.
		stats[siid] = ConvertStatsFromAbsoluteToRelative(
			(*pb.Body.LBStatus)[0].Started,
			(*pb.Body.LBStatus)[0].BucketMins,
			(*pb.Body.LBStatus)[1:])

	}

	// Done
	return

}
