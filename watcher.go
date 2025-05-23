// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Trace
const watcherTrace = true
const watcherHttpTrace = true

// Synchronous vs asynchronous sheet request handling, because we're getting "operation timeout"
const asyncSheetRequest = true

// Current "live" info
type serviceSummary struct {
	ServiceVersion       string
	BucketSecs           int64
	ContinuousHandlers   int64
	NotificationHandlers int64
	EphemeralHandlers    int64
	DiscoveryHandlers    int64
	ServiceInstanceIDs   []string
	ServiceInstanceAddrs []string
}

// Throughput stats
var lastEventsDequeued map[string]int64 = map[string]int64{}
var lastEventsDequeuedTime map[string]time.Time = map[string]time.Time{}
var lastEventsCount map[string]int64 = map[string]int64{}
var lastEventsThroughput map[string]float64 = map[string]float64{}
var lastEventsThroughputSecs map[string]float64 = map[string]float64{}

// Service instances the last time we looked
var serviceLock sync.Mutex
var lastServiceVersions map[string]string
var lastServiceHandlers map[string][]AppHandler

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
	_, _, serviceInstanceIDs, serviceInstanceAddrs, _, err := watcherGetServiceInstances(hostname, hostaddr)
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

// This is the central method to get the list of handlers, diff'ing them against the prior versions returned, and
// sending a message to the service if we've detected that the list has changed.
func watcherGetServiceInstances(hostname string, hostaddr string) (serviceVersionChanged bool, serviceVersion string, serviceInstanceIDs []string, serviceInstanceAddrs []string, handlers map[string]AppHandler, err error) {

	// Only one task in here at a time
	serviceLock.Lock()

	// Initialize
	refreshCache := false
	if lastServiceVersions == nil {
		lastServiceVersions = map[string]string{}
		refreshCache = true
	}
	if lastServiceHandlers == nil {
		lastServiceHandlers = map[string][]AppHandler{}
		refreshCache = true
	}

	// Get the latest service instances, and exit if error
	serviceVersion, serviceInstanceIDs, serviceInstanceAddrs, handlers, err = getServiceInstances(hostaddr)

	// Substitute very common errors
	if err != nil {
		if strings.Contains(err.Error(), "unexpected end of JSON input") {
			err = fmt.Errorf("server not responding")
		}
	}
	if err != nil {
		err = fmt.Errorf("%s: error pinging host: %s", hostname, err)
	}

	// Check to see if the service version is the same
	if err == nil && lastServiceVersions[hostname] != serviceVersion {
		if lastServiceVersions[hostname] != "" {
			err = fmt.Errorf("@channel: %s restarted from %s to %s", hostname, lastServiceVersions[hostname], serviceVersion)
			serviceVersionChanged = true
		}
		refreshCache = true
	}

	// Check to see if the handlers are the same
	lastHandlers, exists := lastServiceHandlers[hostname]
	if !exists {
		refreshCache = true
	} else if err == nil {

		// Generate a list of differences
		addedHandlers := map[string]AppHandler{}
		sameHandlers := map[string]AppHandler{}
		removedHandlers := map[string]AppHandler{}
		for _, v := range lastHandlers {
			_, exists := handlers[v.NodeID]
			if !exists {
				removedHandlers[v.NodeID] = v
			} else {
				sameHandlers[v.NodeID] = v
			}
		}
		for k, v := range handlers {
			_, exists := sameHandlers[k]
			if !exists {
				addedHandlers[k] = v
			}
		}
		if len(addedHandlers) > 0 || len(removedHandlers) > 0 {
			s := "@channel: " + hostname + " handlers changed:\n"
			if len(addedHandlers) > 0 {
				s += "  BORN:\n"
				for k := range addedHandlers {
					s += "    " + k + "\n"
				}
			}
			if len(removedHandlers) > 0 {
				s += "  DIED:\n"
				for k := range removedHandlers {
					s += "    " + k + "\n"
				}
			}
			err = fmt.Errorf("%s", s)
			refreshCache = true
		}
	}

	// If an error, post it
	if err != nil {
		slackSendMessage(err.Error())
	}

	// If we need to re-cache service info, do it.  If this was successful, it means that no error actually occurred
	if refreshCache {
		err = nil
		lastServiceVersions[hostname] = serviceVersion
		newHandlers := []AppHandler{}
		for _, v := range handlers {
			newHandlers = append(newHandlers, v)
		}
		lastServiceHandlers[hostname] = newHandlers
	}

	// Done
	serviceLock.Unlock()
	return

}

// Get the list of handlers
func getServiceInstances(hostaddr string) (serviceVersion string, serviceInstanceIDs []string, serviceInstanceAddrs []string, handlers map[string]AppHandler, err error) {

	url := "https://" + hostaddr + "/ping?show=\"handlers\""
	req, err2 := http.NewRequest("GET", url, nil)
	if err2 != nil {
		err = err2
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(30),
	}
	if watcherHttpTrace {
		fmt.Printf("getServiceInstances: %s\n", url)
	}
	rsp, err2 := httpclient.Do(req)
	if watcherHttpTrace {
		if err2 != nil {
			fmt.Printf("getServiceInstances: %s\n", err2)
		} else {
			fmt.Printf("getServiceInstances: OK\n")
		}
	}
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
		err = fmt.Errorf("%s (%s)", err, string(rspJSON))
		return
	}
	if pb.Body.ServiceVersion == "" && pb.Body.LegacyServiceVersion != 0 {
		pb.Body.ServiceVersion = time.Unix(pb.Body.LegacyServiceVersion, 0).Format("20060102-150405")
	}

	if pb.Body.AppHandlers == nil {
		err = fmt.Errorf("no handlers in " + string(rspJSON))
		return
	}

	serviceVersion = pb.Body.ServiceVersion

	handlers = map[string]AppHandler{}
	for _, h := range *pb.Body.AppHandlers {
		// Create the SIID out of the NodeID combined with the primary service.  This technique is mimicked
		// within the actual http-ping.go handling in notehub, and is required for unique addressing of
		// a service instance simply because on Local Dev we have a single NodeID that hosts all of the
		// different services that collect stats within their own process address spaces.  Note that
		// we replace the NodeID in the structure so that the caller can make that assumption.
		h.NodeID = h.NodeID + ":" + h.PrimaryService
		serviceInstanceIDs = append(serviceInstanceIDs, h.NodeID)
		addr := fmt.Sprintf("http://%s", hostaddr)
		serviceInstanceAddrs = append(serviceInstanceAddrs, addr)
		handlers[h.NodeID] = h
	}

	// Always return them in a deterministic order to make it easier to look at the spreadsheet
	sort.Strings(serviceInstanceIDs)

	return

}

// Retrieve the ping info from a handler
func getServiceInstanceInfo(addr string, siid string, requestWhat string, showWhat string) (pb PingBody, err error) {

	// Prefix in case it's missing
	if !strings.Contains(addr, "://") {
		addr = "https://" + addr
	}

	// Get the data
	Url := ""
	if siid != "" {
		Url = fmt.Sprintf("%s/ping?node=\"%s\"&", addr, siid)
	} else {
		Url = fmt.Sprintf("%s/ping?", addr)
	}
	if showWhat != "" && requestWhat == "" {
		Url += fmt.Sprintf("show=\"%s\"", url.QueryEscape(showWhat))
	} else if showWhat == "" && requestWhat != "" {
		Url += fmt.Sprintf("req=\"%s\"", url.QueryEscape(requestWhat))
	} else {
		Url += fmt.Sprintf("show=\"%s\"&req=\"%s\"", url.QueryEscape(showWhat), url.QueryEscape(requestWhat))
	}

	req, err2 := http.NewRequest("GET", Url, nil)
	if err2 != nil {
		err = err2
		return
	}
	httpclient := &http.Client{
		Timeout: time.Second * time.Duration(60),
	}
	if watcherHttpTrace {
		fmt.Printf("getServiceInstanceInfo: %s\n", Url)
	}
	rsp, err2 := httpclient.Do(req)
	if err2 != nil {
		if watcherHttpTrace {
			if err2 != nil {
				fmt.Printf("getServiceInstanceInfo: %s\n", err2)
			} else {
				fmt.Printf("getServiceInstanceInfo: OK\n")
			}
		}
		err = fmt.Errorf("%s: %s", Url, err2)
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
		err = fmt.Errorf("%s (%s)", err, string(rspJSON))
		return
	}
	if pb.Body.ServiceVersion == "" && pb.Body.LegacyServiceVersion != 0 {
		pb.Body.ServiceVersion = time.Unix(pb.Body.LegacyServiceVersion, 0).Format("20060102-150405")
	}

	// Done
	return

}

// Show something about a service instance
func watcherShowServiceInstance(addr string, siid string, showWhat string) (response string, errstr string) {

	// Get the info from the service instance
	pb, err := getServiceInstanceInfo(addr, siid, "", showWhat)
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
func ConvertStatsFromAbsoluteToRelative(stats []StatsStat, bucketSecs int64) (out []StatsStat) {

	// Do prep work to make the code below flow more naturally without
	// getting access violations because of uninitialized maps
	if len(stats) == 0 {
		stats = append(stats, StatsStat{})
	}
	if stats[0].Databases == nil {
		stats[0].Databases = make(map[string]StatsDatabase)
	}
	if stats[0].Caches == nil {
		stats[0].Caches = make(map[string]StatsCache)
	}
	if stats[0].API == nil {
		stats[0].API = make(map[string]int64)
	}
	if stats[0].Fatals == nil {
		stats[0].Fatals = make(map[string]int64)
	}
	stats[0].SnapshotTaken = (stats[0].SnapshotTaken / bucketSecs) * bucketSecs

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

		stats[i].SnapshotTaken = (stats[i].SnapshotTaken / bucketSecs) * bucketSecs
		stats[i].BucketMins = 0

		stats[i].OSDiskRead -= stats[i+1].OSDiskRead
		stats[i].OSDiskWrite -= stats[i+1].OSDiskWrite
		stats[i].HttpConnTotal -= stats[i+1].HttpConnTotal
		stats[i].HttpConnReused -= stats[i+1].HttpConnReused

		// Special handling for these two stats, which seem odd because
		// occasionally the OS will return numbers lower than the previous ones
		if stats[i+1].OSNetReceived > stats[i].OSNetReceived {
			stats[i].OSNetReceived = stats[i+1].OSNetReceived
		}
		stats[i].OSNetReceived -= stats[i+1].OSNetReceived
		if stats[i+1].OSNetSent > stats[i].OSNetSent {
			stats[i].OSNetSent = stats[i+1].OSNetSent
		}
		stats[i].OSNetSent -= stats[i+1].OSNetSent

		// For Handlers, Activated is the 'new activations' whereas Deactivated is 'currently active' count
		stats[i].DiscoveryHandlersDeactivated = stats[i].DiscoveryHandlersActivated - stats[i].DiscoveryHandlersDeactivated
		stats[i].DiscoveryHandlersActivated -= stats[i+1].DiscoveryHandlersActivated
		stats[i].ContinuousHandlersDeactivated = stats[i].ContinuousHandlersActivated - stats[i].ContinuousHandlersDeactivated
		stats[i].ContinuousHandlersActivated -= stats[i+1].ContinuousHandlersActivated
		stats[i].NotificationHandlersDeactivated = stats[i].NotificationHandlersActivated - stats[i].NotificationHandlersDeactivated
		stats[i].NotificationHandlersActivated -= stats[i+1].NotificationHandlersActivated
		stats[i].EphemeralHandlersDeactivated = stats[i].EphemeralHandlersActivated - stats[i].EphemeralHandlersDeactivated
		stats[i].EphemeralHandlersActivated -= stats[i+1].EphemeralHandlersActivated

		stats[i].EventsEnqueued -= stats[i+1].EventsEnqueued
		stats[i].EventsDequeued = 0

		stats[i].EventsRouted -= stats[i+1].EventsRouted

		if stats[i+1].Databases == nil {
			stats[i+1].Databases = make(map[string]StatsDatabase)
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
			stats[i+1].Caches = make(map[string]StatsCache)
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
func watcherGetStats(hostname string, hostaddr string, warnWhenPendingEventsPerHandlerExceed int) (serviceVersionChanged bool, ss serviceSummary, handlers map[string]AppHandler, stats map[string][]StatsStat, err error) {

	if watcherTrace {
		fmt.Printf("watcherGetStats: fetching stats for %s\n", hostaddr)
		defer fmt.Printf("watcherGetStats: completed\n")
	}

	// Instantiate the stats map
	stats = map[string][]StatsStat{}

	// Get the list of service instances on the host
	serviceVersionChanged, ss.ServiceVersion, ss.ServiceInstanceIDs, ss.ServiceInstanceAddrs, handlers, err = watcherGetServiceInstances(hostname, hostaddr)
	if err != nil {
		return
	}

	// Iterate over each service instance, gathering its stats
	for i, siid := range ss.ServiceInstanceIDs {

		// Get the info
		var pb PingBody
		pb, err = getServiceInstanceInfo(ss.ServiceInstanceAddrs[i], siid, "", "lb")
		if err != nil {
			return
		}

		// Update the handler with info only contained in the ping body
		h := handlers[siid]
		if pb.Body.LegacyServiceVersion != 0 {
			h.NodeStarted = pb.Body.LegacyServiceVersion
		}
		if pb.Body.NodeStarted != "" {
			started, _ := time.Parse("2006-01-02T15:04:05Z", pb.Body.NodeStarted)
			h.NodeStarted = started.Unix()
		}

		// Sanity check for format of stats
		if pb.Body.LBStatus == nil || len(*pb.Body.LBStatus) == 0 {
			// No 'live' stats - should never happen
			continue
		}
		sistats := *pb.Body.LBStatus
		if pb.Body.ServiceVersion != ss.ServiceVersion {
			err = fmt.Errorf("%s: node service version is incorrect: %s != %s", siid, pb.Body.ServiceVersion, ss.ServiceVersion)
		}

		// Update service summary
		ss.BucketSecs = sistats[0].BucketMins * 60
		ss.ContinuousHandlers += sistats[0].ContinuousHandlersActivated - sistats[0].ContinuousHandlersDeactivated
		ss.NotificationHandlers += sistats[0].NotificationHandlersActivated - sistats[0].NotificationHandlersDeactivated
		ss.EphemeralHandlers += sistats[0].EphemeralHandlersActivated - sistats[0].EphemeralHandlersDeactivated
		ss.DiscoveryHandlers += sistats[0].DiscoveryHandlersActivated - sistats[0].DiscoveryHandlersDeactivated

		// If the server hasn't been up long enough to have stats.  Note that [0] is the
		// current stats, and we need at least two more to compute relative stats.
		if len(sistats) < 3 {
			fmt.Printf("node %s hasn't been up long enough to have useful stats\n", siid)
			continue
		}

		// Keep per-handler throughput stats
		throughputUpdate(h.NodeName, sistats)

		// Warning
		if warnWhenPendingEventsPerHandlerExceed > 0 {
			eventsPending := sistats[0].EventsEnqueued - sistats[0].EventsDequeued
			if eventsPending > int64(warnWhenPendingEventsPerHandlerExceed) {
				message := fmt.Sprintf("%s: %s %d pending events (%d routed [%.1f/min] in the last %d mins)\n", hostname, h.NodeName, eventsPending, lastEventsCount[h.NodeName], lastEventsThroughput[h.NodeName]*60, int(lastEventsThroughputSecs[h.NodeName]/60))
				slackSendMessage(message)
			}
		}

		// Extract all available stats, and convert them from absolute to per-bucket relative.
		stats[siid] = ConvertStatsFromAbsoluteToRelative(sistats[1:], ss.BucketSecs)

		// Now that we have valid stats, include the handler
		handlers[siid] = h

	}

	// Done
	return

}

// Show activity about the host
func watcherActivity(hostname string) (response string) {

	// Map name to address
	hostaddr := ""
	for _, v := range Config.MonitoredHosts {
		if !v.Disabled {
			if hostname == v.Name {
				hostaddr = v.Addr
				break
			}
		}
	}
	if hostaddr == "" {
		return "host not found"
	}

	// Get the list of handlers on the host
	_, _, serviceInstanceIDs, serviceInstanceAddrs, handlers, err := watcherGetServiceInstances(hostname, hostaddr)
	if err != nil {
		return err.Error()
	}
	if len(serviceInstanceAddrs) == 0 {
		return "no instances found for host"
	}

	// Grab the activity from all the handlers
	instances := int64(0)
	sessionsActive := int64(0)
	eventsPending := int64(0)
	pendingMessage := ""
	for i, addr := range serviceInstanceAddrs {

		// Get the handler
		h := handlers[serviceInstanceIDs[i]]

		// Get the info from the service instance
		pb, err := getServiceInstanceInfo(addr, serviceInstanceIDs[i], "", "lb")
		if err != nil {
			fmt.Printf("getServiceInstanceInfo(%s, %s): %s\n", addr, serviceInstanceIDs[i], err)
			continue
		}
		if pb.Body.LBStatus == nil {
			fmt.Printf("no lb info for (%s, %s)\n", addr, serviceInstanceIDs[i])
			continue
		}
		instances++
		sistats := *pb.Body.LBStatus
		sessions := sistats[0].ContinuousHandlersActivated - sistats[0].ContinuousHandlersDeactivated
		sessions += sistats[0].EphemeralHandlersActivated - sistats[0].EphemeralHandlersDeactivated
		events := sistats[0].EventsEnqueued - sistats[0].EventsDequeued
		sessionsActive += sessions
		eventsPending += events
		throughputUpdate(h.NodeName, sistats)
		if sessions > 0 || events > 0 {
			handlerTags := strings.Join(h.NodeTags, " ")
			handlerTags = strings.ReplaceAll(handlerTags, "_igress", "")
			handlerID := fmt.Sprintf("%s %s %-7s", strings.TrimSuffix(serviceInstanceIDs[i], ":notehandler-tcp"), h.NodeName, handlerTags)
			pendingMessage += handlerID + " "
			if sessions == 0 {
				pendingMessage += "               "
			} else {
				pendingMessage += fmt.Sprintf("%5d sess ", sessions)
			}
			if events > 0 {
				pendingMessage += fmt.Sprintf("%4d events ", events)
			} else {
				pendingMessage += "   - events "
			}
			if lastEventsThroughput[h.NodeName] > 0 {
				pendingMessage += fmt.Sprintf("(%4d in last %2dm, %5.1f/min)", lastEventsCount[h.NodeName], int(lastEventsThroughputSecs[h.NodeName]/60), lastEventsThroughput[h.NodeName]*60)
			}
			pendingMessage += "\n"
		}
	}

	// Send it as a slack message to all, rather than a response, because it times out for prod
	message := fmt.Sprintf("%s has %d instances hosting %d active sessions with %d events waiting to be processed\n",
		hostname, instances, sessionsActive, eventsPending)
	if len(pendingMessage) > 0 {
		message += "```"
		message += pendingMessage
		message += "```"
	}
	slackSendMessage(message)
	return ""

}

// Update throughput stats
func throughputUpdate(nodeName string, sistats []StatsStat) {
	_, exists := lastEventsDequeued[nodeName]
	if exists {
		lastEventsThroughputSecs[nodeName] = time.Since(lastEventsDequeuedTime[nodeName]).Seconds()
	} else {
		lastEventsThroughputSecs[nodeName] = 0
	}
	lastEventsDequeuedTime[nodeName] = time.Now()
	if lastEventsThroughputSecs[nodeName] > 0 {
		lastEventsCount[nodeName] = sistats[0].EventsDequeued - lastEventsDequeued[nodeName]
		lastEventsThroughput[nodeName] = float64(lastEventsCount[nodeName]) / lastEventsThroughputSecs[nodeName]
	} else {
		lastEventsThroughput[nodeName] = 0
	}
	lastEventsDequeued[nodeName] = sistats[0].EventsDequeued
}

// Tell the instance to process a request
func watcherSendRequest(hostname string, request string) (response string) {

	// Unquote if quoted
	s, err := strconv.Unquote(request)
	if err == nil {
		request = s
	}

	// Map name to address
	hostaddr := ""
	for _, v := range Config.MonitoredHosts {
		if !v.Disabled {
			if hostname == v.Name {
				hostaddr = v.Addr
				break
			}
		}
	}
	if hostaddr == "" {
		return "host not found"
	}

	// Get the list of handlers on the host
	_, _, serviceInstanceIDs, serviceInstanceAddrs, _, err := watcherGetServiceInstances(hostname, hostaddr)
	if err != nil {
		return err.Error()
	}
	if len(serviceInstanceAddrs) == 0 {
		return "no instances found for host"
	}

	// Grab the activity from all the handlers
	instances := int64(0)
	for i, addr := range serviceInstanceAddrs {
		_, err := getServiceInstanceInfo(addr, serviceInstanceIDs[i], request, "")
		if err != nil {
			fmt.Printf("getServiceInstanceInfo(%s, %s): %s\n", addr, serviceInstanceIDs[i], err)
			continue
		}
		instances++
	}

	return fmt.Sprintf("sent request to %d instances on %s\n", instances, hostname)

}
