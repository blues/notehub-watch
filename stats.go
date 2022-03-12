// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"sync"
	"time"
)

// AggregatedStat is a structure used to aggregate stats across service instances
type AggregatedStat struct {
	Time           int64  `json:"time,omitempty"`
	DiskReads      uint64 `json:"disk_read,omitempty"`
	DiskWrites     uint64 `json:"disk_write,omitempty"`
	NetReceived    uint64 `json:"net_received,omitempty"`
	NetSent        uint64 `json:"net_sent,omitempty"`
	Handlers       int64  `json:"handlers_discovery,omitempty"`
	EventsReceived int64  `json:"events_received,omitempty"`
	EventsRouted   int64  `json:"events_routed,omitempty"`
	DatabaseReads  int64  `json:"database_read,omitempty"`
	DatabaseWrites int64  `json:"database_write,omitempty"`
	APICalls       int64  `json:"api_calls,omitempty"`
}

// Periodic stats publisher.  The stats publisher maintains, in the local system's data directory,
// a file that shadows what it keeps in-memory: 1 day's worth of stats data starting at UTC midnight.
// One of these files is maintained for each host being monitored.  On an hourly basis aligned with
// midnight UTC, these files are archived to an S3 bucket.
//
// Separately, there is a goroutine responsible for examining the in-memory structure and streaming
// new values out to real-time listeners including DataDog.  This process takes our native stats
// format, aggregates the service endpoints, converts our stats into publishable metrics, and
// publishes it.

// This represents a set of stats aggregated for a host.  We use this structure for
// the files we write (which are UTC midnight-based for 1 day), and we use the structure
// for the in-memory structure we maintain (which is up to a rolling 48-hours).
type HostStats struct {
	Name       string                 `json:"name,omitempty"`
	Addr       string                 `json:"address,omitempty"`
	Time       int64                  `json:"time,omitempty"`
	BucketMins int64                  `json:"minutes,omitempty"`
	Stats      map[string][]AppLBStat `json:"stats,omitempty"`
}

// Globals
const secs1Day = (60 * 60 * 24)

var statsInitCompleted int64
var statsMaintainNow *Event
var statsLock sync.Mutex
var stats map[string]HostStats

// Trace
const addStatsTrace = true

// Stats maintenance task
func statsMaintainer() {
	var err error

	// Load past stats into the in-memory maps
	statsInit()

	// Wait for a signal to update them, or a timeout
	for {

		// Proceed if signalled, else do this several times per hour
		// because stats are only maintained by services for an hour.
		statsMaintainNow.Wait(time.Minute * time.Duration(Config.MonitorPeriodMins))

		// Maintain for every enabled host
		for _, host := range Config.MonitoredHosts {
			if !host.Disabled {
				err = statsMaintainHost(host.Name, host.Addr)
				if err != nil {
					fmt.Printf("statsMaintainHost: %s\n", err)
				}
			}
		}

	}

}

// Get the stats filename for a given UTC date
func statsFilename(host string, filetime int64) (filename string) {
	return host + "-" + time.Unix(filetime, 0).Format("20060102") + ".json"
}

// Get the stats filename's full path
func statsFilepath(host string, filetime int64) (filepath string) {
	return configDataDirectory + "/" + statsFilename(host, filetime)
}

// Load stats from the file system and initialize for processing
func statsInit() {

	// Create the maintenance event and pre-trigger a maintenance cycle
	statsMaintainNow = EventNew()
	statsMaintainNow.Signal()

	// Load yesterday's and today's stats from the file system
	statsLock.Lock()
	defer statsLock.Unlock()

	stats = make(map[string]HostStats)

	for _, host := range Config.MonitoredHosts {
		if !host.Disabled {
			hs, err := readFileLocally(host.Name, todayTime())
			if err == nil {
				added, _ := uStatsAdd(host.Name, host.Addr, hs.Stats)
				if added > 0 {
					fmt.Printf("stats: loaded %d stats for %s from today\n", added, host.Name)
				}
			}
			hs, err = readFileLocally(host.Name, yesterdayTime())
			if err == nil {
				added, _ := uStatsAdd(host.Name, host.Addr, hs.Stats)
				if added > 0 {
					fmt.Printf("stats: loaded %d stats for %s from yesterday\n", added, host.Name)
				}
			}
		}
	}

	// Remember when we began initialization
	statsInitCompleted = time.Now().UTC().Unix()

}

// Add stats
func statsAdd(hostname string, hostaddr string, s map[string][]AppLBStat) (added int, addedStats map[string][]AppLBStat) {
	statsLock.Lock()
	defer statsLock.Unlock()
	added, addedStats = uStatsAdd(hostname, hostaddr, s)
	return
}

// Add stats to the in-memory vector of stats.
func uStatsAdd(hostname string, hostaddr string, s map[string][]AppLBStat) (added int, addedStats map[string][]AppLBStat) {

	// Exit if no map
	if s == nil {
		return
	}

	// Initialize output map
	addedStats = make(map[string][]AppLBStat)

	// Get the host's stats record
	hs := stats[hostname]
	if hs.Stats == nil {
		hs.Stats = map[string][]AppLBStat{}
	}

	// Make sure there are map entries for all the service instances we're adding, and
	// that we can always feel safe in referencing the [0] entry of a stats array
	for siid, sis := range s {

		// If no stats in this entry, delete it (defensive coding)
		if len(sis) == 0 {
			delete(s, siid)
			continue
		}

		// If it doesn't exist, create it
		if hs.Stats[siid] == nil {
			hs.Stats[siid] = []AppLBStat{}
		}

	}

	// Find the lowest and highest base times within any of the new stats to be added
	var mostRecentTimeSet, leastRecentTimeSet bool
	var mostRecentTime, leastRecentTime int64
	var bucketMins int64
	for _, serviceInstance := range s {
		if len(serviceInstance) > 0 {
			if !mostRecentTimeSet || serviceInstance[0].SnapshotTaken > mostRecentTime {
				mostRecentTimeSet = true
				mostRecentTime = serviceInstance[0].SnapshotTaken
				bucketMins = serviceInstance[0].BucketMins
			}
			buckets := int64(len(serviceInstance))
			bucketSecs := (serviceInstance[0].BucketMins * 60)
			ht := serviceInstance[0].SnapshotTaken - (buckets * bucketSecs)
			if !leastRecentTimeSet || ht < leastRecentTime {
				leastRecentTimeSet = true
				leastRecentTime = ht
			}

		}
	}
	if mostRecentTime == 0 || leastRecentTime == 0 {
		return
	}
	if addStatsTrace {
		fmt.Printf("host:%s recent:%d least:%d\n", hostname, mostRecentTime, leastRecentTime)
	}

	// If the base time isn't yet set in our host stats, set it
	if hs.Time == 0 {
		hs.Time = mostRecentTime
		hs.BucketMins = bucketMins
		hs.Name = hostname
		hs.Addr = hostaddr
	}

	// If the time is more recent than the existing base time, extend all arrays at the front
	if mostRecentTime > hs.Time {
		arrayEntries := (mostRecentTime - hs.Time) / 60 / hs.BucketMins
		if addStatsTrace {
			fmt.Printf("adding %d entries at front (more recent)\n", arrayEntries)
		}
		z := make([]AppLBStat, arrayEntries)
		for i := int64(0); i < arrayEntries; i++ {
			z[i].SnapshotTaken = mostRecentTime - (bucketMins * 60 * i)
			z[i].BucketMins = bucketMins
		}
		for siid := range hs.Stats {
			hs.Stats[siid] = append(z, hs.Stats[siid]...)
			if addStatsTrace {
				fmt.Printf("%s now %d entries\n", siid, len(hs.Stats[siid]))
			}
		}
		hs.Time = mostRecentTime
	}

	// If the time is less recent than the one found, extend all arrays at the end
	for siid, sis := range hs.Stats {
		hsLeastRecentTime := mostRecentTime - (int64(len(sis)) * bucketMins * 60)
		if addStatsTrace {
			fmt.Printf("leastRecent Time in HS = %d\n", hsLeastRecentTime)
		}
		if hsLeastRecentTime > leastRecentTime {
			arrayEntries := (hsLeastRecentTime - leastRecentTime) / 60 / hs.BucketMins
			if addStatsTrace {
				fmt.Printf("for %s adding %d entries at end\n", siid, arrayEntries)
			}
			z := make([]AppLBStat, arrayEntries)
			for i := int64(0); i < arrayEntries; i++ {
				z[i].SnapshotTaken = hsLeastRecentTime - (bucketMins * 60 * i)
				z[i].BucketMins = bucketMins
			}
			hs.Stats[siid] = append(hs.Stats[siid], z...)
			if addStatsTrace {
				fmt.Printf("%s now %d entries\n", siid, len(hs.Stats[siid]))
			}
		}
		hs.Time = mostRecentTime
	}

	// For each new stat coming in, set the array contents
	for siid, sis := range s {
		var newStats []AppLBStat
		for _, snew := range sis {
			i := (mostRecentTime - snew.SnapshotTaken) / 60 / bucketMins
			if i >= int64(len(sis)) {
				break
			}
			if hs.Stats[siid][i].Started == snew.Started {
				if addStatsTrace {
					fmt.Printf("skipping %s entry %d\n", siid, i)
				}
			} else {
				if addStatsTrace {
					fmt.Printf("overwriting %s entry %d\n", siid, i)
				}
				hs.Stats[siid][i] = snew
				newStats = append(newStats, snew)
				added++
			}
		}
		if len(newStats) > 0 {
			addedStats[siid] = newStats
		}
	}

	// Update the main stats
	stats[hostname] = hs
	return

}

// Extract stats for the given host for a time range
func statsExtract(hostname string, beginTime int64, duration int64) (hsret HostStats, exists bool) {
	statsLock.Lock()
	defer statsLock.Unlock()
	hsret, exists = uStatsExtract(hostname, beginTime, duration)
	return
}

// Extract stats for the given host for a time range, locked
func uStatsExtract(hostname string, beginTime int64, duration int64) (hsret HostStats, exists bool) {

	// Get the existing value, and exit if we want the whole thing
	hsret, exists = stats[hostname]
	if duration == 0 {
		return
	}

	// Initialize host stats
	hs := hsret
	hsret = HostStats{}
	hsret.Name = hs.Name
	hsret.Addr = hs.Addr
	hsret.BucketMins = hs.BucketMins
	hsret.Stats = map[string][]AppLBStat{}

	// Loop, appending and filtering
	for siid, sis := range hs.Stats {
		if len(sis) == 0 {
			continue
		}

		// Initialize a new return array
		sisret := []AppLBStat{}

		// Iterate over the stats, filtering.  We use the knowledge that the statss
		// are ordered most-recent to least-recent in how we stop the scan.
		for _, s := range sis {
			if s.SnapshotTaken < beginTime {
				break
			}
			if s.SnapshotTaken < (beginTime + duration) {
				sisret = append(sisret, s)
				if s.SnapshotTaken > hsret.Time {
					hsret.Time = s.SnapshotTaken
				}
			}
		}

		// Store the stats for this instance
		if len(sisret) != 0 {
			hsret.Stats[siid] = sisret
		}

	}

	// Done
	return

}

// Get the UTC for today's midnight
func todayTime() int64 {
	return (time.Now().UTC().Unix() / secs1Day) * secs1Day
}

// Get the UTC for today's midnight
func yesterdayTime() int64 {
	return todayTime() - secs1Day
}

// Maintain a single host
func statsMaintainHost(hostname string, hostaddr string) (err error) {

	// Get the stats
	var stats map[string][]AppLBStat
	_, stats, err = watcherGetStats(hostaddr)
	if err != nil {
		return
	}

	// Update the stats in-memory
	added, addedStats := statsAdd(hostname, hostaddr, stats)
	if added > 0 {
		fmt.Printf("stats: added %d new stats for %s\n", added, hostname)
	}

	// Update the stats for yesterday and today into the file system
	contents, err := writeFileLocally(hostname, todayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, todayTime()), err)
	} else {
		err = s3UploadStats(statsFilename(hostname, todayTime()), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, todayTime()), err)
		}
	}
	contents, err = writeFileLocally(hostname, yesterdayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, yesterdayTime()), err)
	} else {
		err = s3UploadStats(statsFilename(hostname, yesterdayTime()), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, yesterdayTime()), err)
		}
	}

	// If this is just the initial set of stats that were being loaded from the file system, ignore it,
	// else write the stats to datadog
	if len(addedStats) > 0 && time.Now().UTC().Unix() > statsInitCompleted+60 {
		datadogUploadStats(hostname, addedStats)
	}

	// Done
	return

}

// Read a file locally
func readFileLocally(hostname string, beginTime int64) (hs HostStats, err error) {
	var contents []byte
	contents, err = ioutil.ReadFile(statsFilepath(hostname, beginTime))
	if err != nil {
		return
	}
	err = json.Unmarshal(contents, &hs)
	if err != nil {
		return
	}
	return
}

// Write a file locally
func writeFileLocally(hostname string, beginTime int64, duration int64) (contents []byte, err error) {
	hs, _ := statsExtract(hostname, beginTime, duration)
	contents, err = json.Marshal(hs)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(statsFilepath(hostname, beginTime), contents, 0644)
	if err != nil {
		return
	}
	return
}

type statOccurrence []AggregatedStat

func (list statOccurrence) Len() int { return len(list) }

func (list statOccurrence) Swap(i, j int) { list[i], list[j] = list[j], list[i] }

func (list statOccurrence) Less(i, j int) bool {
	var si = list[i]
	var sj = list[j]
	return si.Time < sj.Time
}

// Aggregate a notehub stats structure across service instances
func statsAggregate(allStats map[string][]AppLBStat) (bucketSecs int64, aggregatedStats []AggregatedStat) {

	// Assuming that all stats are on the same aligned timebase, fetch it
	if len(allStats) == 0 {
		return
	}
	for _, sis := range allStats {
		if len(sis) != 0 {
			bucketSecs = sis[0].BucketMins * 60
			break
		}
	}
	if bucketSecs == 0 {
		return
	}

	// Create a data structure that aggregates stats.  This is somewhat challenging because
	// the different service instances began their "5 minute buckets" on different time bases
	// and as such we can only aggregate them by seeing if they are in the same 'time bucket',
	// as defined by the snapshot time divided by the seconds-per-bucket.
	aggregatedStatsByBucket := make(map[int]AggregatedStat)
	for _, sis := range allStats {
		for _, s := range sis {
			bucketID := int(s.SnapshotTaken / bucketSecs)
			as := aggregatedStatsByBucket[bucketID]
			as.Time = int64(bucketID) * bucketSecs

			// Aggregate a common stat across instances
			as.DiskReads += s.OSDiskRead
			as.DiskWrites += s.OSDiskWrite
			as.NetReceived += s.OSNetReceived
			as.NetSent += s.OSNetSent

			// Aggregate handlers.  Note that I made an explicit decision not to aggregate
			// Notification handlers or Discovery handlers because generally the use of
			// this statistic is to understand how many total devices are being served.
			as.Handlers += s.EphemeralHandlersActivated
			as.Handlers += s.ContinuousHandlersActivated

			// Events
			as.EventsReceived += s.EventsEnqueued
			as.EventsRouted += s.EventsRouted

			// Databases
			if s.Databases != nil {
				for _, db := range s.Databases {
					as.DatabaseReads += db.Reads
					as.DatabaseWrites += db.Writes
				}
			}

			// API calls
			if s.API != nil {
				for _, apiCalls := range s.API {
					as.APICalls += apiCalls
				}
			}

			// Update the aggregated stat
			aggregatedStatsByBucket[bucketID] = as

		}
	}

	// Generate a flat array of stats
	for _, s := range aggregatedStatsByBucket {
		aggregatedStats = append(aggregatedStats, s)
	}

	// Sort the stats
	sort.Sort(statOccurrence(aggregatedStats))

	// Done
	return

}
