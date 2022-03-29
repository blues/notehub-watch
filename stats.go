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
	Started              int64                    `json:"started,omitempty"`
	Time                 int64                    `json:"time,omitempty"`
	DiskReads            uint64                   `json:"disk_read,omitempty"`
	DiskWrites           uint64                   `json:"disk_write,omitempty"`
	NetReceived          uint64                   `json:"net_received,omitempty"`
	NetSent              uint64                   `json:"net_sent,omitempty"`
	HandlersEphemeral    int64                    `json:"handlers_ephemeral,omitempty"`
	HandlersDiscovery    int64                    `json:"handlers_discovery,omitempty"`
	HandlersContinuous   int64                    `json:"handlers_continuous,omitempty"`
	HandlersNotification int64                    `json:"handlers_notification,omitempty"`
	EventsReceived       int64                    `json:"events_received,omitempty"`
	EventsRouted         int64                    `json:"events_routed,omitempty"`
	DatabaseReads        int64                    `json:"database_reads,omitempty"`
	DatabaseWrites       int64                    `json:"database_writes,omitempty"`
	APITotal             int64                    `json:"api_total,omitempty"`
	Databases            map[string]StatsDatabase `json:"databases,omitempty"`
	Caches               map[string]StatsCache    `json:"caches,omitempty"`
	API                  map[string]int64         `json:"api,omitempty"`
	Fatals               map[string]int64         `json:"fatals,omitempty"`
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
	Stats      map[string][]StatsStat `json:"stats,omitempty"`
}

// Globals
const secs1Day = (60 * 60 * 24)

var statsInitCompleted int64
var statsMaintainNow *Event
var statsLock sync.Mutex
var stats map[string]HostStats
var statsServiceVersions map[string]string
var statsUpdateLock sync.Mutex

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
				_, _, err = statsUpdateHost(host.Name, host.Addr)
				if err != nil {
					fmt.Printf("%s: error updating stats: %s\n", host.Name, err)
				}
			}
		}

	}

}

// Get the stats filename for a given UTC date
func statsFilename(host string, serviceVersion string, filetime int64) (filename string) {
	return host + "-" + serviceVersion + "-" + time.Unix(filetime, 0).Format("20060102") + ".json"
}

// Get the stats filename's full path
func statsFilepath(host string, serviceVersion string, filetime int64) (filepath string) {
	return configDataDirectory + "/" + statsFilename(host, serviceVersion, filetime)
}

// Load stats from files
func uLoadStats(hostname string, hostaddr string, serviceVersion string, bucketSecs int64) (err error) {

	// Begin by clearing out the host
	statsServiceVersions[hostname] = ""
	statsVerify(hostname, hostaddr, serviceVersion, bucketSecs)

	// Load the files
	var hs HostStats
	hs, err = readFileLocally(hostname, serviceVersion, todayTime())
	if err != nil {
		err = nil
	} else {
		added, _ := uStatsAdd(hostname, hs.Addr, hs.Stats)
		if added > 0 {
			fmt.Printf("stats: loaded %d stats for %s from today\n", added, hostname)
		}
	}
	hs, err = readFileLocally(hostname, serviceVersion, yesterdayTime())
	if err != nil {
		err = nil
	} else {
		added, _ := uStatsAdd(hostname, hs.Addr, hs.Stats)
		if added > 0 {
			fmt.Printf("stats: loaded %d stats for %s from yesterday\n", added, hostname)
		}
	}

	// Done
	return

}

// Load stats from the file system and initialize for processing
func statsInit() {

	// Create the maintenance event and pre-trigger a maintenance cycle
	statsMaintainNow = EventNew()
	statsMaintainNow.Signal()

	// Initialize stats maps
	stats = make(map[string]HostStats)
	statsServiceVersions = make(map[string]string)

	// Remember when we began initialization
	statsInitCompleted = time.Now().UTC().Unix()

}

// Verify that the stats buckets are set up properly
func statsVerify(hostname string, hostaddr string, serviceVersion string, bucketSecs int64) {

	// Lock
	statsLock.Lock()
	defer statsLock.Unlock()

	// If service version is wrong, initialize
	if serviceVersion != statsServiceVersions[hostname] {
		statsServiceVersions[hostname] = serviceVersion
		hs := HostStats{}
		hs.Name = hostname
		hs.Addr = hostaddr
		hs.BucketMins = bucketSecs / 60
		stats[hostname] = hs
		fmt.Printf("stats: reset stats for %s\n", hostname)
	}

}

// Add stats
func statsAdd(hostname string, hostaddr string, s map[string][]StatsStat) (added int, addedStats map[string][]StatsStat) {

	// Lock and exit if no stats loaded yet
	statsLock.Lock()
	defer statsLock.Unlock()
	if !uStatsLoaded(hostname) {
		return
	}

	// Add the stats in-memory
	return uStatsAdd(hostname, hostaddr, s)

}

// Add stats to the in-memory vector of stats.
func uStatsAdd(hostname string, hostaddr string, s map[string][]StatsStat) (added int, addedStats map[string][]StatsStat) {

	// Exit if no map
	if s == nil {
		fmt.Printf("uStatsAdd: %s: *** no stats to add ***\n", hostname)
		return
	}

	// Initialize output map
	addedStats = make(map[string][]StatsStat)

	// Get the host's stats record
	hs := stats[hostname]
	if hs.Stats == nil {
		hs.Stats = map[string][]StatsStat{}
	}
	bucketMins := hs.BucketMins
	bucketSecs := (bucketMins * 60)

	// Exit if hoststats is invalid
	if hs.BucketMins == 0 {
		fmt.Printf("uStatsAdd: %s: *** invalid host stats ***\n", hostname)
		return
	}

	// Make sure there are map entries for all the service instances we're adding, and
	// that we can always feel safe in referencing the [0] entry of a stats array.
	// Also, as a side-effect, ensure that there is uniformity in the length of
	// the stats array in each and all of the stats across all service instances.
	buckets := int64(0)
	var mostRecentTime, leastRecentTime int64
	for siid, sis := range s {
		if hs.Stats[siid] == nil {
			hs.Stats[siid] = []StatsStat{}
		}
		if buckets == 0 {
			buckets = int64(len(sis))
			mostRecentTime = sis[0].SnapshotTaken
			leastRecentTime = mostRecentTime - (buckets * bucketSecs)
		}
		if int64(len(sis)) != buckets || buckets == 0 {
			fmt.Printf("uStatsAdd: %s: *** non-uniform stats len (%d != %d) ***\n", hostname, len(sis), buckets)
			return
		}
		if mostRecentTime != sis[0].SnapshotTaken || mostRecentTime == 0 {
			fmt.Printf("uStatsAdd: %s: *** non-uniform stats time (%d != %d) ***\n", hostname, sis[0].SnapshotTaken, mostRecentTime)
		}
	}
	if addStatsTrace {
		fmt.Printf("uStatsAdd: %s: buckets:%d recent:%d least:%d\n", hostname, buckets, mostRecentTime, leastRecentTime)
	}

	// If the base time needs to be updated, do so
	if hs.Time == 0 {
		hs.Time = mostRecentTime
		fmt.Printf("uStatsAdd: %s: initializing time\n", hostname)
	}

	// If the time is more recent than the existing base time, extend all arrays at the front
	if mostRecentTime > hs.Time {
		arrayEntries := (mostRecentTime - hs.Time) / bucketSecs
		if addStatsTrace {
			fmt.Printf("adding %d entries at front (more recent)\n", arrayEntries)
		}
		z := make([]StatsStat, arrayEntries)
		for i := int64(0); i < arrayEntries; i++ {
			z[i].SnapshotTaken = mostRecentTime - (bucketSecs * i)
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
		hsLeastRecentTime := hs.Time - (int64(len(sis)) * bucketSecs)
		if addStatsTrace {
			fmt.Printf("leastRecent Time in HS = %d\n", hsLeastRecentTime)
		}
		if hsLeastRecentTime > leastRecentTime {
			arrayEntries := (hsLeastRecentTime - leastRecentTime) / bucketSecs
			if addStatsTrace {
				fmt.Printf("for %s adding %d entries at end\n", siid, arrayEntries)
			}
			z := make([]StatsStat, arrayEntries)
			for i := int64(0); i < arrayEntries; i++ {
				z[i].SnapshotTaken = hsLeastRecentTime - (bucketSecs * i)
			}
			hs.Stats[siid] = append(hs.Stats[siid], z...)
			if addStatsTrace {
				fmt.Printf("%s now %d entries\n", siid, len(hs.Stats[siid]))
			}
		}
	}

	// As purely a sanity check to validate the performance of the above, validate
	// the core assumptions that all siids are uniform, and that all siids encompass
	// the window of the stats being inserted
	hsBuckets := 0
	for _, sis := range hs.Stats {
		if hs.Time != sis[0].SnapshotTaken {
			fmt.Printf("*** error: unexpected %d != snapshot taken %d\n", hs.Time, sis[0].SnapshotTaken)
			return
		}
		if hs.Time < mostRecentTime {
			fmt.Printf("*** error: unexpected %d < most recent time %d\n", hs.Time, mostRecentTime)
			return
		}
		if hsBuckets == 0 {
			hsBuckets = len(sis)
			hsLeastRecentTime := hs.Time - (bucketSecs * int64(hsBuckets))
			if hsLeastRecentTime > leastRecentTime {
				fmt.Printf("*** error: hs truncated %d > %d\n", hsLeastRecentTime, leastRecentTime)
				return
			}
		}
		if len(sis) != hsBuckets {
			fmt.Printf("*** error: nonuniform numBuckets %d != %d\n", hsBuckets, len(sis))
			return
		}
	}

	// For each new stat coming in, set the array contents
	for siid, sis := range s {
		var newStats []StatsStat
		for _, snew := range sis {
			i := (mostRecentTime - snew.SnapshotTaken) / bucketSecs
			if i < 0 || i > int64(len(hs.Stats[siid])) {
				fmt.Printf("*** error: out of bounds %d, %d\n", i, len(hs.Stats[siid]))
				return
			}
			if hs.Stats[siid][i].OSMemTotal != 0 {
				if snew.OSMemTotal != 0 {
					fmt.Printf("overwriting %s non-blank entry with non-blank entry %d\n", siid, i)
				} else {
					fmt.Printf("overwriting %s non-blank entry with blank entry %d\n", siid, i)
				}
				statsAnalyze("EXISTING", hs.Stats[siid])
				statsAnalyze("ADDING", s[siid])
				return
			}
			if snew.OSMemTotal == 0 {
				fmt.Printf("adding %s blank entry %d\n", siid, i)
				statsAnalyze("EXISTING", hs.Stats[siid])
				statsAnalyze("ADDING", s[siid])
				return
			}
			hs.Stats[siid][i] = snew
			newStats = append(newStats, snew)
			added++
		}
		if len(newStats) > 0 {
			addedStats[siid] = newStats
		}
	}

	// Update the main stats
	stats[hostname] = hs
	return

}

// Analyze stats for a host
func statsAnalyzeHost(hostname string) {

	// Lock and exit if no stats loaded yet
	statsLock.Lock()
	defer statsLock.Unlock()
	if !uStatsLoaded(hostname) {
		return
	}

	// Perform the analysis
	fmt.Printf("Stats for host: %s\n", hostname)
	hs := stats[hostname]
	for siid, sis := range hs.Stats {
		fmt.Printf("    %s\n", siid)
		statsAnalyze("        ", sis)
	}

}

// Analyze stats
func statsAnalyze(prefix string, stats []StatsStat) {
	var highest, lowest, prev, bucketMins int64
	count := 0
	blank := 0
	for i, s := range stats {
		if s.SnapshotTaken == 0 {
			blank++
			continue
		}
		count++
		bucketMins = s.BucketMins
		if blank != 0 {
			fmt.Printf("%s*** blank before nonblank entry\n", prefix)
		}
		if highest == 0 {
			highest = s.SnapshotTaken
		}
		lowest = s.SnapshotTaken
		if prev != 0 {
			if lowest >= prev {
				t1 := time.Unix(prev, 0).UTC()
				t1s := t1.Format("01-02 15:04:05")
				t2 := time.Unix(lowest, 0).UTC()
				t2s := t2.Format("01-02 15:04:05")
				fmt.Printf("%s*** %d prev:%s this:%s\n", prefix, i, t1s, t2s)
			}
			shouldBe := prev + (bucketMins * 60)
			if shouldBe != lowest {
				t1 := time.Unix(lowest, 0).UTC()
				t1s := t1.Format("01-02 15:04:05")
				t2 := time.Unix(shouldBe, 0).UTC()
				t2s := t2.Format("01-02 15:04:05")
				fmt.Printf("%s*** %d this:%s shouldBe:%s\n", prefix, i, t1s, t2s)
			}
		}
		prev = lowest
	}
	t1 := time.Unix(highest, 0).UTC()
	t1s := t1.Format("01-02 15:04:05")
	t2 := time.Unix(lowest, 0).UTC()
	t2s := t2.Format("01-02 15:04:05")
	t3 := time.Unix(lowest-(int64(blank)*bucketMins*60), 0).UTC()
	t3s := t3.Format("01-02 15:04:05")
	fmt.Printf("%s%s - %s (%d entries) [%d blank %s]\n", prefix, t1s, t2s, count, blank, t3s)
}

// Extract stats for the given host for a time range
func statsExtract(hostname string, beginTime int64, duration int64) (hsret HostStats, exists bool) {

	// Lock and exit if no stats loaded yet
	statsLock.Lock()
	defer statsLock.Unlock()
	if !uStatsLoaded(hostname) {
		exists = false
		return
	}

	// Perform the extraction
	return uStatsExtract(hostname, beginTime, duration)

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
	hsret.Stats = map[string][]StatsStat{}

	// Loop, appending and filtering
	for siid, sis := range hs.Stats {
		if len(sis) == 0 {
			continue
		}

		// Initialize a new return array
		sisret := []StatsStat{}

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

// Update the files with the data currently in-memory
func uSaveStats(hostname string, serviceVersion string) (err error) {

	// Update the stats for yesterday and today into the file system
	contents, err := writeFileLocally(hostname, serviceVersion, todayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, serviceVersion, todayTime()), err)
	} else {
		err = s3UploadStats(statsFilename(hostname, serviceVersion, todayTime()), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, serviceVersion, todayTime()), err)
		}
	}
	if err == nil {
		contents, err = writeFileLocally(hostname, serviceVersion, yesterdayTime(), secs1Day)
		if err != nil {
			fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, serviceVersion, yesterdayTime()), err)
		} else {
			err = s3UploadStats(statsFilename(hostname, serviceVersion, yesterdayTime()), contents)
			if err != nil {
				fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, serviceVersion, yesterdayTime()), err)
			}
		}
	}
	return
}

// Return true if stats are loaded
func uStatsLoaded(hostname string) bool {
	_, statsExist := stats[hostname]
	return statsServiceVersions[hostname] != "" && statsExist
}

// Update the host's data structures both in-memory and on-disk
func statsUpdateHost(hostname string, hostaddr string) (ss serviceSummary, handlers map[string]AppHandler, err error) {

	// Only one in here at a time
	statsUpdateLock.Lock()
	defer statsUpdateLock.Unlock()

	// Get the stats
	var serviceVersionChanged bool
	var statsLastHour map[string][]StatsStat
	serviceVersionChanged, ss, handlers, statsLastHour, err = watcherGetStats(hostname, hostaddr)
	if err != nil {
		return
	}

	// If the stats for that service version were never yet loaded, load them
	if !uStatsLoaded(hostname) {
		err = uLoadStats(hostname, hostaddr, ss.ServiceVersion, ss.BucketSecs)
		if err != nil {
			fmt.Printf("stats: error loading %s stats: %s\n", hostname, err)
			return
		}
		serviceVersionChanged = false
	}

	// If the service version changed, make sure that we write and re-load the stats
	// using the new service version.  We do this because when the service version
	// changes, all the node IDs change and thus spreadsheets would be unusable.
	if serviceVersionChanged {
		fmt.Printf("stats: %s service version changed\n", hostname)
		err = uSaveStats(hostname, ss.ServiceVersion)
		if err != nil {
			fmt.Printf("stats: error saving %s stats: %s\n", hostname, err)
		}
		err = uLoadStats(hostname, hostaddr, ss.ServiceVersion, ss.BucketSecs)
		if err != nil {
			fmt.Printf("stats: error loading %s stats: %s\n", hostname, err)
		}
	}

	// Verify that the in-memory stats are set up properly
	statsVerify(hostname, hostaddr, ss.ServiceVersion, ss.BucketSecs)

	// Update the stats in-memory
	added, addedStats := statsAdd(hostname, hostaddr, statsLastHour)
	if added > 0 {
		fmt.Printf("stats: added %d new stats for %s\n", added, hostname)
	}

	// Save the stats in case we crash
	uSaveStats(hostname, ss.ServiceVersion)

	// If this is just the initial set of stats that were being loaded from the file system, ignore it,
	// else write the stats to datadog
	if len(addedStats) > 0 && time.Now().UTC().Unix() > statsInitCompleted+60 {
		datadogUploadStats(hostname, ss.BucketSecs, addedStats)
	}

	// Done
	return

}

// Read a file locally
func readFileLocally(hostname string, serviceVersion string, beginTime int64) (hs HostStats, err error) {
	var contents []byte
	contents, err = ioutil.ReadFile(statsFilepath(hostname, serviceVersion, beginTime))
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
func writeFileLocally(hostname string, serviceVersion string, beginTime int64, duration int64) (contents []byte, err error) {
	hs, _ := statsExtract(hostname, beginTime, duration)
	contents, err = json.Marshal(hs)
	if err != nil {
		return
	}
	err = ioutil.WriteFile(statsFilepath(hostname, serviceVersion, beginTime), contents, 0644)
	if err != nil {
		return
	}
	return
}

// Sort new-to-old
type statRecency []AggregatedStat

func (list statRecency) Len() int      { return len(list) }
func (list statRecency) Swap(i, j int) { list[i], list[j] = list[j], list[i] }
func (list statRecency) Less(i, j int) bool {
	var si = list[i]
	var sj = list[j]
	return si.Time > sj.Time
}

// Aggregate a notehub stats structure across service instances back into an StatsStat structure
func statsAggregateAsLBStat(allStats map[string][]StatsStat, bucketSecs int64) (aggregatedStats []StatsStat) {

	as := statsAggregate(allStats, bucketSecs)

	// Pull them together
	for _, s := range as {
		lbs := StatsStat{}
		lbs.SnapshotTaken = s.Time
		lbs.OSDiskRead = s.DiskReads
		lbs.OSDiskWrite = s.DiskWrites
		lbs.OSNetReceived = s.NetReceived
		lbs.OSNetSent = s.NetSent
		lbs.DiscoveryHandlersActivated = s.HandlersDiscovery
		lbs.EphemeralHandlersActivated = s.HandlersEphemeral
		lbs.ContinuousHandlersActivated = s.HandlersContinuous
		lbs.NotificationHandlersActivated = s.HandlersNotification
		lbs.EventsEnqueued = s.EventsReceived
		lbs.EventsRouted = s.EventsRouted
		lbs.Databases = s.Databases
		lbs.Caches = s.Caches
		lbs.API = s.API
		lbs.Fatals = s.Fatals
		aggregatedStats = append(aggregatedStats, lbs)
	}

	return
}

// Aggregate a notehub stats structure across service instances
func statsAggregate(allStats map[string][]StatsStat, bucketSecs int64) (aggregatedStats []AggregatedStat) {

	// Assuming that all stats are on the same aligned timebase, fetch it
	if len(allStats) == 0 {
		return
	}

	// Create a data structure that aggregates stats, under the assumption that the stat
	// buckets are aligned.
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

			// Aggregate handlers.
			as.HandlersEphemeral += s.EphemeralHandlersActivated
			as.HandlersContinuous += s.ContinuousHandlersActivated
			as.HandlersDiscovery += s.DiscoveryHandlersActivated
			as.HandlersNotification += s.NotificationHandlersActivated

			// Events
			as.EventsReceived += s.EventsEnqueued
			as.EventsRouted += s.EventsRouted

			// Databases
			if as.Databases == nil {
				as.Databases = map[string]StatsDatabase{}
			}
			if s.Databases != nil {
				for key, db := range s.Databases {
					as.DatabaseReads += db.Reads
					as.DatabaseWrites += db.Writes
					v := as.Databases[key]
					v.Reads += db.Reads
					v.Writes += db.Writes
					if db.ReadMsMax > v.ReadMsMax {
						v.ReadMsMax = db.ReadMsMax
					}
					if db.WriteMsMax > v.WriteMsMax {
						v.WriteMsMax = db.WriteMsMax
					}
					as.Databases[key] = v
				}
			}

			// Caches
			if as.Caches == nil {
				as.Caches = map[string]StatsCache{}
			}
			if s.Caches != nil {
				for key, cache := range s.Caches {
					v := as.Caches[key]
					v.Invalidations += cache.Invalidations
					if cache.EntriesHWM > v.EntriesHWM {
						v.EntriesHWM = cache.EntriesHWM
					}
					as.Caches[key] = v
				}
			}

			// API calls
			if as.API == nil {
				as.API = map[string]int64{}
			}
			if s.API != nil {
				for key, apiCalls := range s.API {
					as.APITotal += apiCalls
					as.API[key] += apiCalls
				}
			}

			// Fatals calls
			if as.Fatals == nil {
				as.Fatals = map[string]int64{}
			}
			if s.Fatals != nil {
				for key, fatals := range s.Fatals {
					as.Fatals[key] += fatals
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
	sort.Sort(statRecency(aggregatedStats))

	// Done
	return

}
