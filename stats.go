// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"sync"
	"time"
)

// Standard or zip file
const zipType = ".zip"
const jsonType = ".json"
const currentType = zipType

// AggregatedStat is a structure used to aggregate stats across service instances
type AggregatedStat struct {
	Started                 int64                    `json:"started,omitempty"`
	Time                    int64                    `json:"time,omitempty"`
	DiskReads               uint64                   `json:"disk_read,omitempty"`
	DiskWrites              uint64                   `json:"disk_write,omitempty"`
	NetReceived             uint64                   `json:"net_received,omitempty"`
	NetSent                 uint64                   `json:"net_sent,omitempty"`
	HandlersEphemeral       int64                    `json:"handlers_ephemeral,omitempty"`
	HandlersDiscovery       int64                    `json:"handlers_discovery,omitempty"`
	HandlersContinuous      int64                    `json:"handlers_continuous,omitempty"`
	HandlersNotification    int64                    `json:"handlers_notification,omitempty"`
	NewHandlersEphemeral    int64                    `json:"handlers_ephemeral_new,omitempty"`
	NewHandlersDiscovery    int64                    `json:"handlers_discovery_new,omitempty"`
	NewHandlersContinuous   int64                    `json:"handlers_continuous_new,omitempty"`
	NewHandlersNotification int64                    `json:"handlers_notification_new,omitempty"`
	EventsReceived          int64                    `json:"events_received,omitempty"`
	EventsRouted            int64                    `json:"events_routed,omitempty"`
	DatabaseReads           int64                    `json:"database_reads,omitempty"`
	DatabaseWrites          int64                    `json:"database_writes,omitempty"`
	APITotal                int64                    `json:"api_total,omitempty"`
	Databases               map[string]StatsDatabase `json:"databases,omitempty"`
	Caches                  map[string]StatsCache    `json:"caches,omitempty"`
	API                     map[string]int64         `json:"api,omitempty"`
	Fatals                  map[string]int64         `json:"fatals,omitempty"`
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

// Trace
const addStatsTrace = true

// Stats maintenance task
func statsMaintainer() {
	var err error

	// Load past stats into the in-memory maps
	statsInit()

	// Wait for a signal to update them, or a timeout
	for {
		lastUpdatedDay := todayTime()

		// Proceed if signalled, else do this several times per hour
		// because stats are only maintained by services for an hour.
		statsMaintainNow.Wait(time.Minute * time.Duration(Config.MonitorPeriodMins))

		// Maintain for every enabled host
		for _, host := range Config.MonitoredHosts {
			if !host.Disabled {
				_, _, err = statsUpdateHost(host.Name, host.Addr, lastUpdatedDay != todayTime())
				if err != nil {
					fmt.Printf("%s: error updating stats: %s\n", host.Name, err)
				}
			}
		}
	}

}

// Get the stats filename for a given UTC date
func statsFilename(host string, serviceVersion string, filetime int64, filetype string) (filename string) {
	return host + "-" + serviceVersion + "-" + time.Unix(filetime, 0).Format("20060102") + filetype
}

// Get the stats filename's full path
func statsFilepath(host string, serviceVersion string, filetime int64, filetype string) (filepath string) {
	return configDataDirectory + "/" + statsFilename(host, serviceVersion, filetime, filetype)
}

// Load stats from files
func uLoadStats(hostname string, hostaddr string, serviceVersion string, bucketSecs int64) (err error) {

	// Begin by clearing out the host
	statsServiceVersions[hostname] = ""
	uStatsVerify(hostname, hostaddr, serviceVersion, bucketSecs)

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
	fmt.Printf("OZZIE: PAUSE AFTER LOADING TODAY\n")
	time.Sleep(10 * time.Second) // OZZIE
	hs, err = readFileLocally(hostname, serviceVersion, yesterdayTime())
	if err != nil {
		err = nil
	} else {
		added, _ := uStatsAdd(hostname, hs.Addr, hs.Stats)
		if added > 0 {
			fmt.Printf("stats: loaded %d stats for %s from yesterday\n", added, hostname)
		}
	}
	fmt.Printf("OZZIE: PAUSE AFTER LOADING YESTERDAY\n")
	time.Sleep(10 * time.Second) // OZZIE

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
func uStatsVerify(hostname string, hostaddr string, serviceVersion string, bucketSecs int64) {

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

// Validate the continuity of the specified stats array, to correct any possible corruption
func uValidateStats(fixupType string, s map[string][]StatsStat, normalizedTime int64, bucketSecs64 int64) (totalEntries int, blankEntries int) {
	bucketSecs := int(bucketSecs64)

	// Get the maximum length of any entry, which will determine what we're normalizing to.  Also,
	// if normalizedTime wasn't specified, pull it out of the first entry.
	normalizedLength := 0
	maxTime := int64(0)
	for _, sis := range s {
		if len(sis) > 0 && sis[0].SnapshotTaken > maxTime {
			maxTime = sis[0].SnapshotTaken
		}
		if len(sis) > normalizedLength {
			normalizedLength = len(sis)
		}
	}
	if normalizedTime == 0 {
		normalizedTime = maxTime
	}

	// Iterate over each stats array, normalizing to normalizedTime and normalizedLength
	for siid, sis := range s {

		// Do a pre-check to see if the entire array is fine
		bad := false
		if normalizedLength != len(sis) {
			bad = true
			fmt.Printf("fixup: length %d != %d\n", normalizedLength, len(sis))
		}
		for i := 0; i < normalizedLength; i++ {
			if sis[i].SnapshotTaken != normalizedTime-int64(i*bucketSecs) {
				bad = true
				t1 := time.Unix(sis[i].SnapshotTaken, 0).UTC()
				t1s := t1.Format("01-02 15:04:05")
				t2 := time.Unix(normalizedTime-int64(i*bucketSecs), 0).UTC()
				t2s := t2.Format("01-02 15:04:05")
				fmt.Printf("fixup %s: len:%d entry %d's time %s != expected time %s\n", fixupType, normalizedLength, i, t1s, t2s)
			}
			if sis[i].OSMemTotal == 0 {
				blankEntries++
			}
			totalEntries++
		}

		// Don't do the fixup if it's fine
		if !bad {
			continue
		}
		fmt.Printf("fixup %s: doing fixup: length:%d total:%d blank:%d\n", fixupType, normalizedLength, totalEntries, blankEntries)

		// Do the fixup, which is a slow process
		newStats := make([]StatsStat, normalizedLength)
		for i := 0; i < normalizedLength; i++ {
			newStats[i].SnapshotTaken = normalizedTime - int64(bucketSecs*i)
		}
		for sn, stat := range sis {
			i := int(normalizedTime-stat.SnapshotTaken) / bucketSecs
			if i < 0 || i >= normalizedLength {
				fmt.Printf("can't place stat %d during fixup\n", i)
			} else {
				if newStats[i].SnapshotTaken != stat.SnapshotTaken {
					fmt.Printf("huh?")
				} else {
					if sn != i {
						fmt.Printf("fixup: placed %d in %d\n", sn, i)
					}
					newStats[i] = stat
				}
			}
		}

		// Done
		fmt.Printf("fixup %s: %s FIXED UP to be of length %d instead of %d\n", fixupType, siid, len(newStats), len(s[siid]))
		s[siid] = newStats

	}

	// Done
	return

}

// Add stats to the in-memory vector of stats.
func uStatsAdd(hostname string, hostaddr string, s map[string][]StatsStat) (added int, addedStats map[string][]StatsStat) {

	// Exit if no map (this is to be expected in initialization cases)
	if s == nil {
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

	// Validate both existing stats arrays and the ones being added, just as a sanity check
	if len(s) > 0 {
		totalEntries, blankEntries := uValidateStats("new", s, 0, bucketSecs)
		if blankEntries > 0 {
			fmt.Printf("uStatsAdd: adding %d blank entries (of %d total) to %s\n", blankEntries, totalEntries, hostname)
		}
	}
	fmt.Printf("OZZIE: PAUSE AFTER NEW STATS VALIDATED\n")
	time.Sleep(10 * time.Second) // OZZIE
	if len(hs.Stats) > 0 {
		uValidateStats("existing", hs.Stats, hs.Time, bucketSecs)
	}
	fmt.Printf("OZZIE: PAUSE AFTER EXISTING STATS VALIDATED\n")
	time.Sleep(10 * time.Second) // OZZIE

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
		}
		hs.Time = mostRecentTime
	}

	// If the time is less recent than the one found, extend all arrays at the end
	for siid, sis := range hs.Stats {
		hsLeastRecentTime := hs.Time - (int64(len(sis)) * bucketSecs)
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
		}
	}

	// Checkpoint the stats here so that if an error happens below, our stat arrays
	// will at least all be the correct length
	stats[hostname] = hs

	// As purely a sanity check to validate the performance of the above, validate
	// the core assumptions that all siids are uniform, and that all siids encompass
	// the window of the stats being inserted
	hsBuckets := 0
	for _, sis := range hs.Stats {
		if hs.Time != sis[0].SnapshotTaken {
			fmt.Printf("*** error: unexpected %d != snapshot taken %d\n", hs.Time, sis[0].SnapshotTaken)
			statsAnalyze("", sis, bucketSecs)
			return
		}
		if hs.Time < mostRecentTime {
			fmt.Printf("*** error: unexpected %d < most recent time %d\n", hs.Time, mostRecentTime)
			statsAnalyze("", sis, bucketSecs)
			return
		}
		if hsBuckets == 0 {
			hsBuckets = len(sis)
			hsLeastRecentTime := hs.Time - (bucketSecs * int64(hsBuckets))
			if hsLeastRecentTime > leastRecentTime {
				fmt.Printf("*** error: hs truncated %d > %d\n", hsLeastRecentTime, leastRecentTime)
				statsAnalyze("", sis, bucketSecs)
				return
			}
		}
		if len(sis) != hsBuckets {
			fmt.Printf("*** error: nonuniform numBuckets %d != %d\n", hsBuckets, len(sis))
			statsAnalyze("", sis, bucketSecs)
			return
		}
	}

	// For each new stat coming in, set the array contents
	OZZIEMessageCount := 0
	for siid, sis := range s {
		var newStats []StatsStat
		for sn, snew := range sis {
			i := (mostRecentTime - snew.SnapshotTaken) / bucketSecs
			if i < 0 || i > int64(len(hs.Stats[siid])) {
				fmt.Printf("*** error: out of bounds %d, %d\n", i, len(hs.Stats[siid]))
				continue
			}
			if i != int64(sn) {
				fmt.Printf("adding input stat %d as new stat %d\n", i, sn)
			}
			if hs.Stats[siid][i].SnapshotTaken != snew.SnapshotTaken {
				OZZIEMessageCount++
				if OZZIEMessageCount < 10 {
					fmt.Printf("currentIndex:%d NewIndex:%d out of place?  %d != %d\n", hs.Stats[siid][i].SnapshotTaken, snew.SnapshotTaken)
				}
				statsAnalyze("BEING ADDED ", sis, bucketSecs)
				statsAnalyze("CURRENT ", hs.Stats[siid], bucketSecs)
				time.Sleep(60 * time.Second)
			}
			if snew.OSMemTotal != 0 {
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
	fmt.Printf("OZZIE: PAUSE AFTER STATS ADDED\n")
	time.Sleep(10 * time.Second) // OZZIE
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
	hs := stats[hostname]
	t := time.Unix(hs.Time, 0).UTC()
	ts := t.Format("01-02 15:04:05")
	fmt.Printf("Stats for host %s (%s)\n", hostname, ts)
	for siid, sis := range hs.Stats {
		fmt.Printf("    %s\n", siid)
		statsAnalyze("        ", sis, hs.BucketMins*60)
	}

}

// Analyze stats
func statsAnalyze(prefix string, stats []StatsStat, bucketSecs int64) {
	var highest, lowest, prev int64
	count := 0
	for i, s := range stats {
		blank := ""
		if s.OSMemTotal == 0 {
			blank = "blank"
		}
		count++
		if highest == 0 {
			highest = s.SnapshotTaken
		}
		lowest = s.SnapshotTaken
		if prev == 0 {
			t2 := time.Unix(lowest, 0).UTC()
			t2s := t2.Format("01-02 15:04:05")
			fmt.Printf("%s*** %d ok this:%s %s\n", prefix, i, t2s, blank)
		} else {
			if lowest >= prev {
				t1 := time.Unix(prev, 0).UTC()
				t1s := t1.Format("01-02 15:04:05")
				t2 := time.Unix(lowest, 0).UTC()
				t2s := t2.Format("01-02 15:04:05")
				fmt.Printf("%s*** not descending %d prev:%s this:%s %s\n", prefix, i, t1s, t2s, blank)
			} else {
				shouldBe := prev - bucketSecs
				if shouldBe != lowest {
					t1 := time.Unix(lowest, 0).UTC()
					t1s := t1.Format("01-02 15:04:05")
					t2 := time.Unix(shouldBe, 0).UTC()
					t2s := t2.Format("01-02 15:04:05")
					fmt.Printf("%s*** not exact %d this:%s shouldBe:%s %s\n", prefix, i, t1s, t2s, blank)
				} else {
					t2 := time.Unix(lowest, 0).UTC()
					t2s := t2.Format("01-02 15:04:05")
					fmt.Printf("%s*** %d ok this:%s %s\n", prefix, i, t2s, blank)
				}
			}
		}
		prev = lowest
	}
	t1 := time.Unix(highest, 0).UTC()
	t1s := t1.Format("01-02 15:04:05")
	t2 := time.Unix(lowest, 0).UTC()
	t2s := t2.Format("01-02 15:04:05")
	fmt.Printf("%s%s - %s (%d entries)\n", prefix, t1s, t2s, count)
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

	// Update today's stats into the file system and S3
	contents, err := writeFileLocally(hostname, serviceVersion, todayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, serviceVersion, todayTime(), currentType), err)
	} else {
		err = s3UploadStats(statsFilename(hostname, serviceVersion, todayTime(), currentType), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, serviceVersion, todayTime(), currentType), err)
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
func statsUpdateHost(hostname string, hostaddr string, reload bool) (ss serviceSummary, handlers map[string]AppHandler, err error) {

	// Only one in here at a time
	statsLock.Lock()
	defer statsLock.Unlock()

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
	if reload || serviceVersionChanged {
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
	uStatsVerify(hostname, hostaddr, ss.ServiceVersion, ss.BucketSecs)

	// Update the stats in-memory
	added, addedStats := uStatsAdd(hostname, hostaddr, statsLastHour)
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

	// Read the contents
	var contents []byte
	filepath := statsFilepath(hostname, serviceVersion, beginTime, currentType)
	contents, err = ioutil.ReadFile(filepath)
	if err != nil {
		return
	}

	// If it's a zip type, unzip the first file within the archive
	if currentType == zipType {
		lenBefore := len(contents)
		archive, err2 := zip.NewReader(bytes.NewReader(contents), int64(len(contents)))
		if err2 != nil {
			err = err2
			return
		}
		for _, zf := range archive.File {
			f, err2 := zf.Open()
			if err != nil {
				err = err2
				return
			}
			contents, err = ioutil.ReadAll(f)
			f.Close()
			if err != nil {
				return
			}
			if len(contents) > 0 {
				break
			}
		}
		fmt.Printf("readFile: unzipped %d to %d\n", lenBefore, len(contents))
	}

	// Unmarshal it
	err = json.Unmarshal(contents, &hs)
	if err != nil {
		return
	}
	return
}

// Write a file locally
func writeFileLocally(hostname string, serviceVersion string, beginTime int64, duration int64) (contents []byte, err error) {

	// Marshal the stats into a bytes buffer
	hs, _ := statsExtract(hostname, beginTime, duration)
	contents, err = json.Marshal(hs)
	if err != nil {
		return
	}

	// If desired, convert the bytes to zip format
	if currentType == zipType {
		lenBefore := len(contents)
		buf := new(bytes.Buffer)
		zipWriter := zip.NewWriter(buf)
		zipFile, err2 := zipWriter.Create(statsFilename(hostname, serviceVersion, beginTime, jsonType))
		if err2 != nil {
			err = err2
			return
		}
		_, err = zipFile.Write(contents)
		if err != nil {
			return
		}
		err = zipWriter.Close()
		if err != nil {
			return
		}
		contents = buf.Bytes()
		fmt.Printf("writeFile: zipped %d to %d\n", lenBefore, len(contents))
	}

	// Write the file
	err = ioutil.WriteFile(statsFilepath(hostname, serviceVersion, beginTime, currentType), contents, 0644)
	if err != nil {
		return
	}

	// Return the contents
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
func statsAggregateAsStatsStat(allStats map[string][]StatsStat, bucketSecs int64) (aggregatedStats []StatsStat) {

	as := statsAggregate(allStats, bucketSecs)

	// Pull them together
	for _, s := range as {
		lbs := StatsStat{}
		lbs.SnapshotTaken = s.Time
		lbs.OSDiskRead = s.DiskReads
		lbs.OSDiskWrite = s.DiskWrites
		lbs.OSNetReceived = s.NetReceived
		lbs.OSNetSent = s.NetSent
		lbs.DiscoveryHandlersActivated = s.NewHandlersDiscovery
		lbs.EphemeralHandlersActivated = s.NewHandlersEphemeral
		lbs.ContinuousHandlersActivated = s.NewHandlersContinuous
		lbs.NotificationHandlersActivated = s.NewHandlersNotification
		lbs.DiscoveryHandlersDeactivated = s.HandlersDiscovery
		lbs.EphemeralHandlersDeactivated = s.HandlersEphemeral
		lbs.ContinuousHandlersDeactivated = s.HandlersContinuous
		lbs.NotificationHandlersDeactivated = s.HandlersNotification
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
			as.NewHandlersEphemeral += s.EphemeralHandlersActivated
			as.NewHandlersContinuous += s.ContinuousHandlersActivated
			as.NewHandlersDiscovery += s.DiscoveryHandlersActivated
			as.NewHandlersNotification += s.NotificationHandlersActivated
			as.HandlersEphemeral += s.EphemeralHandlersDeactivated
			as.HandlersContinuous += s.ContinuousHandlersDeactivated
			as.HandlersDiscovery += s.DiscoveryHandlersDeactivated
			as.HandlersNotification += s.NotificationHandlersDeactivated

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
					if cache.Invalidations > v.Invalidations {
						v.Invalidations = cache.Invalidations
					}
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
