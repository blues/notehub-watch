// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"
)

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
var statsMaintainNow *Event
var statsLock sync.Mutex
var stats map[string]HostStats

// Stats maintenance task
func statsMaintainer() {
	var err error

	// Load past stats into the in-memory maps
	statsInit()

	// Wait for a signal to update them, or a timeout
	for {

		// Proceed if signalled, else do this several times per hour
		// because stats are only maintained by services for an hour.
		statsMaintainNow.Wait(time.Duration(time.Minute * 15))

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
	timestr := time.Unix(filetime, 0).Format("20060102-150405")
	filename = configDataDirectory + "/" + host + "-" + timestr + ".json"
	return

}

// Load stats from the file system and initialize for processing
func statsInit() {

	// Create the maintenance event and pre-trigger a maintenance cycle
	statsMaintainNow = EventNew()
	statsMaintainNow.Signal()

	// Load yesterday's and today's stats from the file system
	statsLock.Lock()
	stats = make(map[string]HostStats)

	for _, host := range Config.MonitoredHosts {
		if !host.Disabled {
			fnToday := statsFilename(host.Name, time.Now().UTC().Unix())
			contents, err := ioutil.ReadFile(fnToday)
			if err == nil {
				var hs HostStats
				err = json.Unmarshal(contents, &hs)
				if err == nil {
					uAddStats(host.Name, host.Addr, hs.Stats)
					fmt.Printf("%d stats loaded from today\n", len(hs.Stats))
				}
			}
			fnYesterday := statsFilename(host.Name, time.Now().UTC().Unix()-(60*60*24))
			contents, err = ioutil.ReadFile(fnYesterday)
			if err == nil {
				var hs HostStats
				err = json.Unmarshal(contents, &hs)
				if err == nil {
					uAddStats(host.Name, host.Addr, hs.Stats)
					fmt.Printf("%d stats loaded from yesterday\n", len(hs.Stats))
				}
			}
		}
	}
	statsLock.Unlock()

}

// Add stats to the in-memory vector of stats.
func uAddStats(hostname string, hostaddr string, s map[string][]AppLBStat) {

	// Exit if no map
	if s == nil {
		return
	}

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
	var lowestTimeSet, highestTimeSet bool
	var lowestTime, highestTime int64
	var bucketMins int64
	for _, serviceInstance := range s {
		if len(serviceInstance) > 0 {
			if !lowestTimeSet || serviceInstance[0].SnapshotTaken < lowestTime {
				lowestTimeSet = true
				lowestTime = serviceInstance[0].SnapshotTaken
				bucketMins = serviceInstance[0].BucketMins
			}
			buckets := int64(len(serviceInstance))
			bucketSecs := (serviceInstance[0].BucketMins * 60)
			ht := serviceInstance[0].SnapshotTaken + (buckets * bucketSecs)
			if !highestTimeSet || ht > highestTime {
				highestTimeSet = true
				highestTime = ht
			}

		}
	}
	if lowestTime == 0 || highestTime == 0 {
		return
	}
	fmt.Printf("OZZIE host:%s lowest:%d highest:%d mins:%d\n", hostname, lowestTime, highestTime, bucketMins)

	// If the base time isn't yet set in our host stats, set it
	if hs.Time == 0 {
		hs.Time = lowestTime
		hs.BucketMins = bucketMins
		hs.Name = hostname
		hs.Addr = hostaddr
		fmt.Printf("OZZIE initialized hs: %+v\n", hs)
	}

	// If the time is lower than the existing base time, extend all arrays at the front
	if lowestTime < hs.Time {
		arrayEntries := (hs.Time - lowestTime) / 60 / hs.BucketMins
		fmt.Printf("OZZIE adding %d entries below\n", arrayEntries)
		z := make([]AppLBStat, arrayEntries)
		for i := int64(0); i < arrayEntries; i++ {
			z[i].SnapshotTaken = lowestTime + (bucketMins * 60 * i)
			z[i].BucketMins = bucketMins
		}
		for siid := range hs.Stats {
			hs.Stats[siid] = append(z, hs.Stats[siid]...)
			fmt.Printf("OZZIE %s now %d entries\n", siid, len(hs.Stats))
		}
		hs.Time = lowestTime
	}

	// If the time is higher than the high time, extend all arrays at the end
	for siid, sis := range hs.Stats {
		highTime := lowestTime + (int64(len(sis)) * bucketMins * 60)
		if highestTime > highTime {
			arrayEntries := (highestTime - highTime) / 60 / hs.BucketMins
			fmt.Printf("OZZIE for %s adding %d entries at end\n", siid, arrayEntries)
			z := make([]AppLBStat, arrayEntries)
			for i := int64(0); i < arrayEntries; i++ {
				z[i].SnapshotTaken = highTime + (bucketMins * 60 * i)
				z[i].BucketMins = bucketMins
			}
			hs.Stats[siid] = append(hs.Stats[siid], z...)
			fmt.Printf("OZZIE %s now %d entries\n", siid, len(hs.Stats))
		}
		hs.Time = lowestTime
	}

	// For each new stat coming in, set the array contents
	for ssid, sis := range s {
		for _, snew := range sis {
			i := (snew.SnapshotTaken - lowestTime) / 60 / bucketMins
			fmt.Printf("OZZIE overwriting:\n      %+v\n with %+v\n", hs.Stats[ssid][i], snew)
			hs.Stats[ssid][i] = snew
		}
	}

	// Update the main stats
	stats[hostname] = hs

}

// Maintain a single host
func statsMaintainHost(hostname string, hostaddr string) (err error) {

	// Get the stats
	var stats map[string][]AppLBStat
	stats, err = watcherGetStats(hostaddr)
	if err != nil {
		return
	}

	// Update the stats
	statsLock.Lock()
	uAddStats(hostname, hostaddr, stats)
	statsLock.Lock()

	// Done
	return

}
