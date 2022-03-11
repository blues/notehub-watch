// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
const secs1Day = (60 * 60 * 24)

var statsMaintainNow *Event
var statsLock sync.Mutex
var stats map[string]HostStats

// Trace
const addStatsTrace = false

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
	stats = make(map[string]HostStats)

	for _, host := range Config.MonitoredHosts {
		if !host.Disabled {
			hs, err := readFileLocally(host.Name, todayTime())
			if err == nil {
				added := uAddStats(host.Name, host.Addr, hs.Stats)
				if added > 0 {
					fmt.Printf("stats: loaded %d stats for %s from today\n", added, host.Name)
				}
			}
			hs, err = readFileLocally(host.Name, yesterdayTime())
			if err == nil {
				added := uAddStats(host.Name, host.Addr, hs.Stats)
				if added > 0 {
					fmt.Printf("stats: loaded %d stats for %s from yesterday\n", added, host.Name)
				}
			}
		}
	}
	statsLock.Unlock()

}

// Add stats to the in-memory vector of stats.
func uAddStats(hostname string, hostaddr string, s map[string][]AppLBStat) (added int) {

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
		for _, snew := range sis {
			i := (mostRecentTime - snew.SnapshotTaken) / 60 / bucketMins
			if hs.Stats[siid][i].Started == snew.Started {
				if addStatsTrace {
					fmt.Printf("skipping %s entry %d\n", siid, i)
				}
			} else {
				if addStatsTrace {
					fmt.Printf("overwriting %s entry %d\n", siid, i)
				}
				hs.Stats[siid][i] = snew
				added++
			}
		}
	}

	// Update the main stats
	stats[hostname] = hs
	return

}

// Extract stats for the given host for a time range
func uExtractStats(hostname string, beginTime int64, duration int64) (hsret HostStats) {

	// Initialize host stats
	hs := stats[hostname]
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
	stats, err = watcherGetStats(hostaddr)
	if err != nil {
		return
	}

	// Update the stats in-memory
	statsLock.Lock()
	added := uAddStats(hostname, hostaddr, stats)
	statsLock.Unlock()
	if added > 0 {
		fmt.Printf("stats: added %d stats for %s\n", added, hostname)
	}

	// Update the stats for yesterday and today into the file system
	contents, err := writeFileLocally(hostname, todayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, todayTime()), err)
	} else {
		err = writeFileToS3(statsFilename(hostname, todayTime()), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, todayTime()), err)
		}
	}
	contents, err = writeFileLocally(hostname, yesterdayTime(), secs1Day)
	if err != nil {
		fmt.Printf("stats: error writing %s: %s\n", statsFilename(hostname, yesterdayTime()), err)
	} else {
		err = writeFileToS3(statsFilename(hostname, yesterdayTime()), contents)
		if err != nil {
			fmt.Printf("stats: error uploading %s to S3: %s\n", statsFilename(hostname, yesterdayTime()), err)
		}
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
	statsLock.Lock()
	hs := uExtractStats(hostname, beginTime, duration)
	statsLock.Unlock()
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

// Write a file to S3
func writeFileToS3(filename string, contents []byte) (err error) {

	var sess *session.Session
	sess, err = session.NewSession(
		&aws.Config{
			Region: aws.String(Config.AWSRegion),
			Credentials: credentials.NewStaticCredentials(
				Config.AWSAccessKeyID,
				Config.AWSAccessKey,
				"",
			),
		})
	if err != nil {
		return
	}

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(Config.AWSBucket),
		ACL:    aws.String("public-read"),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(contents),
	})

	return
}
