// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// The route to our sheet handler
const sheetRoute = "/sheet/"

// Handler to retrieve a sheet
func inboundWebSheetHandler(w http.ResponseWriter, r *http.Request) {

	// Open the file
	filename := r.RequestURI[len(sheetRoute):]
	file := configDataDirectory + "/" + filename
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("%s", err), http.StatusNotFound)
		return
	}

	// Write the file to the HTTPS client as binary, with its original filename
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(contents)))

	// Copy the file to output
	io.Copy(w, bytes.NewReader(contents))

}

// Generate a sheet for this host
func sheetGetHostStats(host string) (response string) {

	// Get the list of handlers on the host
	handlerNodeIDs, handlerAddrs, handlerTypes, errstr := watcherGetHandlers(host)
	if errstr != "" {
		return errstr
	}

	// Create a new spreadsheet
	f := excelize.NewFile()

	// Generate a page within the sheet for each handler node
	sheetNums := map[string]int{}
	for i := range handlerAddrs {

		// Generate the sheet name
		ht := handlerTypes[i]
		sn := sheetNums[ht]
		sn++
		sheetNums[ht] = sn
		var sheetName string
		switch ht {
		case DcServiceNameNoteDiscovery:
			sheetName = fmt.Sprintf("Discovery #%d", sn)
		case DcServiceNameNoteboard:
			sheetName = fmt.Sprintf("Noteboard #%d", sn)
		case DcServiceNameNotehandlerTCP:
			sheetName = fmt.Sprintf("Handler #%d", sn)
		default:
			sheetName = fmt.Sprintf("%s #%d", ht, sn)
		}

		// Generate the sheet for this handler
		errstr = sheetAddNode(f, sheetName, handlerAddrs[i], handlerNodeIDs[i])
		if errstr != "" {
			response = errstr
			return
		}

	}

	// Delete the default sheet
	f.DeleteSheet("Sheet1")

	// Save the spreadsheet to a temp file
	hostCleaned := strings.TrimSuffix(host, ".blues.tools")
	hostCleaned = strings.TrimPrefix(hostCleaned, "api.")
	hostCleaned = strings.TrimPrefix(hostCleaned, "a.")
	hostCleaned = strings.TrimPrefix(hostCleaned, "i.")
	if hostCleaned == "notefile.net" {
		hostCleaned = "prod"
	}
	filename := fmt.Sprintf("%s-%s.xlsx", hostCleaned, time.Now().UTC().Format("20060102-150405"))
	err := f.SaveAs(configDataDirectory + "/" + filename)
	if err != nil {
		return fmt.Sprintf("%s", err)
	}

	// Done
	response = fmt.Sprintf("[%s](%s%s/%s)", filename, Config.HostURL, sheetRoute, filename)
	return

}

// Add the stats for a node as a sheet within the spreadsheet file
func sheetAddNode(f *excelize.File, sheetName string, addr string, nodeID string) (errstr string) {

	// Get the info from the handler
	var pb PingBody
	pb, errstr = getHandlerInfo(addr, nodeID, "lb")
	if errstr != "" {
		return
	}
	if pb.Body.LBStatus == nil || len(*pb.Body.LBStatus) == 0 {
		return "no data available from handler"
	}

	// Generate the sheet
	f.NewSheet(sheetName)

	// Node ID
	f.SetCellValue(sheetName, "B2", "Node")
	f.SetCellValue(sheetName, "C2", nodeID)

	// Uptime
	uptimeSecs := time.Now().Unix() - (*pb.Body.LBStatus)[0].Started
	uptimeDays := uptimeSecs / (24 * 60 * 60)
	uptimeSecs -= uptimeDays * (24 * 60 * 60)
	uptimeHours := uptimeSecs / (60 * 60)
	uptimeSecs -= uptimeHours * (60 * 60)
	uptimeMins := uptimeSecs / 60
	uptimeSecs -= uptimeMins * 60
	uptimeStr := fmt.Sprintf("%dd:%dh:%dm", uptimeDays, uptimeHours, uptimeMins)
	f.SetCellValue(sheetName, "B3", "Uptime")
	f.SetCellValue(sheetName, "C3", uptimeStr)

	// Handlers
	continuousActive := (*pb.Body.LBStatus)[0].ContinuousHandlersActivated -
		(*pb.Body.LBStatus)[0].ContinuousHandlersDeactivated
	notificationActive := (*pb.Body.LBStatus)[0].NotificationHandlersActivated -
		(*pb.Body.LBStatus)[0].NotificationHandlersDeactivated
	ephemeralActive := (*pb.Body.LBStatus)[0].EphemeralHandlersActivated -
		(*pb.Body.LBStatus)[0].EphemeralHandlersDeactivated
	discoveryActive := (*pb.Body.LBStatus)[0].DiscoveryHandlersActivated -
		(*pb.Body.LBStatus)[0].DiscoveryHandlersDeactivated
	totalActive := continuousActive + notificationActive + ephemeralActive + discoveryActive
	f.SetCellValue(sheetName, "B5", "Handlers")
	f.SetCellValue(sheetName, "C5", totalActive)
	f.SetCellValue(sheetName, "D6", "continuous")
	f.SetCellValue(sheetName, "C6", continuousActive)
	f.SetCellValue(sheetName, "D7", "notification")
	f.SetCellValue(sheetName, "C7", notificationActive)
	f.SetCellValue(sheetName, "D8", "ephemeral")
	f.SetCellValue(sheetName, "C8", ephemeralActive)
	f.SetCellValue(sheetName, "D9", "discovery")
	f.SetCellValue(sheetName, "C9", discoveryActive)

	// Base for dynamic info
	col := 1
	row := 11

	// Generate aggregate info if available
	if len(*pb.Body.LBStatus) >= 2 {

		// Extract all available stats, and convert them from absolute to
		// per-bucket relative
		stats := absoluteToRelative((*pb.Body.LBStatus)[1:])

		// Limit the number of buckets because of slack UI block width
		buckets := len(stats)
		if slackUsingBlocksForResponses() && buckets > 10 {
			buckets = 10
		}
		bucketMins := int((*pb.Body.LBStatus)[0].BucketMins)

		// OS stats
		f.SetCellValue(sheetName, cell(col, row), "OS (MiB)")
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "mfree")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemFree/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "mtotal")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemTotal/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "diskrd")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskRead/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "diskwr")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskWrite/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "netrcv")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetReceived/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "netsnd")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetSent/(1024*1024))
		}
		row++

		row++

		// Fatals stats
		if len(stats[0].Fatals) > 0 {

			f.SetCellValue(sheetName, cell(col, row), "Fatals")
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			for k := range stats[0].Fatals {

				f.SetCellValue(sheetName, cell(col, row), k)
				for i, stat := range stats {
					if i >= buckets {
						break
					}
					f.SetCellValue(sheetName, cell(col+1+i, row), stat.Fatals[k])
				}
				row++

			}

			row++

		}

		// Cache stats
		f.SetCellValue(sheetName, cell(col, row), "Caches")
		row++

		for k := range stats[0].Caches {

			f.SetCellValue(sheetName, cell(col, row), k+" cache")
			row++
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			f.SetCellValue(sheetName, cell(col, row), "refresh")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Invalidations)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "entries")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Entries)
			}
			row++

		}

		row++

		// Handler stats
		f.SetCellValue(sheetName, cell(col, row), "Handlers")
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "contin")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.ContinuousHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "notif")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.NotificationHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "ephem")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EphemeralHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "disco")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.DiscoveryHandlersActivated)
		}
		row++

		row++

		// Event stats
		f.SetCellValue(sheetName, cell(col, row), "Events")
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "queued")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsEnqueued)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "routed")
		for i, stat := range stats {
			if i >= buckets {
				break
			}
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsRouted)
		}
		row++

		row++

		// Database stats
		f.SetCellValue(sheetName, cell(col, row), "Databases")
		row++

		for k := range stats[0].Databases {

			f.SetCellValue(sheetName, cell(col, row), k)
			row++
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			f.SetCellValue(sheetName, cell(col, row), "reads")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Reads)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "writes")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Writes)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "readMs")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].ReadMs)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "writeMs")
			for i, stat := range stats {
				if i >= buckets {
					break
				}
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].WriteMs)
			}
			row++

		}

		row++

		// API stats
		if len(stats[0].API) > 0 {

			f.SetCellValue(sheetName, cell(col, row), "API")
			row++

			for k := range stats[0].API {

				f.SetCellValue(sheetName, cell(col, row), k)
				row++
				timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
				row++

				for i, stat := range stats {
					if i >= buckets {
						break
					}
					f.SetCellValue(sheetName, cell(col+1+i, row), stat.API[k])
				}

			}

			row++

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

// Get the cell address for a given 1-based coordinate
func cell(col int, row int) string {
	cell, _ := excelize.CoordinatesToCellName(col, row)
	return cell
}

// Generate a time header at the specified col/row
func timeHeader(f *excelize.File, sheetName string, col int, row int, bucketMins int, buckets int) {
	for i := 0; i < buckets; i++ {
		f.SetCellValue(sheetName, cell(col+i, row), fmt.Sprintf("%dm", (i+1)*bucketMins))
	}
}
