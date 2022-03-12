// Copyright 2022 Blues Inc.  All rights reserved.
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
const sheetRoute = "/file/"

// Handler to retrieve a sheet
func inboundWebSheetHandler(w http.ResponseWriter, r *http.Request) {

	// Open the file
	filename := r.RequestURI[len(sheetRoute):]
	file := configDataDirectory + filename
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
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
func sheetGetHostStats(hostname string, hostaddr string) (response string) {

	// Get the most recent stats
	ss, stats1h, err := watcherGetStats(hostaddr)
	if err != nil {
		return err.Error()
	}

	// Update the stats in-memory
	statsAdd(hostname, hostaddr, stats1h)

	// Get the entire set of stats available in-memory
	hs, exists := statsExtract(hostname, 0, 0)
	if !exists {
		response = fmt.Sprintf("unknown host: %s", hostname)
	}

	// Create a new spreadsheet
	f := excelize.NewFile()

	// Generate a page within the sheet for each service instance
	sheetNums := map[string]int{}
	for siid, stats48h := range hs.Stats {

		// Generate the sheet name
		s := strings.Split(siid, ":")
		ht := "unknown-service-type"
		if len(s) == 2 {
			ht = s[1]
		}
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

		// Generate the sheet for this service instance
		errstr := sheetAddTab(f, sheetName, siid, stats48h)
		if errstr != "" {
			response = errstr
			return
		}

	}

	// Delete the default sheet
	f.DeleteSheet("Sheet1")

	// Save the spreadsheet to a temp file
	hostCleaned := strings.TrimSuffix(hostaddr, ".blues.tools")
	hostCleaned = strings.TrimPrefix(hostCleaned, "api.")
	hostCleaned = strings.TrimPrefix(hostCleaned, "a.")
	hostCleaned = strings.TrimPrefix(hostCleaned, "i.")
	if hostCleaned == "notefile.net" {
		hostCleaned = "prod"
	}
	filename := fmt.Sprintf("%s-%s.xlsx", hostCleaned, time.Now().UTC().Format("20060102-150405"))
	err = f.SaveAs(configDataDirectory + filename)
	if err != nil {
		return err.Error()
	}

	// Generate response
	stime := time.Unix(ss.Started, 0).UTC()
	est, _ := time.LoadLocation("EST")
	estFmt := stime.In(est).Format("Mon Jan 02 15:04PM MST")
	utcFmt := stime.Format("2006-01-02T15:04:05Z")
	response += "```"
	response += fmt.Sprintf("      host: %s\n", hostCleaned)
	response += fmt.Sprintf("   started: %s (%s)\n", estFmt, utcFmt)
	response += fmt.Sprintf("    uptime: %s\n", uptimeStr(ss.Started, time.Now().UTC().Unix()))
	response += fmt.Sprintf("     nodes: %d\n", len(ss.ServiceInstanceIDs))
	response += fmt.Sprintf("  handlers: %d (continuous:%d notification:%d ephemeral:%d discovery:%d)\n",
		ss.ContinuousHandlers+ss.NotificationHandlers+ss.EphemeralHandlers+ss.DiscoveryHandlers,
		ss.ContinuousHandlers, ss.NotificationHandlers, ss.EphemeralHandlers, ss.DiscoveryHandlers)
	response += "```" + "\n"
	response += fmt.Sprintf("<%s%s%s|%s>", Config.HostURL, sheetRoute, filename, filename)
	return

}

// Add the stats for a service instance as a tabbed sheet within the xlsx
func sheetAddTab(f *excelize.File, sheetName string, siid string, stats []AppLBStat) (errstr string) {

	// Generate the sheet
	f.NewSheet(sheetName)

	// Generate styles
	styleMetric, _ := f.NewStyle(`{"font":{"color":"00007f"}}`)
	styleCategory, _ := f.NewStyle(`{"font":{"color":"ff0000","bold":true,"italic":true}}`)
	styleSubcategory, _ := f.NewStyle(`{"font":{"color":"007f00","bold":true,"italic":false}}`)
	styleRightAligned, _ := f.NewStyle(`{"alignment":{"horizontal":"right"}}`)

	// Base for dynamic info
	row := 2
	col := 2
	colname, _ := excelize.ColumnNumberToName(col)
	f.SetColWidth(sheetName, colname, colname, 13)

	// Title banner
	f.SetCellValue(sheetName, cell(col, row), "Node SIID")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	f.SetCellValue(sheetName, cell(col+1, row), siid)
	row++
	row++

	// Exit if no stats
	if len(stats) == 0 {
		return
	}

	// Bucket parameters are assumed to be uniform
	buckets := len(stats)
	bucketMins := int(stats[0].BucketMins)

	// OS stats
	f.SetCellValue(sheetName, cell(col, row), "OS (MiB)")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
	row++

	f.SetCellValue(sheetName, cell(col, row), "sampled UTC")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		if stat.SnapshotTaken != 0 {
			f.SetCellValue(sheetName, cell(col+1+i, row), time.Unix(stat.SnapshotTaken, 0))
			colname, _ := excelize.ColumnNumberToName(col + 1 + i)
			f.SetColWidth(sheetName, colname, colname, 13)
		}
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "boot EST")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		if stat.Started != 0 {
			stime := time.Unix(stat.Started, 0).UTC()
			est, _ := time.LoadLocation("EST")
			estFmt := stime.In(est).Format("Mon Jan 02")
			f.SetCellValue(sheetName, cell(col+1+i, row), estFmt)
			f.SetCellStyle(sheetName, cell(col+1+i, row), cell(col+1+i, row), styleRightAligned)
		}
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "uptime")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		if stat.Started != 0 {
			f.SetCellValue(sheetName, cell(col+1+i, row), uptimeStr(stat.Started, stat.SnapshotTaken))
			f.SetCellStyle(sheetName, cell(col+1+i, row), cell(col+1+i, row), styleRightAligned)
		}
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "mfree")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemFree/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "mtotal")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemTotal/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "diskrd")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskRead/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "diskwr")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskWrite/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "netrcv")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetReceived/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "netsnd")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetSent/(1024*1024))
	}
	row++

	row++

	// Handler stats
	f.SetCellValue(sheetName, cell(col, row), "Handlers")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
	row++

	f.SetCellValue(sheetName, cell(col, row), "contin")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.ContinuousHandlersActivated)
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "notif")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.NotificationHandlersActivated)
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "ephem")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.EphemeralHandlersActivated)
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "disco")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.DiscoveryHandlersActivated)
	}
	row++

	row++

	// Event stats
	f.SetCellValue(sheetName, cell(col, row), "Events")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
	row++

	f.SetCellValue(sheetName, cell(col, row), "queued")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsEnqueued)
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "routed")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsRouted)
	}
	row++

	row++

	// Fatals stats
	if len(stats[0].Fatals) > 0 {

		f.SetCellValue(sheetName, cell(col, row), "Fatals")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		for k := range stats[0].Fatals {

			f.SetCellValue(sheetName, cell(col, row), k)
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Fatals[k])
			}
			row++

		}

		row++

	}

	// Cache stats
	f.SetCellValue(sheetName, cell(col, row), "Caches")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	row++

	for k := range stats[0].Caches {
		row++

		f.SetCellValue(sheetName, cell(col, row), k+" cache")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "refreshed")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Invalidations)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "entries")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Entries)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "entriesHWM")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].EntriesHWM)
		}
		row++

	}

	row++

	// Database stats
	f.SetCellValue(sheetName, cell(col, row), "Databases")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	row++

	for k := range stats[0].Databases {
		row++

		f.SetCellValue(sheetName, cell(col, row), k)
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
		row++
		f.SetCellValue(sheetName, cell(col, row), "database")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "reads")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Reads)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "writes")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Writes)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "readMs")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].ReadMs)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "writeMs")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].WriteMs)
		}
		row++

	}

	row++

	// API stats
	if len(stats[0].API) > 0 {

		f.SetCellValue(sheetName, cell(col, row), "API")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		row++

		for k := range stats[0].API {
			row++

			f.SetCellValue(sheetName, cell(col, row), k)
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
			row++
			f.SetCellValue(sheetName, cell(col, row), "api")
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			f.SetCellValue(sheetName, cell(col, row), "calls")
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.API[k])
			}

		}

		row++

	}

	// Done
	return
}

// Generate an uptime string
func uptimeStr(started int64, now int64) (s string) {
	uptimeSecs := now - started
	uptimeDays := uptimeSecs / (24 * 60 * 60)
	uptimeSecs -= uptimeDays * (24 * 60 * 60)
	uptimeHours := uptimeSecs / (60 * 60)
	uptimeSecs -= uptimeHours * (60 * 60)
	uptimeMins := uptimeSecs / 60
	uptimeSecs -= uptimeMins * 60
	if uptimeDays > 0 {
		s = fmt.Sprintf("%dd %dh %dm", uptimeDays, uptimeHours, uptimeMins)
	} else if uptimeHours > 0 {
		s = fmt.Sprintf("%dh %dm", uptimeHours, uptimeMins)
	} else {
		s = fmt.Sprintf("%dm", uptimeMins)
	}
	return s
}

// Get the cell address for a given 1-based coordinate
func cell(col int, row int) string {
	cell, _ := excelize.CoordinatesToCellName(col, row)
	return cell
}

// Generate a time header at the specified col/row
func timeHeader(f *excelize.File, sheetName string, col int, row int, bucketMins int, buckets int) {
	style, _ := f.NewStyle(`{"alignment":{"horizontal":"right"},"font":{"color":"0000ff","bold":true,"italic":true}}`)
	for i := 0; i < buckets; i++ {
		f.SetCellValue(sheetName, cell(col+i, row), uptimeStr(0, (int64(i)+1)*int64(bucketMins)*60))
		f.SetCellStyle(sheetName, cell(col+i, row), cell(col+i, row), style)
	}
}
