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
	"sort"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// Trace
const sheetTrace = true

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

// Add all the tabs for this service type
func sheetAddTabs(serviceType string, hs *HostStats, ss serviceSummary, handlers map[string]AppHandler, f *excelize.File) (response string) {
	var sn int

	if sheetTrace {
		fmt.Printf("sheetAddTabs: %s\n", serviceType)
	}

	sheetAddTab(f, "Summary", "summary", ss, AppHandler{}, statsAggregateAsLBStat(hs.Stats, hs.BucketMins*60))

	keys := make([]string, 0, len(hs.Stats))
	for key := range hs.Stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, siid := range keys {

		// Generate the sheet name
		s := strings.Split(siid, ":")
		ht := "unknown-service-type"
		if len(s) == 2 {
			ht = s[1]
		}

		// Skip if it's not what we're looking for
		if ht != serviceType {
			continue
		}

		// Bump the sheet number
		sn++

		// Generate the title
		var sheetName string
		switch ht {
		case DcServiceNameNoteDiscovery:
			sheetName = fmt.Sprintf("Discover%d", sn)
		case DcServiceNameNoteboard:
			sheetName = fmt.Sprintf("Noteboard%d", sn)
		case DcServiceNameNotehandlerTCP:
			sheetName = fmt.Sprintf("Handler%d", sn)
		default:
			sheetName = fmt.Sprintf("%s%d", ht, sn)
		}

		// Generate the sheet for this service instance
		response = sheetAddTab(f, sheetName, siid, ss, handlers[siid], hs.Stats[siid])
		if response != "" {
			break
		}

	}

	return
}

// Generate a sheet for this host
func sheetGetHostStats(hostname string, hostaddr string) (response string) {

	// Update with the most recent stats, ignoring errors
	if sheetTrace {
		fmt.Printf("sheetGetHostStats: get stats for %s\n", hostname)
	}
	ss, handlers, err := statsUpdateHost(hostname, hostaddr)
	if err != nil {
		fmt.Printf("sheetGetHostStats: error updating %s: %s\n", hostname, err)
	}

	// Get the entire set of stats available in-memory
	if sheetTrace {
		fmt.Printf("sheetGetHostStats: extract stats\n")
	}
	hs, exists := statsExtract(hostname, 0, 0)
	if !exists {
		response = fmt.Sprintf("unknown host: %s", hostname)
	}

	// Create a new spreadsheet
	f := excelize.NewFile()

	// Generate a page within the sheet for each service instance
	if response == "" {
		response = sheetAddTabs(DcServiceNameNotehandlerTCP, &hs, ss, handlers, f)
	}
	if response == "" {
		response = sheetAddTabs(DcServiceNameNoteDiscovery, &hs, ss, handlers, f)
	}
	if response == "" {
		response = sheetAddTabs(DcServiceNameNoteboard, &hs, ss, handlers, f)
	}
	if response == "" {
		response = sheetAddTabs("", &hs, ss, handlers, f)
	}
	if response != "" {
		return
	}

	// Delete the default sheet
	f.DeleteSheet("Sheet1")

	// Save the spreadsheet to a temp file
	if sheetTrace {
		fmt.Printf("sheetGetHostStats: saving sheet\n")
	}
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
	response += "```"
	response += fmt.Sprintf("      host: %s\n", hostCleaned)
	response += fmt.Sprintf("   version: %s)\n", ss.ServiceVersion)
	response += fmt.Sprintf("     nodes: %d\n", len(ss.ServiceInstanceIDs))
	response += fmt.Sprintf("  handlers: %d (continuous:%d notification:%d ephemeral:%d discovery:%d)\n",
		ss.ContinuousHandlers+ss.NotificationHandlers+ss.EphemeralHandlers+ss.DiscoveryHandlers,
		ss.ContinuousHandlers, ss.NotificationHandlers, ss.EphemeralHandlers, ss.DiscoveryHandlers)
	response += "```" + "\n"
	response += fmt.Sprintf("<%s%s%s|%s>", Config.HostURL, sheetRoute, filename, filename)

	// Done
	if sheetTrace {
		fmt.Printf("sheetGetHostStats: done\n")
	}
	return

}

// Add the stats for a service instance as a tabbed sheet within the xlsx
func sheetAddTab(f *excelize.File, sheetName string, siid string, ss serviceSummary, handler AppHandler, stats []StatsStat) (errstr string) {

	// Determine if summary sheet, for special treatment
	isSummarySheet := siid == "summary"

	// Generate the sheet
	f.NewSheet(sheetName)

	// Generate styles
	styleMetric, _ := f.NewStyle(`{"font":{"color":"00007f"}}`)
	styleCategory, _ := f.NewStyle(`{"font":{"color":"ff0000","bold":true,"italic":true}}`)
	styleSubcategory, _ := f.NewStyle(`{"font":{"color":"007f00","bold":true,"italic":false}}`)
	styleRightAligned, _ := f.NewStyle(`{"alignment":{"horizontal":"right"}}`)
	styleLeftAligned, _ := f.NewStyle(`{"alignment":{"horizontal":"left"}}`)

	// Base for dynamic info
	row := 1
	col := 1
	colname, _ := excelize.ColumnNumberToName(col)
	f.SetColWidth(sheetName, colname, colname, 13)

	// Freeze panes
	f.SetPanes(sheetName, `{"freeze":true,"x_split":1,"y_split":2,"top_left_cell":"B3","active_pane":"bottomRight","panes":[{"pane":"topLeft"},{"pane":"topRight"},{"pane":"bottomLeft"},{"active_cell":"B3", "sqref":"B3", "pane":"bottomRight"}]}`)

	// Node info
	f.SetCellValue(sheetName, cell(col, row), "Node")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	f.SetCellValue(sheetName, cell(col+1, row), siid)
	row++

	f.SetCellValue(sheetName, cell(col, row), "Version")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
	f.SetCellValue(sheetName, cell(col+1, row), ss.ServiceVersion)
	row++

	if !isSummarySheet {
		f.SetCellValue(sheetName, cell(col, row), "Node Tags")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		s := ""
		for _, t := range handler.NodeTags {
			if !strings.Contains(t, "/") {
				if s != "" {
					s += ", "
				}
				s += t
			}
		}
		f.SetCellValue(sheetName, cell(col+1, row), s)
	}
	row++

	if !isSummarySheet {
		f.SetCellValue(sheetName, cell(col, row), "Started")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		if handler.NodeStarted == 0 {
			f.SetCellValue(sheetName, cell(col+1, row), "unknown")
		} else {
			f.SetCellValue(sheetName, cell(col+1, row), time.Unix(handler.NodeStarted, 0).Format("01-02 15:04:05"))
		}
	}
	row++

	if !isSummarySheet {
		f.SetCellValue(sheetName, cell(col, row), "IPv4")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		f.SetCellValue(sheetName, cell(col+1, row), handler.Ipv4)
		f.SetCellValue(sheetName, cell(col+2, row), "tcp")
		f.SetCellStyle(sheetName, cell(col+2, row), cell(col+2, row), styleRightAligned)
		f.SetCellValue(sheetName, cell(col+3, row), handler.TCPPort)
		f.SetCellStyle(sheetName, cell(col+3, row), cell(col+3, row), styleLeftAligned)
		f.SetCellValue(sheetName, cell(col+4, row), "tcps")
		f.SetCellStyle(sheetName, cell(col+4, row), cell(col+4, row), styleRightAligned)
		f.SetCellValue(sheetName, cell(col+5, row), handler.TCPSPort)
		f.SetCellStyle(sheetName, cell(col+5, row), cell(col+5, row), styleLeftAligned)
		f.SetCellValue(sheetName, cell(col+6, row), "http")
		f.SetCellStyle(sheetName, cell(col+6, row), cell(col+6, row), styleRightAligned)
		f.SetCellValue(sheetName, cell(col+7, row), handler.HTTPPort)
		f.SetCellStyle(sheetName, cell(col+7, row), cell(col+7, row), styleLeftAligned)
		f.SetCellValue(sheetName, cell(col+8, row), "https")
		f.SetCellStyle(sheetName, cell(col+8, row), cell(col+8, row), styleRightAligned)
		f.SetCellValue(sheetName, cell(col+9, row), handler.HTTPSPort)
		f.SetCellStyle(sheetName, cell(col+9, row), cell(col+9, row), styleLeftAligned)
	}
	row++

	if !isSummarySheet {
		f.SetCellValue(sheetName, cell(col, row), "Public IPv4")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		f.SetCellValue(sheetName, cell(col+1, row), handler.PublicIpv4)
	}
	row++

	row++

	// Exit if no stats
	if len(stats) == 0 {
		return
	}

	// Bucket parameters are assumed to be uniform
	buckets := len(stats)
	bucketMins := int(ss.BucketSecs / 60)

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

	f.SetCellValue(sheetName, cell(col, row), "malloc mb")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		if stat.OSMemTotal != 0 {
			f.SetCellValue(sheetName, cell(col+1+i, row), (stat.OSMemTotal-stat.OSMemFree)/(1024*1024))
		}
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "mtotal mb")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		if stat.OSMemTotal != 0 {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemTotal/(1024*1024))
		}
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

	f.SetCellValue(sheetName, cell(col, row), "netrcv mb")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
	for i, stat := range stats {
		f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetReceived/(1024*1024))
	}
	row++

	f.SetCellValue(sheetName, cell(col, row), "netsnd mb")
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
	km := map[string]bool{}
	for _, stat := range stats {
		for k := range stat.Fatals {
			km[k] = true
		}
	}
	keys := make([]string, 0, len(km))
	for k := range km {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) > 0 {
		f.SetCellValue(sheetName, cell(col, row), "Fatals")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++
	}
	for _, k := range keys {
		f.SetCellValue(sheetName, cell(col, row), k)
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Fatals[k])
		}
		row++
	}
	if len(keys) > 0 {
		row++
	}

	// Cache stats
	km = map[string]bool{}
	for _, stat := range stats {
		for k := range stat.Caches {
			km[k] = true
		}
	}
	keys = make([]string, 0, len(km))
	for k := range km {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) > 0 {
		f.SetCellValue(sheetName, cell(col, row), "Caches")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		row++
	}
	for _, k := range keys {
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
	if len(keys) > 0 {
		row++
	}

	// API stats
	km = map[string]bool{}
	for _, stat := range stats {
		for k := range stat.API {
			km[k] = true
		}
	}
	keys = make([]string, 0, len(km))
	for k := range km {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) > 0 {
		f.SetCellValue(sheetName, cell(col, row), "API")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		row++
	}
	for _, k := range keys {
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
	if len(keys) > 0 {
		row++
	}

	// Database stats (display the ones beginning with "app" at the end)
	kmApps := map[string]bool{}
	kmNonApps := map[string]bool{}
	for _, stat := range stats {
		for k := range stat.Databases {
			if strings.HasPrefix(k, "app:") {
				kmApps[k] = true
			} else {
				kmNonApps[k] = true
			}
		}
	}
	apps := make([]string, 0, len(kmApps))
	for k := range kmApps {
		keys = append(keys, k)
	}
	sort.Strings(apps)
	nonapps := make([]string, 0, len(kmNonApps))
	for k := range kmNonApps {
		keys = append(keys, k)
	}
	sort.Strings(nonapps)
	keys = append(nonapps, apps...)

	if len(keys) > 0 {
		f.SetCellValue(sheetName, cell(col, row), "Databases")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleCategory)
		row++
	}
	for _, k := range keys {
		row++

		f.SetCellValue(sheetName, cell(col, row), k)
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleSubcategory)
		row++
		f.SetCellValue(sheetName, cell(col, row), "database")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "queries")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Reads)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "execs")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Writes)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "queryMsAvg")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].ReadMs)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "execMsAvg")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleMetric)
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].WriteMs)
		}
		row++

	}
	if len(keys) > 0 {
		row++
	}

	// Done
	return
}

// Generate an uptime string
func uptimeStr(started int64, now int64) (s string) {
	uptimeSecs := now - started
	if uptimeSecs < 0 {
		uptimeSecs = -uptimeSecs
	}
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
