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

// Current "live" info
type serviceSummary struct {
	Started              int64
	ServiceInstances     int64
	ContinuousHandlers   int64
	NotificationHandlers int64
	EphemeralHandlers    int64
	DiscoveryHandlers    int64
}

// Generate a sheet for this host
func sheetGetHostStats(hostaddr string) (response string) {

	// Get the list of service instances on the host
	serviceInstanceIDs, serviceInstanceAddrs, serviceNames, err := watcherGetServiceInstances(hostaddr)
	if err != nil {
		return err.Error()
	}

	// Create a new spreadsheet
	f := excelize.NewFile()

	// Generate a page within the sheet for each service instance
	ss := serviceSummary{}
	sheetNums := map[string]int{}
	for i := range serviceInstanceAddrs {

		// Generate the sheet name
		ht := serviceNames[i]
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
		errstr := sheetAddTab(f, &ss, sheetName, serviceInstanceAddrs[i], serviceInstanceIDs[i])
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
	estFmt := stime.In(est).Format("Mon Jan 02 15:04 MST")
	utcFmt := stime.Format("2006-01-02T15:04:05Z")
	response += "```"
	response += fmt.Sprintf("      host: %s\n", hostCleaned)
	response += fmt.Sprintf("   started: %s (%s)\n", estFmt, utcFmt)
	response += fmt.Sprintf("    uptime: %s\n", uptimeStr(ss.Started, time.Now().UTC().Unix()))
	response += fmt.Sprintf("     nodes: %d\n", ss.ServiceInstances)
	response += fmt.Sprintf("  handlers: %d (continuous:%d notification:%d ephemeral:%d discovery:%d)\n",
		ss.ContinuousHandlers+ss.NotificationHandlers+ss.EphemeralHandlers+ss.DiscoveryHandlers,
		ss.ContinuousHandlers, ss.NotificationHandlers, ss.EphemeralHandlers, ss.DiscoveryHandlers)
	response += fmt.Sprintf("  download: *_<%s%s%s|%s>_*", Config.HostURL, sheetRoute, filename, filename)
	response += "```"
	return

}

// Add the stats for a service instance as a tabbed sheet within the xlsx
func sheetAddTab(f *excelize.File, ss *serviceSummary, sheetName string, addr string, siid string) (errstr string) {

	// Get the info from the handler
	var pb PingBody
	pb, err := getServiceInstanceInfo(addr, siid, "lb")
	if err != nil {
		errstr = err.Error()
		return
	}
	if pb.Body.LBStatus == nil || len(*pb.Body.LBStatus) == 0 {
		return "no data available from handler"
	}

	// Update service summary
	ss.ServiceInstances++
	ss.Started = (*pb.Body.LBStatus)[0].Started
	ss.ContinuousHandlers += (*pb.Body.LBStatus)[0].ContinuousHandlersActivated -
		(*pb.Body.LBStatus)[0].ContinuousHandlersDeactivated
	ss.NotificationHandlers += (*pb.Body.LBStatus)[0].NotificationHandlersActivated -
		(*pb.Body.LBStatus)[0].NotificationHandlersDeactivated
	ss.EphemeralHandlers += (*pb.Body.LBStatus)[0].EphemeralHandlersActivated -
		(*pb.Body.LBStatus)[0].EphemeralHandlersDeactivated
	ss.DiscoveryHandlers += (*pb.Body.LBStatus)[0].DiscoveryHandlersActivated -
		(*pb.Body.LBStatus)[0].DiscoveryHandlersDeactivated

	// Generate the sheet
	f.NewSheet(sheetName)

	// Generate styles
	styleBold, _ := f.NewStyle(`{"font":{"bold":true,"italic":false}}`)
	styleBoldItalic, _ := f.NewStyle(`{"font":{"bold":true,"italic":true}}`)

	// Base for dynamic info
	col := 2
	row := 2

	// Title banner
	f.SetCellValue(sheetName, cell(col, row), "Node SIID")
	f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
	f.SetCellValue(sheetName, cell(col+1, row), siid)
	row++
	row++

	// Generate aggregate info if enough are available to convert absolute to relative - that is,
	// [0] is the 'current stats', and all the rest are absolute.  In order to produce 1 bucket
	// of good 'relative' data, we need to subtract it from the one beyond it.
	if len(*pb.Body.LBStatus) > 2 {

		// Extract all available stats, and convert them from absolute to per-bucket relative.
		stats := ConvertStatsFromAbsoluteToRelative(
			(*pb.Body.LBStatus)[0].Started,
			(*pb.Body.LBStatus)[0].BucketMins,
			(*pb.Body.LBStatus)[1:])

		// Number of buckets to process
		bucketMins := int((*pb.Body.LBStatus)[0].BucketMins)
		buckets := len(stats)

		// OS stats
		f.SetCellValue(sheetName, cell(col, row), "OS (MiB)")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "mfree")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemFree/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "mtotal")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSMemTotal/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "diskrd")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskRead/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "diskwr")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSDiskWrite/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "netrcv")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetReceived/(1024*1024))
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "netsnd")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.OSNetSent/(1024*1024))
		}
		row++

		row++

		// Handler stats
		f.SetCellValue(sheetName, cell(col, row), "Handlers")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "contin")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.ContinuousHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "notif")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.NotificationHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "ephem")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EphemeralHandlersActivated)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "disco")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.DiscoveryHandlersActivated)
		}
		row++

		row++

		// Event stats
		f.SetCellValue(sheetName, cell(col, row), "Events")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
		timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
		row++

		f.SetCellValue(sheetName, cell(col, row), "queued")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsEnqueued)
		}
		row++

		f.SetCellValue(sheetName, cell(col, row), "routed")
		for i, stat := range stats {
			f.SetCellValue(sheetName, cell(col+1+i, row), stat.EventsRouted)
		}
		row++

		row++

		// Fatals stats
		if len(stats[0].Fatals) > 0 {

			f.SetCellValue(sheetName, cell(col, row), "Fatals")
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			for k := range stats[0].Fatals {

				f.SetCellValue(sheetName, cell(col, row), k)
				f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBold)
				for i, stat := range stats {
					f.SetCellValue(sheetName, cell(col+1+i, row), stat.Fatals[k])
				}
				row++

			}

			row++

		}

		// Cache stats
		f.SetCellValue(sheetName, cell(col, row), "Caches")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
		row++

		for k := range stats[0].Caches {
			row++

			f.SetCellValue(sheetName, cell(col, row), k+" cache")
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBold)
			row++
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			f.SetCellValue(sheetName, cell(col, row), "refreshed")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Invalidations)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "entries")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].Entries)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "entriesHWM")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Caches[k].EntriesHWM)
			}
			row++

		}

		row++

		// Database stats
		f.SetCellValue(sheetName, cell(col, row), "Databases")
		f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
		row++

		for k := range stats[0].Databases {
			row++

			f.SetCellValue(sheetName, cell(col, row), k)
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBold)
			row++
			timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
			row++

			f.SetCellValue(sheetName, cell(col, row), "reads")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Reads)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "writes")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].Writes)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "readMs")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].ReadMs)
			}
			row++

			f.SetCellValue(sheetName, cell(col, row), "writeMs")
			for i, stat := range stats {
				f.SetCellValue(sheetName, cell(col+1+i, row), stat.Databases[k].WriteMs)
			}
			row++

		}

		row++

		// API stats
		if len(stats[0].API) > 0 {

			f.SetCellValue(sheetName, cell(col, row), "API")
			f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBoldItalic)
			row++

			for k := range stats[0].API {
				row++

				f.SetCellValue(sheetName, cell(col, row), k)
				f.SetCellStyle(sheetName, cell(col, row), cell(col, row), styleBold)
				row++
				timeHeader(f, sheetName, col+1, row, bucketMins, buckets)
				row++

				for i, stat := range stats {
					f.SetCellValue(sheetName, cell(col+1+i, row), stat.API[k])
				}

			}

			row++

		}

	}

	// Done
	return
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
