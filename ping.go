// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Ping hosts for up/down notification
func pingWatcher() {

	// Wait for a signal to update them, or a timeout
	startTimes := map[string]int64{}
	for {

		// Perform the check for every enabled host
		for _, host := range Config.MonitoredHosts {
			if !host.Disabled {
				var err error
				var req *http.Request
				var rsp *http.Response
				var rspJSON []byte
				var pb PingBody

				// Do a ping
				url := "https://" + host.Addr + "/ping"
				req, err = http.NewRequest("GET", url, nil)
				if err == nil {
					httpclient := &http.Client{
						Timeout: time.Second * time.Duration(30),
					}
					rsp, err = httpclient.Do(req)
				}
				if err == nil {
					rspJSON, err = io.ReadAll(rsp.Body)
					rsp.Body.Close()
				}
				if err == nil {
					err = json.Unmarshal(rspJSON, &pb)
				}

				// Substitute a different error
				if strings.Contains(err.Error(), "unexpected end of JSON input") {
					err = fmt.Errorf("server not responding")
				}

				// See if the start time is the same
				if err != nil {
					err = fmt.Errorf("%s: error pinging host: %s", host.Name, err)
				} else {
					prevTime := startTimes[host.Name]
					if prevTime == 0 {
						prevTime = pb.Body.Started
					}
					diffSecs := prevTime - pb.Body.Started
					if diffSecs < 0 {
						diffSecs = -diffSecs
					}
					if diffSecs > 300 {
						err = fmt.Errorf("@channel: %s was just restarted after having been active for %s",
							host.Name, uptimeStr(prevTime, pb.Body.Started))
					}
					startTimes[host.Name] = pb.Body.Started
				}

				// If an error, post it
				if err != nil {
					slackSendMessage(err.Error())
				}

			}

		}

		// Sleep
		time.Sleep(time.Duration(1) * time.Minute)

	}

}
