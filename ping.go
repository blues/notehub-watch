// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"fmt"
	"time"
)

// Ping hosts for up/down notification
func pingWatcher() {

	// Wait for a signal to update them, or a timeout
	for {

		// Get the service instances for the service, sending slack messages if anything changed
		for _, host := range Config.MonitoredHosts {
			if !host.Disabled {
				_, _, _, _, _, err := watcherGetServiceInstances(host.Name, host.Addr)
				if err != nil {
					fmt.Printf("%s: ping: %s\n", host.Name, err)
				}
			}
		}

		// Sleep
		time.Sleep(time.Duration(1) * time.Minute)

	}

}
