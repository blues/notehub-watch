// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"os"
	"time"
)

// Directory that will be used for data
const configDataDirectoryBase = "/data/"

// Fully-resolved data directory
var configDataDirectory = ""

// Main service entry point
func main() {

	// Read creds
	ServiceReadConfig()

	// Compute folder location
	configDataDirectory = os.Getenv("HOME") + configDataDirectoryBase
	_ = configDataDirectory

	// Spawn the console input handler
	go inputHandler()

	// Init our web request inbound server
	go HTTPInboundHandler(":80")

	// Housekeeping
	for {
		time.Sleep(1 * time.Minute)
	}

}
