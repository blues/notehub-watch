// Copyright 2020 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

// Serves Health Checks
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
)

// Canary handler
func inboundWebCanaryHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Get the body if supplied
	eventJSON, err := ioutil.ReadAll(httpReq.Body)
	if err != nil {
		eventJSON = []byte("{}")
	}

	// Determine the difference between note creation time and the time of receipt
	fmt.Printf("\n%s\n", eventJSON)

}
