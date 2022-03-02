// Copyright Blues Inc.	 All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

// Common support for all HTTP topic handlers
package main

import (
	"fmt"
	"net/http"
)

// HTTPInboundHandler kicks off inbound messages coming from all sources, then serve HTTP
func HTTPInboundHandler(port string) {

	// Topics
	http.HandleFunc("/github", inboundWebGithubHandler)
	http.HandleFunc("/ping", inboundWebPingHandler)
	http.HandleFunc("/", inboundWebRootHandler)

	// HTTP
	fmt.Printf("Now handling inbound HTTP on %s\n", port)
	go http.ListenAndServe(port, nil)

}
