// Copyright 2020 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"net/http"
	"net/url"
	"path"
)

// Root handler
func inboundWebRootHandler(httpRsp http.ResponseWriter, httpReq *http.Request) {

	// Process the request URI, looking for things that will indicate "dev"
	method := httpReq.Method
	if method == "" {
		method = "GET"
	}

	// Get the target
	parsedURL, _ := url.Parse(httpReq.RequestURI)
	target := path.Base(parsedURL.Path)

	// Exit if just the favicon
	if target == "favicon.ico" {
		return
	}

	// Done
	httpRsp.Write([]byte("I'm watching you."))

}
