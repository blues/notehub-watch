// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

//
// notehub/http-ping.go
//

// PingBody is the structure wrapping the ping request structure
type PingBody struct {
	Body PingRequest `json:"body,omitempty"`
}

// PingRequest is the structure returned to the caller
type PingRequest struct {
	Protocol        string        `json:"protocol,omitempty"`
	ClientIP        string        `json:"client,omitempty"`
	ServerIP        string        `json:"server,omitempty"`
	InstanceID      string        `json:"instance,omitempty"`
	Time            string        `json:"time,omitempty"`
	HeapSize        uint64        `json:"heap_size,omitempty"`
	HeapFree        uint64        `json:"heap_free,omitempty"`
	HeapUsed        uint64        `json:"heap_used,omitempty"`
	HeapCount       uint64        `json:"heap_count,omitempty"`
	GoroutineStatus string        `json:"status_goroutine,omitempty"`
	HeapStatus      string        `json:"status_heap,omitempty"`
	LBStatus        *[]AppLBStat  `json:"status_lb,omitempty"`
	AppHandlers     *[]AppHandler `json:"handlers,omitempty"`
}

//
// hublib/app.go
//

type AppHandler struct {
	NodeID     string `json:"node_id"`
	DataCenter string `json:"datacenter"`
	Ipv4       string `json:"ipv4"`
	TCPPort    int    `json:"tcp_port"`
	TCPSPort   int    `json:"tcps_port"`
	HTTPPort   int    `json:"http_port"`
	HTTPSPort  int    `json:"https_port"`
	PublicIpv4 string `json:"public_ipv4"`
	PublicPath string `json:"public_path"`
	LoadLevel  int    `json:"load_level"`
}

//
// hublib/applb.go
//

// A handler statistic
type AppLBHandler struct {
	DeviceUID      string `json:"device,omitempty"`
	AppUID         string `json:"app,omitempty"`
	Discovery      bool   `json:"discovery,omitempty"`
	Continuous     bool   `json:"continuous,omitempty"`
	Notification   bool   `json:"notification,omitempty"`
	EventsEnqueued int    `json:"events_enqueued,omitempty"`
	EventsDequeued int    `json:"events_dequeued,omitempty"`
	EventsRouted   int    `json:"events_routed,omitempty"`
}

// A database statistic
type AppLBDatabase struct {
	Reads  int `json:"reads,omitempty"`
	Writes int `json:"writes,omitempty"`
}

// A cache statistic
type AppLBCache struct {
	Invalidations int `json:"invalidations,omitempty"`
	Entries       int `json:"entries,omitempty"`
	EntriesHWM    int `json:"hwm,omitempty"`
}

// AppLBStat is the data structure of a single running statistics batch
type AppLBStat struct {
	Started              int64                    `json:"started,omitempty"`
	BucketMins           int                      `json:"minutes,omitempty"`
	SnapshotTaken        int64                    `json:"when,omitempty"`
	DiscoveryHandlers    int                      `json:"handlers_discovery,omitempty"`
	EphemeralHandlers    int                      `json:"handlers_ephemeral,omitempty"`
	ContinuousHandlers   int                      `json:"handlers_continuous,omitempty"`
	NotificationHandlers int                      `json:"handlers_notification,omitempty"`
	EventsEnqueued       int                      `json:"events_enqueued,omitempty"`
	EventsDequeued       int                      `json:"events_dequeued,omitempty"`
	EventsRouted         int                      `json:"events_routed,omitempty"`
	Handlers             map[string]AppLBHandler  `json:"handlers,omitempty"`
	Databases            map[string]AppLBDatabase `json:"databases,omitempty"`
	Caches               map[string]AppLBCache    `json:"caches,omitempty"`
	Authorizations       map[string]int           `json:"authorizations,omitempty"`
	Fatals               map[string]int           `json:"fatals,omitempty"`
}
