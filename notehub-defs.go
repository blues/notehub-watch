// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

//
// hublib/dc.go
//

// DcServiceNameNoteDiscovery is the name of the service registered with consul for notehub discovery
const DcServiceNameNoteDiscovery = "notediscovery"

// DcServiceNameNoteboard is the name of the service registered with consul for the noteboard HTTP task
const DcServiceNameNoteboard = "noteboard-http"

// DcServiceNameNotehandlerTCP is the name of the service registered with consul for notehub handler on the tcp port
const DcServiceNameNotehandlerTCP = "notehandler-tcp"

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
	NodeID         string `json:"node_id,omitempty"`
	DataCenter     string `json:"datacenter,omitempty"`
	Ipv4           string `json:"ipv4,omitempty"`
	TCPPort        int    `json:"tcp_port,omitempty"`
	TCPSPort       int    `json:"tcps_port,omitempty"`
	HTTPPort       int    `json:"http_port,omitempty"`
	HTTPSPort      int    `json:"https_port,omitempty"`
	PublicIpv4     string `json:"public_ipv4,omitempty"`
	PublicPath     string `json:"public_path,omitempty"`
	LoadLevel      int    `json:"load_level,omitempty"`
	PrimaryService string `json:"primary_service,omitempty"`
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
	EventsEnqueued int64  `json:"events_enqueued,omitempty"`
	EventsDequeued int64  `json:"events_dequeued,omitempty"`
	EventsRouted   int64  `json:"events_routed,omitempty"`
}

// A database statistic
type AppLBDatabase struct {
	Reads      int64 `json:"reads,omitempty"`
	ReadMs     int64 `json:"read_ms,omitempty"`
	ReadMsMax  int64 `json:"read_ms_max,omitempty"`
	Writes     int64 `json:"writes,omitempty"`
	WriteMs    int64 `json:"write_ms,omitempty"`
	WriteMsMax int64 `json:"write_ms_max,omitempty"`
}

// A cache statistic
type AppLBCache struct {
	Invalidations int64 `json:"invalidations,omitempty"`
	Entries       int64 `json:"entries,omitempty"`
	EntriesHWM    int64 `json:"hwm,omitempty"`
}

// AppLBStat is the data structure of a single running statistics batch
type AppLBStat struct {
	Started                         int64                    `json:"started,omitempty"`
	BucketMins                      int64                    `json:"minutes,omitempty"`
	SnapshotTaken                   int64                    `json:"when,omitempty"`
	OSMemTotal                      uint64                   `json:"mem_total,omitempty"`
	OSMemFree                       uint64                   `json:"mem_free,omitempty"`
	OSDiskRead                      uint64                   `json:"disk_read,omitempty"`
	OSDiskWrite                     uint64                   `json:"disk_write,omitempty"`
	OSNetReceived                   uint64                   `json:"net_received,omitempty"`
	OSNetSent                       uint64                   `json:"net_sent,omitempty"`
	DiscoveryHandlersActivated      int64                    `json:"handlers_discovery_activated,omitempty"`
	EphemeralHandlersActivated      int64                    `json:"handlers_ephemeral_activated,omitempty"`
	ContinuousHandlersActivated     int64                    `json:"handlers_continuous_activated,omitempty"`
	NotificationHandlersActivated   int64                    `json:"handlers_notification_activated,omitempty"`
	DiscoveryHandlersDeactivated    int64                    `json:"handlers_discovery_deactivated,omitempty"`
	EphemeralHandlersDeactivated    int64                    `json:"handlers_ephemeral_deactivated,omitempty"`
	ContinuousHandlersDeactivated   int64                    `json:"handlers_continuous_deactivated,omitempty"`
	NotificationHandlersDeactivated int64                    `json:"handlers_notification_deactivated,omitempty"`
	EventsEnqueued                  int64                    `json:"events_enqueued,omitempty"`
	EventsDequeued                  int64                    `json:"events_dequeued,omitempty"`
	EventsRouted                    int64                    `json:"events_routed,omitempty"`
	Handlers                        map[string]AppLBHandler  `json:"handlers,omitempty"`
	Databases                       map[string]AppLBDatabase `json:"databases,omitempty"`
	Caches                          map[string]AppLBCache    `json:"caches,omitempty"`
	API                             map[string]int64         `json:"api,omitempty"`
	Fatals                          map[string]int64         `json:"fatals,omitempty"`
}
