// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

// A monitored host and all data needed for it
type MonitoredHost struct {
	Disabled bool   `json:"disabled,omitempty"`
	Name     string `json:"name,omitempty"`
	Addr     string `json:"address,omitempty"`
}

// ServiceConfig is the service configuration file format
type ServiceConfig struct {

	// Host URL
	HostURL string `json:"host_url,omitempty"`

	// Monitoring period
	MonitorPeriodMins int `json:"monitor_mins,omitempty"`

	// Monitored hosts
	MonitoredHosts []MonitoredHost `json:"monitor,omitempty"`

	// Twilio "from" phone number & email (addr & name)
	TwilioSMS   string `json:"twilio_sms,omitempty"`
	TwilioEmail string `json:"twilio_email,omitempty"`
	TwilioFrom  string `json:"twilio_from,omitempty"`

	// Twilio SID and Secret access key
	TwilioSID string `json:"twilio_sid,omitempty"`
	TwilioSAK string `json:"twilio_sak,omitempty"`

	// Twilio Sendgrid API key
	TwilioSendgridAPIKey string `json:"twilio_sendgrid_api_key,omitempty"`

	// Slack app integration
	SlackWebhookURL string `json:"slack_webhook_url,omitempty"`

	// AWS info used for S3 upload
	AWSRegion      string `json:"aws_region,omitempty"`
	AWSAccessKeyID string `json:"aws_access_key_id,omitempty"`
	AWSAccessKey   string `json:"aws_access_key,omitempty"`
	AWSBucket      string `json:"aws_bucket,omitempty"`

	// Datadog creds
	DatadogSite   string `json:"datadog_site,omitempty"`
	DatadogAppKey string `json:"datadog_app_key,omitempty"`
	DatadogAPIKey string `json:"datadog_api_key,omitempty"`
}

// ConfigPath (here for golint)
const ConfigPath = "/config/config.json"

// Config is our configuration, read out of a file for security reasons
var Config ServiceConfig

// ServiceReadConfig gets the current value of the service config
func ServiceReadConfig() {

	// Read the file and unmarshall if no error
	homedir, _ := os.UserHomeDir()
	path := homedir + ConfigPath
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Printf("can't load config from %s: %s\n", path, err)
		os.Exit(-1)
	}

	err = json.Unmarshal(contents, &Config)
	if err != nil {
		fmt.Printf("Can't parse config JSON from: %s: %s\n", path, err)
		os.Exit(-1)
	}

}
