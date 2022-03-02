// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/slack-go/slack"
)

// Send a message to Slack.  See:
// https://api.slack.com/reference/messaging/payload
// https://github.com/slack-go/slack
func slackSendMessage(message string) (err error) {

	payload := &slack.WebhookMessage{
		Text: message,
	}

	return slack.PostWebhook(Config.SlackWebhookURL, payload)

}

// Slack inbound 'slash command' request handler
func inboundSlackRequestHandler(w http.ResponseWriter, r *http.Request) {

	s, err := slack.SlashCommandParse(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	switch s.Command {
	case "/watcher":
		w.Write([]byte(slackCommandWatcher(s)))
	default:
		w.Write([]byte("unknown command"))
	}

	return

}

// Slack /watcher request handler
func slackCommandWatcher(s slack.SlashCommand) (response string) {

	// Register flags
	f := flag.NewFlagSet("/watcher", flag.ContinueOnError)
	var fInterface string
	f.StringVar(&fInterface, "interface", "", "select 'serial' or 'i2c' interface for notecard")

	// Pre-generate error output
	errOutput := bytes.NewBufferString("")
	errOutput.WriteString("\nOptions:\n")
	defer f.SetOutput(nil)
	f.SetOutput(errOutput)
	f.PrintDefaults()

	// Parse flags
	f.Parse(strings.Split(s.Text, " "))

	// If no args, the request wasn't specified
	if f.NArg() < 2 {
		return "/watcher staging [command] [args]"
	}

	// Dispatch based on primary arg
	switch f.Arg(1) {

	case "show":
		return watcherShow(f.Arg(0), f.Arg(2))

	}

	return fmt.Sprintf("request '%s' not recognized\n"+errOutput.String(), f.Arg(0))

}
