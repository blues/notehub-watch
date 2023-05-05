// Copyright 2022 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
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
func inboundWebSlackRequestHandler(w http.ResponseWriter, r *http.Request) {

	s, err := slack.SlashCommandParse(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	switch s.Command {
	case "/notehub":
		responseMarkdown := slackCommandWatcher(s)
		if slackUsingBlocksForResponses() {
			blocks := slack.Blocks{
				BlockSet: []slack.Block{
					slack.NewSectionBlock(
						&slack.TextBlockObject{
							Type: slack.MarkdownType,
							Text: responseMarkdown,
						},
						nil,
						nil,
					),
				},
			}
			w.Header().Set("Content-type", "application/json")
			slackResponse := slack.WebhookMessage{}
			slackResponse.Blocks = &blocks
			slackResponseJSON, _ := json.Marshal(slackResponse)
			w.Write(slackResponseJSON)
		} else {
			w.Write([]byte(responseMarkdown))
		}
	default:
		w.Write([]byte("unknown command"))
	}

}

// True if we're using blocks, which have certain limitations
func slackUsingBlocksForResponses() bool {
	return true
}

// Slack /notehub request handler
func slackCommandWatcher(s slack.SlashCommand) (response string) {

	// Register flags
	f := flag.NewFlagSet("/notehub", flag.ContinueOnError)

	// Add options here
	//	var fTest string
	//	f.StringVar(&fTest, "test", "", "testing 1-2-3")

	// Pre-generate error output
	errOutput := bytes.NewBufferString("")
	errOutput.WriteString("\nOptions:\n")
	defer f.SetOutput(nil)
	f.SetOutput(errOutput)
	f.PrintDefaults()

	// Parse flags
	f.Parse(strings.Split(s.Text, " "))

	// Server arg is required
	if f.Arg(0) == "" {
		return "/notehub <server> [<action> [<args>]]"
	}

	// Dispatch based on primary arg
	switch f.Arg(1) {

	case "":
		return watcherShow(f.Arg(0), "")

	case "stats":
		statsMaintainNow.Signal()
		return "stats maintenance update requested"

	case "show":
		return watcherShow(f.Arg(0), f.Arg(2))

	case "activity":
		return watcherActivity(f.Arg(0))

	}

	return fmt.Sprintf("request '%s' not recognized\n"+errOutput.String(), f.Arg(0))

}
