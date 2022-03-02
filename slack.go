// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"fmt"
	"net/http"

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
	response = fmt.Sprintf("echo: %v", s.Text)
	return
}
