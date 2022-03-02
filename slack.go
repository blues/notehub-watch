// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
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
