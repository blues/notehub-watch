// Copyright 2017 Inca Roads LLC.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jessevdk/go-flags"
	"github.com/slack-go/slack"
)

type watcherOptions struct {

	// Slice of bool will append 'true' each time the option
	// is encountered (can be set multiple times, like -vvv)
	Verbose []bool `short:"v" long:"verbose" description:"Show verbose debug information"`

	// Example of automatic marshalling to desired type (uint)
	Offset uint `long:"offset" description:"Offset"`

	// Example of a callback, called each time the option is found.
	Call func(string) `short:"c" description:"Call phone number"`

	// Example of a required flag
	Name string `short:"n" long:"name" description:"A name"`

	// Example of a value name
	File string `short:"f" long:"file" description:"A file" value-name:"FILE"`

	// Example of a pointer
	Ptr *int `short:"p" description:"A pointer to an integer"`

	// Example of a slice of strings
	StringSlice []string `short:"s" description:"A slice of strings"`

	// Example of a slice of pointers
	PtrSlice []*string `long:"ptrslice" description:"A slice of pointers to string"`

	// Example of a map
	IntMap map[string]int `long:"intmap" description:"A map from string to int"`

	// Example of positional string arguments
	Args struct {
		Request string
		//		Other   []string
	} `positional-args:"yes" description:"Watcher request" required:"true"`
}

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

	os.Args[0] = "//watcher"
	var opts watcherOptions
	_, err := flags.ParseArgs(&opts, strings.Split(s.Text, " "))
	if err != nil {
		return fmt.Sprintf("%s", err)
	}

	return fmt.Sprintf("%+v", opts)

}
