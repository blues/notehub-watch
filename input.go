// Copyright 2020 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.

package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func inputHandler() {

	// Spawn our signal handler
	go signalHandler()

	// Create a scanner to watch stdin
	scanner := bufio.NewScanner(os.Stdin)
	var message string

	for {

		scanner.Scan()
		message = scanner.Text()

		args := strings.Split(message, " ")
		argsLC := strings.Split(strings.ToLower(message), " ")

		arg0 := ""
		arg0LC := ""
		if len(args) > 0 {
			arg0 = args[0]
			arg0LC = argsLC[0]
		}

		arg1 := ""
		arg1LC := ""
		if len(args) > 1 {
			arg1 = args[1]
			arg1LC = argsLC[1]
		}
		_ = arg1

		arg2 := ""
		arg2LC := ""
		if len(args) > 2 {
			arg2 = args[2]
			arg2LC = argsLC[2]
		}
		_ = arg2

		messageAfterFirstWord := ""
		if len(args) > 1 {
			messageAfterFirstWord = strings.Join(args[1:], " ")
		}

		if false {
			unused := arg0 + arg1LC + arg2LC + messageAfterFirstWord
			fmt.Printf("%s", unused)
		}

		switch arg0LC {

		case "":

		default:
			fmt.Printf("Unrecognized: '%s'\n", message)

		case "q":
			os.Exit(0)

		}

		// Prompt after performing command
		fmt.Print("\n> ")

	}

}

// Our app's signal handler
func signalHandler() {
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGTERM)
	signal.Notify(ch, syscall.SIGINT)
	signal.Notify(ch, syscall.SIGSEGV)
	for {
		switch <-ch {
		case syscall.SIGINT:
			fmt.Printf("*** Exiting because of SIGNAL \n")
			os.Exit(0)
		case syscall.SIGTERM:
			break
		}
	}
}
