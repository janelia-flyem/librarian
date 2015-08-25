package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	// Display usage if true.
	showHelp = flag.Bool("help", false, "")

	// Run in verbose mode if true.
	runVerbose = flag.Bool("verbose", false, "")

	// Flag for clearing all locks at night.
	dailyClear = flag.Bool("dailyclear", false, "")

	// The HTTP address for help message and API
	httpAddress = flag.String("http", DefaultWebAddress, "")

	// If not empty, save log file here every midnight.
	backup = flag.String("backup", "", "")
)

const helpMessage = `
librarian is a server for coordinating label assignments among different clients.  It acts
like a librarian, allowing check-in and check-out of (uuid, label) tuples given a client id.
The client id is an arbitrary string, e.g., a user name.  All check-ins and check-outs are
recorded in a human-readable librarian log file.

Usage: librarian [options] /path/to/librarian.log

      -http       =string   Address for HTTP communication.
      -backup     =string   Daily (midnight) backup copies librarian log to this file.
      -dailyclear (flag)    Clear all locks at 2 AM every night.
      -verbose    (flag)    Run in verbose mode.
  -h, -help       (flag)    Show help message

To get more information on the REST API, visit the http address with a web browser.
`

var usage = func() {
	fmt.Printf(helpMessage)
}

func currentDir() string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalln("Could not get current directory:", err)
	}
	return currentDir
}

func main() {
	flag.BoolVar(showHelp, "h", false, "Show help message")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		*showHelp = true
	}

	if *showHelp {
		flag.Usage()
		os.Exit(0)
	}

	// Capture ctrl+c and other interrupts.  Then handle graceful shutdown.
	stopSig := make(chan os.Signal)
	go func() {
		for sig := range stopSig {
			log.Printf("Stop signal captured: %q.  Shutting down...\n", sig)
			os.Exit(0)
		}
	}()
	signal.Notify(stopSig, os.Interrupt, os.Kill, syscall.SIGTERM)

	// Load the log
	logfile := flag.Args()[0]
	if err := initLibrary(logfile); err != nil {
		log.Printf("Unable to open librarian log file (%s): %s\n", err.Error())
		os.Exit(1)
	}

	// Run the HTTP server
	serveHttp(*httpAddress)
}
