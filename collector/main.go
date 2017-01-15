// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	var configPath string

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [option]...\n\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.StringVar(&configPath, "config", filepath.Join(os.Getenv("HOME"), ".home_collector.json"), "Path to JSON config file")
	flag.Parse()

	// TODO: Log to syslog instead using log/syslog:
	// syslog.NewLogger(syslog.LOG_INFO|syslog.LOG_DAEMON, log.LstdFlags)
	logger := log.New(os.Stderr, "", log.LstdFlags)
	cfg, err := readConfig(configPath, logger)
	if err != nil {
		logger.Fatalf("Unable to read config from %v: %v", configPath, err)
	}

	r := newReporter(cfg)
	r.start()

	if cfg.PingHost != "" {
		go runPingLoop(cfg, r)
	}

	l := &listener{cfg: cfg, rep: r}
	if err = l.run(); err != nil {
		logger.Fatalf("Got error while serving: %v", err)
	}
}
