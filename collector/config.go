// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"erat.org/cloud"
	"log"
)

type config struct {
	// String describing this device in samples that it generates, e.g.
	// "COLLECTOR".
	Source string

	// Address used to listen for reports, e.g. ":8080".
	ListenAddress string

	// Full URL to report samples, e.g. "http://example.com/report".
	ReportURL string

	// Shared secret used to sign reports.
	ReportSecret string

	// Path to JSON file storing not-yet-reported samples.
	BackingFile string

	// Maximum number of samples to report in a single request.
	ReportBatchSize int

	// Client timeout when communicating with server, in milliseconds.
	ReportTimeoutMs int

	// Time to wait before retrying on failure, in milliseconds.
	ReportRetryMs int

	// Time between ping samples, in seconds.
	PingSampleIntervalSec int

	// Host to ping to test network connectivity, e.g. "www.google.com".
	// Empty to disable pinging.
	PingHost string

	// Number of pings to send for each sample.
	PingCount int

	// Delay between sent pings within a sample, in milliseconds.
	// The ping command may limit this to a minimum of 200 for non-root users.
	PingDelayMs int

	// Total time to wait for each sample's group of pings to complete, in
	// seconds. See the ping command's -w flag for details.
	PingTimeoutSec int

	Logger *log.Logger
}

func readConfig(path string, logger *log.Logger) (*config, error) {
	cfg := &config{}
	cfg.Source = "collector"
	cfg.ListenAddress = ":4587"
	cfg.ReportBatchSize = 10
	cfg.ReportTimeoutMs = 10000
	cfg.ReportRetryMs = 10000
	cfg.PingHost = "8.8.8.8"
	cfg.PingCount = 5
	cfg.PingDelayMs = 1000
	cfg.PingTimeoutSec = 20
	cfg.Logger = logger

	if len(path) != 0 {
		err := cloud.ReadJson(path, cfg)
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}
