// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"log"
)

type config struct {
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

	Logger *log.Logger
}

func readConfig(path string, logger *log.Logger) (*config, error) {
	cfg := &config{}
	cfg.ReportBatchSize = 10
	cfg.ReportTimeoutMs = 10000
	cfg.ReportRetryMs = 10000
	cfg.Logger = logger
	// FIXME: Read path.
	return cfg, nil
}
