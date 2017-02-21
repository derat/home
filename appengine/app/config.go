// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package app

import (
	"time"

	"erat.org/home/common"
	"google.golang.org/appengine"
)

const (
	// Hardcoded secret used when running dev app server.
	devSecret = "secret"

	// Default duration to display in graphs.
	defaultGraphSec = 7200

	// Default reporting interval.
	defaultReportSec = 300
)

// graphLineConfig describes a line within a graph.
type graphLineConfig struct {
	// Label displayed on graph.
	Label string

	// Source and name associated with samples.
	Source string
	Name   string
}

// graphConfig holds configuration for an individual graph.
type graphConfig struct {
	// Graph title.
	Title string

	// Human-units used as label for vertical axis.
	Units string

	// Number of seconds of data to graph.
	Seconds int

	// If empty or unsupplied, the Y-axis range is determined automatically.
	// If one value is present, it is interpreted as the minimum value.
	// If two values are present, they are interpreted as the min and max.
	Range []float32

	// If true, graph uses less vertical space than usual.
	Short bool

	// Reporting interval in seconds. If accurate, aids in choosing when to
	// graph hourly or daily averages instead of individual samples.
	ReportSeconds int

	// Lines within the graph.
	Lines []graphLineConfig
}

// config holds user-configurable top-level settings.
type config struct {
	// Secret used by collector to sign reports.
	ReportSecret string

	// Email addresses of authorized users.
	Users []string

	// Time zone, e.g. "America/Los_Angeles".
	TimeZone string

	// Page title.
	Title string

	// Graphs to display on page.
	Graphs []graphConfig
}

func loadConfig(path string) (*config, *time.Location, error) {
	var err error
	c := &config{}
	if err = common.ReadJson(configPath, c); err != nil {
		return nil, nil, err
	}
	if appengine.IsDevAppServer() {
		c.ReportSecret = devSecret
	}
	if c.TimeZone == "" {
		c.TimeZone = "America/Los_Angeles"
	}
	for i := range c.Graphs {
		if c.Graphs[i].Seconds <= 0 {
			c.Graphs[i].Seconds = defaultGraphSec
		}
		if c.Graphs[i].ReportSeconds <= 0 {
			c.Graphs[i].ReportSeconds = defaultReportSec
		}
	}
	var loc *time.Location
	if loc, err = time.LoadLocation(c.TimeZone); err != nil {
		return nil, nil, err
	}
	return c, loc, nil
}
