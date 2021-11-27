// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/derat/home/appengine/storage"

	"google.golang.org/appengine/v2"
)

const (
	// Hardcoded secret used when running dev app server.
	devSecret = "secret"

	// Default values used in configs.
	defaultGraphSec        = 7200
	defaultReportSec       = 300
	defaultFullDayDelaySec = 24 * 3600
	defaultDaysToKeep      = 3
)

// graphLineConfig describes a line within a graph.
type graphLineConfig struct {
	// Label displayed on graph.
	Label string `json:"label"`

	// Source and name associated with samples.
	Source string `json:"source"`
	Name   string `json:"name"`
}

// graphConfig holds configuration for an individual graph.
type graphConfig struct {
	// Graph title.
	Title string `json:"title"`

	// Human-units used as label for vertical axis.
	Units string `json:"units"`

	// Number of seconds of data to graph.
	Seconds int `json:"seconds"`

	// If empty or unsupplied, the Y-axis range is determined automatically.
	// If one value is present, it is interpreted as the minimum value.
	// If two values are present, they are interpreted as the min and max.
	Range []float32 `json:"range"`

	// If true, graph uses less vertical space than usual.
	Short bool `json:"short"`

	// Reporting interval in seconds. If accurate, aids in choosing when to
	// graph hourly or daily averages instead of individual samples.
	ReportSeconds int `json:"reportSeconds"`

	// Lines within the graph.
	Lines []graphLineConfig `json:"lines"`
}

// config holds user-configurable top-level settings.
type config struct {
	// Google Cloud project ID.
	ProjectID string `json:"projectId"`

	// Secret used by collector to sign reports.
	ReportSecret string `json:"reportSecret"`

	// Email addresses of authorized users.
	Users []string `json:"users"`

	// Time zone, e.g. "America/Los_Angeles".
	TimeZone string `json:"timeZone"`

	// Email address from which alerts will be sent. See
	// https://cloud.google.com/appengine/docs/standard/python/mail/#who_can_send_mail
	// for allowed addresses.
	AlertSender string `json:"alertSender"`

	// Email addresses to which alerts will be sent.
	AlertRecipients []string `json:"alertRecipients"`

	// Conditions that trigger alerts.
	AlertConditions []storage.Condition `json:"alertConditions"`

	// Page title.
	Title string `json:"title"`

	// Graphs to display on page.
	Graphs []graphConfig `json:"graphs"`

	// Days of fully-summarized samples to keep. Older samples are deleted
	// periodically.
	DaysToKeep int `json:"daysToKeep"`

	// Number of seconds to wait after the end of a day before assuming that we
	// won't get any new samples for it (and don't need to continue
	// re-summarizing it).
	FullDayDelaySeconds int `json:"fullDayDelaySeconds"`
}

func loadConfig(path string) (*config, *time.Location, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	c := &config{}
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(c); err != nil {
		return nil, nil, err
	}

	if appengine.IsDevAppServer() {
		c.ReportSecret = devSecret
	}
	if c.TimeZone == "" {
		c.TimeZone = "America/Los_Angeles"
	}
	// TODO: Add some way to permit specifying 0.
	if c.DaysToKeep <= 0 {
		c.DaysToKeep = defaultDaysToKeep
	}
	if c.FullDayDelaySeconds <= 0 {
		c.FullDayDelaySeconds = defaultFullDayDelaySec
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
