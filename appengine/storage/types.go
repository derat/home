// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"time"
)

const (
	// Datastore kind for sample entities.
	sampleKind = "Sample"

	// Datastore kinds for summary entities.
	hourSummaryKind = "HourSummary"
	daySummaryKind  = "DaySummary"

	summaryStateKind = "SummaryState"
	summaryStateId   = 1
)

type summary struct {
	Timestamp time.Time
	Source    string
	Name      string
	NumValues int     `datastore:"-"`
	MinValue  float32 `datastore:",noindex"`
	MaxValue  float32 `datastore:",noindex"`
	AvgValue  float32 `datastore:",noindex"`
}

type summaryState struct {
	LastFullDay time.Time
}
