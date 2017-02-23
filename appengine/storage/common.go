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

	// Datastore kind and ID for storing the summarization state.
	summaryStateKind = "SummaryState"
	summaryStateId   = 1
)

// summary contains information about a range of samples.
type summary struct {
	// Timestamp contains the start of the summarized period of time.
	Timestamp time.Time

	// Source and Name contain the samples' source and name.
	Source string
	Name   string

	// NumValues contains the total count of summarized samples. It is only used
	// to generate AvgValue.
	NumValues int `datastore:"-"`

	// MinValue, MaxValue, and AvgValue contain the minimum, maximum, and
	// average values from the summarized samples.
	MinValue float32 `datastore:",noindex"`
	MaxValue float32 `datastore:",noindex"`
	AvgValue float32 `datastore:",noindex"`
}

// summaryState contains high-level information about the current state of
// summarization.
type summaryState struct {
	// LastFullDay contains the starting time of the last fully-summarized day.
	LastFullDay time.Time
}

// getMsecSinceTime returns the number of elapsed milliseconds since t.
func getMsecSinceTime(t time.Time) int64 {
	return time.Now().Sub(t).Nanoseconds() / int64(time.Millisecond/time.Nanosecond)
}
