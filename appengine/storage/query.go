// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/derat/home/common"

	"google.golang.org/appengine/datastore"
)

const (
	maxQueryDatastoreResults = 60 * 24
	maxQueryPoints           = 100
)

type point struct {
	timestamp time.Time
	value     float32
	err       error
}

// QueryGranularity describes types of points used in query results.
type QueryGranularity int

const (
	IndividualSample QueryGranularity = iota
	HourlyAverage
	DailyAverage
)

// QueryParams describes a query to be performed.
type QueryParams struct {
	// Labels contains human-readable labels for lines.
	Labels []string

	// SourceNames contains "source|name" pairs describing lines. It must be the
	// same length, and be in the same order, as labels.
	SourceNames []string

	// Start and End describe the inclusive time range for the query.
	Start time.Time
	End   time.Time

	// Granularity describes the type of points to use.
	Granularity QueryGranularity

	// Aggregation describes how many sequential points to average together for
	// each returned point. It has no effect if less than or equal to 1.
	Aggregation int
}

// UpdateGranularityAndAggregation updates the Granularity and Aggregation
// fields based on Start, End, sampleInterval (the typical interval between
// samples), and sampleStart (an optional timestamp describing the oldest
// samples that are available).
func (qp *QueryParams) UpdateGranularityAndAggregation(
	sampleInterval time.Duration, sampleStart time.Time) {
	queryDuration := qp.End.Sub(qp.Start)
	day := 24 * time.Hour
	dayCount := int(queryDuration / day)
	hourCount := int(queryDuration / time.Hour)
	sampleCount := int(queryDuration / sampleInterval)
	samplesPerHour := int(time.Hour / sampleInterval)
	hoursPerDay := int(day / time.Hour)

	samplesMissing := !sampleStart.IsZero() && qp.Start.Before(sampleStart)

	qp.Aggregation = 1
	if hourCount/hoursPerDay*2 > maxQueryPoints {
		qp.Granularity = DailyAverage
		if dayCount > maxQueryPoints {
			qp.Aggregation = dayCount / maxQueryPoints
		}
	} else if samplesMissing || sampleCount/samplesPerHour*2 > maxQueryPoints {
		qp.Granularity = HourlyAverage
		if hourCount > maxQueryPoints {
			qp.Aggregation = hourCount / maxQueryPoints
		}
	} else {
		qp.Granularity = IndividualSample
		if sampleCount > maxQueryPoints {
			qp.Aggregation = sampleCount / maxQueryPoints
		}
	}
}

// runQuery runs the query described by qp synchronously and writes a Google
// Chart API DataTable object to w.
func DoQuery(c context.Context, w io.Writer, qp QueryParams) error {
	if len(qp.Labels) != len(qp.SourceNames) {
		return fmt.Errorf("Different numbers of labels and sourcenames")
	}

	kind := sampleKind
	if qp.Granularity == HourlyAverage {
		kind = hourSummaryKind
	} else if qp.Granularity == DailyAverage {
		kind = daySummaryKind
	}

	baseQuery := datastore.NewQuery(kind).Limit(maxQueryDatastoreResults).Order("Timestamp")
	baseQuery = baseQuery.Filter("Timestamp >=", qp.Start).Filter("Timestamp <=", qp.End)

	chans := make([]chan point, len(qp.SourceNames))
	for i, sn := range qp.SourceNames {
		chans[i] = make(chan point)
		parts := strings.Split(sn, "|")
		if len(parts) != 2 {
			return fmt.Errorf("Invalid 'source|name' string %q", sn)
		}
		q := baseQuery.Filter("Source =", parts[0]).Filter("Name =", parts[1])

		go func(q *datastore.Query, ch chan point) {
			var s interface{}
			var mp func(s interface{}) point

			if qp.Granularity == IndividualSample {
				s = &common.Sample{}
				mp = func(s interface{}) point {
					return point{s.(*common.Sample).Timestamp, s.(*common.Sample).Value, nil}
				}
			} else {
				s = &summary{}
				mp = func(s interface{}) point {
					return point{s.(*summary).Timestamp, s.(*summary).AvgValue, nil}
				}
			}

			var points []point
			if qp.Aggregation > 1 {
				points = make([]point, 0, qp.Aggregation)
			}

			it := q.Run(c)
			for {
				if _, err := it.Next(s); err == datastore.Done {
					if points != nil && len(points) > 0 {
						ch <- averagePoints(points)
					}
					close(ch)
					break
				} else if err != nil {
					ch <- point{time.Time{}, 0, err}
					break
				}

				p := mp(s)
				if points == nil {
					ch <- p
				} else {
					points = append(points, p)
					if len(points) == qp.Aggregation {
						ch <- averagePoints(points)
						points = points[:0]
					}
				}

			}
		}(q, chans[i])
	}

	out := make(chan timeData)
	go mergeQueryData(chans, out)
	return writeQueryOutput(w, qp.Labels, out, qp.Start.Location())
}

// averagePoints returns a point containing the midpoint time and average value
// of points, which must be sorted by ascending time.
func averagePoints(points []point) point {
	if len(points) == 0 {
		return point{}
	} else if len(points) == 1 {
		return points[0]
	}

	var total float32
	for i, _ := range points {
		total += points[i].value
	}
	elapsed := points[len(points)-1].timestamp.Sub(points[0].timestamp)
	return point{
		timestamp: points[0].timestamp.Add(elapsed / time.Duration(2)),
		value:     total / float32(len(points)),
		err:       nil,
	}
}

// timeData contains values associated with a given timestamp. If a line did not
// have a value at that time, its entry in values is NaN. Trailing NaN values
// may be omitted.
type timeData struct {
	timestamp time.Time
	values    []float32
	err       error
}

// mergeQueryData reads points in ascending time from channels (one per
// line) and writes per-timestamp sets of values to out.
func mergeQueryData(in []chan point, out chan timeData) {
	nan := float32(math.NaN())
	next := make([]*point, len(in))
	for {
		t := time.Time{}
		for i := range next {
			if next[i] == nil {
				if p, more := <-in[i]; more {
					if p.err != nil {
						out <- timeData{time.Time{}, nil, p.err}
						close(out)
						return
					}
					next[i] = &p
				}
			}
			if next[i] != nil {
				if t.IsZero() || next[i].timestamp.Before(t) {
					t = next[i].timestamp
				}
			}
		}

		// All input channels are closed.
		if t.IsZero() {
			break
		}

		data := timeData{t, make([]float32, len(in)), nil}
		for i := range next {
			if next[i] != nil && next[i].timestamp == t {
				data.values[i] = next[i].value
				next[i] = nil
			} else {
				data.values[i] = nan
			}
		}
		out <- data
	}
	close(out)
}

// writeQueryOutput reads per-timestamp sets of values from ch and writes them
// to w as a JSON object that can be used to construct a Google Chart API
// DataTable object
// (https://developers.google.com/chart/interactive/docs/reference#dataparam).
// labels provides labels for each line, and loc provides the time zone that is
// used when converting timeData's timestamps to symbolic times.
func writeQueryOutput(w io.Writer, labels []string, ch chan timeData, loc *time.Location) error {
	var err error
	write := func(s string) {
		if err != nil {
			return
		}
		_, err = w.Write([]byte(s))
	}

	write("{\"cols\":[")
	write("{\"type\":\"datetime\"}")
	for _, l := range labels {
		write(",{\"label\":\"")
		write(l)
		write("\",\"type\":\"number\"}")
	}
	write("],\"rows\":[")
	rowNum := 0
	for d := range ch {
		if d.err != nil {
			return d.err
		}

		if rowNum > 0 {
			write(",")
		}

		// Well, this is awesome.
		t := d.timestamp.In(loc)
		write("{\"c\":[{\"v\":\"Date(")
		write(fmt.Sprintf("%d,%d,%d,%d,%d,%d",
			t.Year(), int(t.Month())-1, t.Day(), t.Hour(), t.Minute(), t.Second()))
		write(")\"}")

		// Find the index of the last non-NaN value.
		lastCol := -1
		for i, v := range d.values {
			if v == v {
				lastCol = i
			}
		}
		for i := 0; i <= lastCol; i++ {
			var val string
			if d.values[i] != d.values[i] {
				val = "null"
			} else {
				val = strconv.FormatFloat(float64(d.values[i]), 'f', -1, 32)
			}
			write(",{\"v\":")
			write(val)
			write("}")
		}

		write("]}")
		rowNum++
	}
	write("]}")
	return err
}
