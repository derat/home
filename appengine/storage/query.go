// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"erat.org/home/common"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	maxQueryResults = 60 * 24
)

type point struct {
	timestamp time.Time
	value     float32
	err       error
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

// timeData contains values associated with a given timestamp.
// If an input channel did not have a value, its entry in values
// is NaN.
type timeData struct {
	timestamp time.Time
	values    []float32
	err       error
}

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

type QueryGranularity int

const (
	IndividualSample QueryGranularity = iota
	HourlyAverage
	DailyAverage
)

type QueryParams struct {
	Labels           []string
	SourceNames      []string
	Start            time.Time
	End              time.Time
	Granularity      QueryGranularity
	AggregationCount int
}

func RunQuery(c context.Context, w io.Writer, qp QueryParams) error {
	if len(qp.Labels) != len(qp.SourceNames) {
		return fmt.Errorf("Different numbers of labels and sourcenames")
	}

	kind := sampleKind
	if qp.Granularity == HourlyAverage {
		kind = hourSummaryKind
	} else if qp.Granularity == DailyAverage {
		kind = daySummaryKind
	}

	baseQuery := datastore.NewQuery(kind).Limit(maxQueryResults).Order("Timestamp")
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
			if qp.AggregationCount > 1 {
				points = make([]point, 0, qp.AggregationCount)
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
					if len(points) == qp.AggregationCount {
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
