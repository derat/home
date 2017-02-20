// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"bytes"
	"erat.org/home/common"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
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

func generateQueryOutput(labels []string, ch chan timeData, loc *time.Location) (*bytes.Buffer, error) {
	buf := &bytes.Buffer{}
	buf.WriteString("{\"cols\":[")
	buf.WriteString("{\"type\":\"datetime\"}")
	for _, l := range labels {
		buf.WriteString(",{\"label\":\"")
		buf.WriteString(l)
		buf.WriteString("\",\"type\":\"number\"}")
	}
	buf.WriteString("],\"rows\":[")
	rowNum := 0
	for d := range ch {
		if d.err != nil {
			return nil, d.err
		}

		if rowNum > 0 {
			buf.WriteString(",")
		}

		// Well, this is awesome.
		t := d.timestamp.In(loc)
		buf.WriteString("{\"c\":[{\"v\":\"Date(")
		buf.WriteString(fmt.Sprintf("%d,%d,%d,%d,%d,%d",
			t.Year(), int(t.Month())-1, t.Day(), t.Hour(), t.Minute(), t.Second()))
		buf.WriteString(")\"}")

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
			buf.WriteString(",{\"v\":")
			buf.WriteString(val)
			buf.WriteString("}")
		}

		buf.WriteString("]}")
		rowNum++
	}
	buf.WriteString("]}")
	return buf, nil
}

func RunQuery(c context.Context, labels, sourceNames []string, start, end time.Time) (*bytes.Buffer, error) {
	baseQuery := datastore.NewQuery(sampleKind).Limit(maxQueryResults).Order("Timestamp")
	baseQuery = baseQuery.Filter("Timestamp >=", start).Filter("Timestamp <=", end)

	chans := make([]chan point, len(sourceNames))
	for i, sn := range sourceNames {
		chans[i] = make(chan point)
		parts := strings.Split(sn, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid 'source|name' string %q", sn)
		}
		q := baseQuery.Filter("Source =", parts[0]).Filter("Name =", parts[1])

		go func(q *datastore.Query, ch chan point) {
			it := q.Run(c)
			for {
				var s common.Sample
				if _, err := it.Next(&s); err == datastore.Done {
					close(ch)
					break
				} else if err != nil {
					ch <- point{time.Time{}, 0, err}
					break
				}
				ch <- point{s.Timestamp, s.Value, nil}
			}
		}(q, chans[i])
	}

	out := make(chan timeData)
	go mergeQueryData(chans, out)
	return generateQueryOutput(labels, out, start.Location())
}
