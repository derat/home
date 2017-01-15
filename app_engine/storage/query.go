// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"erat.org/home/common"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"math"
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

func RunQuery(c context.Context, sourceNames []string, start, end time.Time) error {
	baseQuery := datastore.NewQuery(sampleKind).Limit(maxQueryResults).Order("Timestamp")
	baseQuery = baseQuery.Filter("Timestamp >=", start).Filter("Timestamp <=", end)

	chans := make([]chan point, len(sourceNames))
	for i, sn := range sourceNames {
		chans[i] = make(chan point)
		parts := strings.Split(sn, "|")
		if len(parts) != 2 {
			return fmt.Errorf("Invalid 'source|name' string %q", sn)
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

	// FIXME: read from out
	out := make(chan timeData)
	mergeQueryData(chans, out)
	return nil
}
