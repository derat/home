// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"erat.org/home/common"
	"google.golang.org/appengine/datastore"
)

func TestGenerateSummaries(t *testing.T) {
	c, done := initTest()
	defer done()

	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("Failed to load location: %v", err)
	}
	d := func(year, month, day, hour, min, sec int) time.Time {
		return time.Date(year, time.Month(month), day, hour, min, sec, 0, loc)
	}

	summariesToString := func(sums []*summary) string {
		strs := make([]string, len(sums))
		for i, s := range sums {
			strs[i] = fmt.Sprintf("%d|%s|%s|%.1f|%.1f|%.1f",
				s.StartTimestamp.Unix(), s.Source, s.Name, s.MinValue, s.MaxValue, s.AvgValue)
		}
		return strings.Join(strs, ",")
	}

	checkSummaries := func(kind string, es []*summary) {
		q := datastore.NewQuery(kind).Order("StartTimestamp").Order("Source").Order("Name")
		as := make([]*summary, 0)
		if _, err := q.GetAll(c, &as); err != nil {
			t.Fatalf("Failed to get summaries: %v", err)
		}
		e := summariesToString(es)
		a := summariesToString(as)
		if e != a {
			t.Errorf("Summary mismatch for %v:\nexpected: %v\n  actual: %v", kind, e, a)
		}
	}

	if err := WriteSamples(c, []common.Sample{
		common.Sample{d(2017, 1, 1, 0, 0, 0), "s0", "n0", 1.0},
		common.Sample{d(2017, 1, 1, 0, 5, 0), "s0", "n0", 2.0},
		common.Sample{d(2017, 1, 1, 0, 55, 0), "s0", "n0", 6.0},
		common.Sample{d(2017, 1, 1, 1, 0, 0), "s0", "n0", 5.0},
		common.Sample{d(2017, 1, 1, 1, 30, 0), "s0", "n0", 15.0},

		common.Sample{d(2017, 1, 1, 0, 8, 5), "s0", "n1", 3.0},
		common.Sample{d(2017, 1, 2, 4, 6, 0), "s0", "n1", 8.0},
		common.Sample{d(2017, 1, 3, 0, 0, 0), "s0", "n1", 5.0},

		common.Sample{d(2017, 1, 1, 0, 0, 0), "s1", "n0", 1.2},
	}); err != nil {
		t.Fatalf("Failed to insert samples: %v", err)
	}

	if err := GenerateSummaries(c, loc); err != nil {
		t.Fatalf("Failed to generate summaries: %v", err)
	}

	checkSummaries(hourSummaryKind, []*summary{
		&summary{d(2017, 1, 1, 0, 0, 0), "s0", "n0", 0, 1.0, 6.0, 3.0},
		&summary{d(2017, 1, 1, 0, 0, 0), "s0", "n1", 0, 3.0, 3.0, 3.0},
		&summary{d(2017, 1, 1, 0, 0, 0), "s1", "n0", 0, 1.2, 1.2, 1.2},
		&summary{d(2017, 1, 1, 1, 0, 0), "s0", "n0", 0, 5.0, 15.0, 10.0},
		&summary{d(2017, 1, 2, 4, 0, 0), "s0", "n1", 0, 8.0, 8.0, 8.0},
		&summary{d(2017, 1, 3, 0, 0, 0), "s0", "n1", 0, 5.0, 5.0, 5.0},
	})
	checkSummaries(daySummaryKind, []*summary{
		&summary{d(2017, 1, 1, 0, 0, 0), "s0", "n0", 0, 1.0, 15.0, 5.8},
		&summary{d(2017, 1, 1, 0, 0, 0), "s0", "n1", 0, 3.0, 3.0, 3.0},
		&summary{d(2017, 1, 1, 0, 0, 0), "s1", "n0", 0, 1.2, 1.2, 1.2},
		&summary{d(2017, 1, 2, 0, 0, 0), "s0", "n1", 0, 8.0, 8.0, 8.0},
		&summary{d(2017, 1, 3, 0, 0, 0), "s0", "n1", 0, 5.0, 5.0, 5.0},
	})
}
