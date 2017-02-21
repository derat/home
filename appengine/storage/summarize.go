// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"fmt"
	"math"
	"time"

	"erat.org/home/common"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const (
	// App Engine imposes a limit of 500 entities per write operation.
	summaryUpdateBatchSize = 500
)

func updateSummary(sums map[string]*summary, sam *common.Sample, ts time.Time) {
	sn := fmt.Sprintf("%s|%s", sam.Source, sam.Name)
	if sum, ok := sums[sn]; ok {
		if sum.Timestamp != ts {
			panic(fmt.Sprintf("summary for %v starts at %v instead of %v", sn, sum.Timestamp, ts))
		}
		sum.NumValues += 1
		sum.MinValue = float32(math.Min(float64(sam.Value), float64(sum.MinValue)))
		sum.MaxValue = float32(math.Max(float64(sam.Value), float64(sum.MaxValue)))
		sum.AvgValue = sum.AvgValue*((float32(sum.NumValues)-1)/float32(sum.NumValues)) +
			sam.Value*(1/float32(sum.NumValues))
	} else {
		sums[sn] = &summary{
			Timestamp: ts,
			Source:    sam.Source,
			Name:      sam.Name,
			NumValues: 1,
			MinValue:  sam.Value,
			MaxValue:  sam.Value,
			AvgValue:  sam.Value,
		}
	}
}

func writeSummaries(c context.Context, ds map[string]*summary,
	hs map[time.Time]map[string]*summary) error {
	keys := make([]*datastore.Key, 0, summaryUpdateBatchSize)
	sums := make([]*summary, 0, summaryUpdateBatchSize)

	numSummaries := 0
	add := func(kind string, s *summary) error {
		numSummaries++
		id := fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
		keys = append(keys, datastore.NewKey(c, kind, id, 0, nil))
		sums = append(sums, s)
		if len(sums) == summaryUpdateBatchSize {
			if _, err := datastore.PutMulti(c, keys, sums); err != nil {
				return err
			}
			keys = make([]*datastore.Key, 0, summaryUpdateBatchSize)
			sums = make([]*summary, 0, summaryUpdateBatchSize)
		}
		return nil
	}

	startTime := time.Now()
	for _, s := range ds {
		if err := add(daySummaryKind, s); err != nil {
			return err
		}
	}
	for _, m := range hs {
		for _, s := range m {
			if err := add(hourSummaryKind, s); err != nil {
				return err
			}
		}
	}
	if len(sums) != 0 {
		if _, err := datastore.PutMulti(c, keys, sums); err != nil {
			return err
		}
	}

	log.Debugf(c, "Wrote %v summaries in %v ms",
		numSummaries, getMsecSinceTime(startTime))
	return nil
}

func summarizeDay(c context.Context, dayStart time.Time) error {
	log.Debugf(c, "Generating summaries for %4d-%02d-%02d",
		dayStart.Year(), dayStart.Month(), dayStart.Day())

	// Keyed by "source|name".
	daySums := make(map[string]*summary)
	hourSums := make(map[time.Time]map[string]*summary)

	numSamples := 0
	startTime := time.Now()
	q := datastore.NewQuery(sampleKind).Order("Timestamp").
		Filter("Timestamp >=", dayStart).Filter("Timestamp <", dayStart.AddDate(0, 0, 1))
	it := q.Run(c)
	for {
		var s common.Sample
		if _, err := it.Next(&s); err == datastore.Done {
			break
		} else if err != nil {
			return err
		}
		numSamples++

		updateSummary(daySums, &s, dayStart)

		// time.Date's handling of DST transitions is ambiguous, so use UTC.
		ut := s.Timestamp.In(time.UTC)
		hourStart := time.Date(ut.Year(), ut.Month(), ut.Day(), ut.Hour(), 0, 0, 0, ut.Location())
		if _, ok := hourSums[hourStart]; !ok {
			hourSums[hourStart] = make(map[string]*summary)
		}
		updateSummary(hourSums[hourStart], &s, hourStart)
	}

	log.Debugf(c, "Processed %v samples in %v ms",
		numSamples, getMsecSinceTime(startTime))
	return writeSummaries(c, daySums, hourSums)
}

func GenerateSummaries(c context.Context, loc *time.Location) error {
	getTimestamp := func(order string) (time.Time, error) {
		it := datastore.NewQuery(sampleKind).Order(order).Limit(1).Run(c)
		var s common.Sample
		if _, err := it.Next(&s); err == datastore.Done {
			return time.Time{}, nil
		} else if err != nil {
			return time.Time{}, err
		}
		return s.Timestamp.In(loc), nil
	}

	var err error
	var min, max time.Time
	if min, err = getTimestamp("Timestamp"); err != nil {
		return err
	}
	if max, err = getTimestamp("-Timestamp"); err != nil {
		return err
	}

	ds := time.Date(min.Year(), min.Month(), min.Day(), 0, 0, 0, 0, loc)
	for ; !max.Before(ds); ds = ds.AddDate(0, 0, 1) {
		if err := summarizeDay(c, ds); err != nil {
			return err
		}
	}
	return nil
}
