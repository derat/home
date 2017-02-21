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

// summarizeDay reads samples starting at queryStart and generates summaries for
// the first day it sees (as interpreted for loc). It returns the start of that
// day, or a zero time if no samples were found.
func summarizeDay(c context.Context, loc *time.Location, queryStart time.Time) (
	dayStart time.Time, err error) {
	// Keyed by "source|name".
	daySums := make(map[string]*summary)
	hourSums := make(map[time.Time]map[string]*summary)

	q := datastore.NewQuery(sampleKind).Order("Timestamp")
	if !queryStart.IsZero() {
		q = q.Filter("Timestamp >=", queryStart)
	}

	numSamples := 0
	startTime := time.Now()
	it := q.Run(c)
	for {
		var s common.Sample
		if _, err := it.Next(&s); err == datastore.Done {
			break
		} else if err != nil {
			return time.Time{}, err
		}
		numSamples++

		lt := s.Timestamp.In(loc)
		ds := time.Date(lt.Year(), lt.Month(), lt.Day(), 0, 0, 0, 0, loc)
		if dayStart.IsZero() {
			dayStart = ds
		} else if ds != dayStart {
			break
		}
		updateSummary(daySums, &s, dayStart)

		// time.Date's handling of DST transitions is ambiguous, so use UTC.
		ut := s.Timestamp.In(time.UTC)
		hourStart := time.Date(ut.Year(), ut.Month(), ut.Day(), ut.Hour(), 0, 0, 0, time.UTC)
		if _, ok := hourSums[hourStart]; !ok {
			hourSums[hourStart] = make(map[string]*summary)
		}
		updateSummary(hourSums[hourStart], &s, hourStart)
	}

	log.Debugf(c, "Processed %v samples in %v ms", numSamples, getMsecSinceTime(startTime))
	return dayStart, writeSummaries(c, daySums, hourSums)
}

func GenerateSummaries(c context.Context, loc *time.Location) error {
	var err error
	dayStart := time.Time{}
	for {
		dayStart, err = summarizeDay(c, loc, dayStart)
		if err != nil {
			return err
		} else if dayStart.IsZero() {
			break
		}
		log.Debugf(c, "Finished summarizing %4d-%02d-%02d",
			dayStart.Year(), dayStart.Month(), dayStart.Day())
		dayStart = dayStart.AddDate(0, 0, 1)
	}
	return nil
}
