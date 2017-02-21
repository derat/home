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

func GenerateSummaries(c context.Context, loc *time.Location) error {
	var dayStart, hourStart time.Time
	// Keyed by "source|name".
	daySums := make(map[string]*summary)
	hourSums := make(map[string]*summary)

	ch := make(chan error)
	numWrites := 0

	writeSummaries := func(kind string, m map[string]*summary) {
		keys := make([]*datastore.Key, len(m))
		sums := make([]*summary, len(m))
		i := 0
		for _, s := range m {
			id := fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
			keys[i] = datastore.NewKey(c, kind, id, 0, nil)
			sums[i] = s
			i++
		}

		numWrites++
		go func() {
			_, err := datastore.PutMulti(c, keys, sums)
			ch <- err
		}()
	}

	it := datastore.NewQuery(sampleKind).Order("Timestamp").Run(c)
	for {
		var s common.Sample
		if _, err := it.Next(&s); err == datastore.Done {
			break
		} else if err != nil {
			return err
		}

		lt := s.Timestamp.In(loc)
		ds := time.Date(lt.Year(), lt.Month(), lt.Day(), 0, 0, 0, 0, lt.Location())
		if dayStart != ds {
			if len(daySums) > 0 {
				writeSummaries(daySummaryKind, daySums)
				daySums = make(map[string]*summary)
			}
			dayStart = ds
		}
		updateSummary(daySums, &s, ds)

		// time.Date's handling of DST transitions is ambiguous, so use UTC.
		ut := s.Timestamp.In(time.UTC)
		hs := time.Date(ut.Year(), ut.Month(), ut.Day(), ut.Hour(), 0, 0, 0, ut.Location())
		if hourStart != hs {
			if len(hourSums) > 0 {
				writeSummaries(hourSummaryKind, hourSums)
				hourSums = make(map[string]*summary)
			}
			hourStart = hs
		}
		updateSummary(hourSums, &s, hs)
	}

	if len(daySums) > 0 {
		writeSummaries(daySummaryKind, daySums)
	}
	if len(hourSums) > 0 {
		writeSummaries(hourSummaryKind, hourSums)
	}

	for i := 0; i < numWrites; i++ {
		if err := <-ch; err != nil {
			return err
		}
	}
	return nil
}
