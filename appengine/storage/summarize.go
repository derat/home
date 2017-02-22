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
	summaryDeleteBatchSize = 500
)

func getSummaryLastFullDay(c context.Context) (time.Time, error) {
	s := summaryState{}
	k := datastore.NewKey(c, summaryStateKind, "", summaryStateId, nil)
	if err := datastore.Get(c, k, &s); err != nil && err != datastore.ErrNoSuchEntity {
		return time.Time{}, err
	}
	return s.LastFullDay, nil
}

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

	writeAndClear := func() error {
		if _, err := datastore.PutMulti(c, keys, sums); err != nil {
			return err
		}
		keys = make([]*datastore.Key, 0, summaryUpdateBatchSize)
		sums = make([]*summary, 0, summaryUpdateBatchSize)
		return nil
	}

	numSummaries := 0
	add := func(kind string, s *summary) error {
		numSummaries++
		id := fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
		keys = append(keys, datastore.NewKey(c, kind, id, 0, nil))
		sums = append(sums, s)
		if len(sums) == summaryUpdateBatchSize {
			if err := writeAndClear(); err != nil {
				return err
			}
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
		if err := writeAndClear(); err != nil {
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
		hourStart := time.Date(
			ut.Year(), ut.Month(), ut.Day(), ut.Hour(), 0, 0, 0, time.UTC)
		if _, ok := hourSums[hourStart]; !ok {
			hourSums[hourStart] = make(map[string]*summary)
		}
		updateSummary(hourSums[hourStart], &s, hourStart)
	}

	if numSamples == 0 {
		return time.Time{}, nil
	}

	log.Debugf(c, "Processed %v samples in %v ms",
		numSamples, getMsecSinceTime(startTime))
	return dayStart, writeSummaries(c, daySums, hourSums)
}

// GenerateSummaries reads samples and inserts daily and hourly summary
// entities. now.Location() is used to define day boundaries; hour boundaries
// are computed based on UTC. fullDayDelay defines how long we wait after the
// end of a day before assuming that we have all the data we're going to get
// from it (and not re-summarizing it in the future).
func GenerateSummaries(c context.Context, now time.Time, fullDayDelay time.Duration) error {
	ct := now.Add(time.Duration(-1) * fullDayDelay)
	partialDay := time.Date(ct.Year(), ct.Month(), ct.Day(), 0, 0, 0, 0, ct.Location())

	// This could all be much simpler if it were possible to do a single query
	// to get all samples, iterate through them in-order, and insert summaries
	// in parallel while we go. However, App Engine appears to impose a
	// five-second deadline on datastore RPCs, which is pretty easy to hit when
	// summarizing multiple days' worth of samples. It's possible to get around
	// this by grabbing a cursor and issuing a new query when near the deadline,
	// but that leads to the second problem: datastore writes are extremely
	// prone to failure, and become even more so when doing multiple writes in
	// parallel.
	//
	// To mostly sidestep all of this garbage, issue a separate query for each
	// day, insert summaries using sequential operations after reading the whole
	// day, and mark the day as complete after summarizing it. This makes it
	// more likely that we'll make forward progress when summarizing multiple
	// days even if/when we hit a write error midway through.
	dayStart := time.Time{}
	if lfd, err := getSummaryLastFullDay(c); err != nil {
		return err
	} else if !lfd.IsZero() {
		dayStart = lfd.In(now.Location()).AddDate(0, 0, 1)
	}

	for {
		var err error
		dayStart, err = summarizeDay(c, now.Location(), dayStart)
		if err != nil {
			return err
		} else if dayStart.IsZero() {
			break
		}
		log.Debugf(c, "Finished summarizing %4d-%02d-%02d",
			dayStart.Year(), dayStart.Month(), dayStart.Day())

		if dayStart.Before(partialDay) {
			log.Debugf(c, "Marking %4d-%02d-%02d as fully summarized",
				dayStart.Year(), dayStart.Month(), dayStart.Day())
			k := datastore.NewKey(c, summaryStateKind, "", summaryStateId, nil)
			if _, err := datastore.Put(c, k, &summaryState{dayStart}); err != nil {
				return err
			}
		}

		dayStart = dayStart.AddDate(0, 0, 1)
	}
	return nil
}

// DeleteSummarizedSamples deletes samples from days that have been "fully"
// summarized (see GenerateSummaries). Samples from partially-summarized days
// are never deleted. loc is used to determine day boundaries. daysToKeep
// defines the number of fully-summarized days for which samples should be
// retained.
func DeleteSummarizedSamples(c context.Context, loc *time.Location, daysToKeep int) error {
	lastFullDay, err := getSummaryLastFullDay(c)
	if err != nil {
		return err
	} else if lastFullDay.IsZero() {
		return nil
	}
	keepDay := lastFullDay.In(loc).AddDate(0, 0, 1-daysToKeep)

	log.Debugf(c, "Deleting all samples earlier than %4d-%02d-%02d",
		keepDay.Year(), keepDay.Month(), keepDay.Day())
	q := datastore.NewQuery(sampleKind).KeysOnly().
		Filter("Timestamp <", keepDay).Limit(summaryDeleteBatchSize)
	for {
		var keys []*datastore.Key
		if keys, err = q.GetAll(c, nil); err != nil {
			return err
		} else if len(keys) == 0 {
			break
		}
		log.Debugf(c, "Deleting %v sample(s)", len(keys))
		if err = datastore.DeleteMulti(c, keys); err != nil {
			return err
		}
	}
	return nil
}
