// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"fmt"
	"strings"
	"time"

	"erat.org/home/common"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

// Condition describes a condition responsible for triggering an alert.
type Condition struct {
	// Source and name associated with sample.
	Source string
	Name   string

	// Operator: one of "eq", "ne", "lt", "gt", "le", "ge", or "ot".
	// "ot" is "older than"; Value is then in seconds.
	Op string

	// Value to compare samples against.
	Value float32
}

func (c *Condition) id() string {
	return fmt.Sprintf("%s|%s|%s|%.1f", c.Source, c.Name, c.Op, c.Value)
}

func (c *Condition) active(s *common.Sample, now time.Time) (bool, error) {
	switch c.Op {
	case "eq":
		return s != nil && s.Value == c.Value, nil
	case "ne":
		return s != nil && s.Value != c.Value, nil
	case "lt":
		return s != nil && s.Value < c.Value, nil
	case "gt":
		return s != nil && s.Value > c.Value, nil
	case "le":
		return s != nil && s.Value <= c.Value, nil
	case "ge":
		return s != nil && s.Value >= c.Value, nil
	case "ot":
		return s == nil || now.Sub(s.Timestamp) > time.Duration(c.Value)*time.Second, nil
	default:
		return false, fmt.Errorf("Invalid condition %q", c.Op)
	}
}

func (c *Condition) msg(s *common.Sample, now time.Time) string {
	if c.Op == "ot" {
		var age string
		if s == nil {
			age = "missing"
		} else {
			age = fmt.Sprintf("%ds", int(now.Sub(s.Timestamp)/time.Second))
		}
		return fmt.Sprintf("%s.%s %s %ds: %s", c.Source, c.Name, c.Op, int(c.Value), age)
	}
	var val string
	if s == nil {
		val = "missing"
	} else {
		val = fmt.Sprintf("%.1f", c.Value)
	}
	return fmt.Sprintf("%s.%s %s %.1f: %s", c.Source, c.Name, c.Op, c.Value, val)
}

// activeCondition contains information about a currently-active condition.
type activeCondition struct {
	id  string
	msg string
}

func EvaluateConds(c context.Context, conds []Condition, now time.Time) error {
	log.Debugf(c, "Querying for samples for %v condition(s)", len(conds))
	samples, err := getSamplesForConds(c, conds)
	if err != nil {
		return err
	}
	log.Debugf(c, "Got %v sample(s)", len(samples))

	acs, err := getActiveConds(conds, samples, now)
	if err != nil {
		return err
	}
	for _, ac := range acs {
		log.Debugf(c, "Active condition %v: %v", ac.id, ac.msg)
	}
	return nil
}

// getSamplesForConds queries for and returns the most recent samples needed to
// evaluate conds. The returned map is keyed by "source|name", and values may be
// nil if corresponding samples weren't found in the datastore.
func getSamplesForConds(c context.Context, conds []Condition) (
	map[string]*common.Sample, error) {
	samples := make(map[string]*common.Sample)
	for _, cond := range conds {
		samples[cond.Source+"|"+cond.Name] = nil
	}

	type sampleError struct {
		s   *common.Sample
		err error
	}
	chans := make([]chan sampleError, 0, len(samples))

	bq := datastore.NewQuery(sampleKind).Limit(1).Order("-Timestamp")
	for sn := range samples {
		chans = append(chans, make(chan sampleError))
		parts := strings.Split(sn, "|")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid 'source|name' string %q", sn)
		}

		q := bq.Filter("Source =", parts[0]).Filter("Name =", parts[1])
		go func(q *datastore.Query, ch chan sampleError) {
			s := make([]common.Sample, 0)
			if _, err := q.GetAll(c, &s); err != nil {
				ch <- sampleError{nil, err}
			} else if len(s) == 0 {
				ch <- sampleError{nil, nil}
			} else {
				ch <- sampleError{&s[0], nil}
			}
		}(q, chans[len(chans)-1])
	}

	for _, ch := range chans {
		ce := <-ch
		if ce.err != nil {
			return nil, ce.err
		} else if ce.s != nil {
			samples[ce.s.Source+"|"+ce.s.Name] = ce.s
		}
	}
	return samples, nil
}

// getActiveConds returns active conditions. samples is keyed by "source|name",
// and values may be nil.
func getActiveConds(conds []Condition, samples map[string]*common.Sample, now time.Time) (
	[]activeCondition, error) {
	acs := make([]activeCondition, 0)
	for _, cond := range conds {
		s := samples[cond.Source+"|"+cond.Name]
		active, err := cond.active(s, now)
		if err != nil {
			return nil, err
		}
		if active {
			acs = append(acs, activeCondition{cond.id(), cond.msg(s, now)})
		}
	}
	return acs, nil
}
