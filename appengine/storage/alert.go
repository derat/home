// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/derat/home/common"

	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/mail"
)

const (
	// Datastore kind and ID for storing the alert state.
	alertStateKind = "AlertState"
	alertStateId   = 1
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

// id returns a string uniquely identifying this condition.
func (c *Condition) id() string {
	return fmt.Sprintf("%s|%s|%s|%.1f", c.Source, c.Name, c.Op, c.Value)
}

// active returns true if s is active.
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

// msg returns a human-readable string describing the condition and the current
// value of its sample.
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
		val = fmt.Sprintf("%.1f", s.Value)
	}
	return fmt.Sprintf("%s.%s %s %.1f: %s", c.Source, c.Name, c.Op, c.Value, val)
}

// conditionState contains information about a condition's current state.
type conditionState struct {
	// ID uniquely identifying the condition.
	Id string

	// True the condition became active, or zero if inactive.
	ActiveTime time.Time

	// Human-readable string describing the condition and its sample's current
	// value.
	Msg string
}

// alertState describes the current alerting state.
type alertState struct {
	ActiveConditions []conditionState

	// Last time at which conditions were evaluated.
	LastEvalTime time.Time
}

func EvaluateConds(c context.Context, conds []Condition, now time.Time,
	sender string, recipients []string) error {
	log.Debugf(c, "Getting samples for %v condition(s)", len(conds))
	samples, err := getSamplesForConditions(c, conds)
	if err != nil {
		return err
	}
	log.Debugf(c, "Evaluating condition(s) against %v sample(s)", len(samples))
	states, err := getConditionStates(conds, samples, now)
	if err != nil {
		return err
	}
	log.Debugf(c, "Updating alert state")
	start, cont, end, err := updateAlertState(c, states, now)
	if err != nil {
		return err
	}
	if msg := createAlertMessage(sender, recipients, start, cont, end); msg != nil {
		log.Debugf(c, "Sending email: %v", msg.Body)
		return mail.Send(c, msg)
	}
	return nil
}

// getSamplesForConditions queries for and returns the most recent samples
// needed to evaluate conds. The returned map is keyed by "source|name" and
// values may be nil if corresponding samples weren't found in the datastore.
func getSamplesForConditions(c context.Context, conds []Condition) (
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

// getConditionStates returns the current states of conditions. samples is keyed
// by "source|name" and values may be nil.
func getConditionStates(conds []Condition, samples map[string]*common.Sample,
	now time.Time) ([]conditionState, error) {
	states := make([]conditionState, len(conds))
	for i, cond := range conds {
		s := samples[cond.Source+"|"+cond.Name]
		if active, err := cond.active(s, now); err != nil {
			return nil, err
		} else {
			activeTime := time.Time{}
			if active {
				activeTime = now
			}
			states[i] = conditionState{cond.id(), activeTime, cond.msg(s, now)}
		}
	}
	return states, nil
}

// updateAlertState gets the current alerting state, identifies newly-active,
// continuing-to-be-active, and no-longer-active conditions, and saves the
// updated state.
func updateAlertState(c context.Context, ns []conditionState, now time.Time) (
	start, cont, end []conditionState, err error) {
	as := alertState{}
	k := datastore.NewKey(c, alertStateKind, "", alertStateId, nil)
	if err = datastore.Get(c, k, &as); err != nil && err != datastore.ErrNoSuchEntity {
		return nil, nil, nil, err
	}
	om := make(map[string]conditionState)
	if as.ActiveConditions != nil {
		for _, s := range as.ActiveConditions {
			om[s.Id] = s
		}
	}

	start = make([]conditionState, 0)
	cont = make([]conditionState, 0)
	end = make([]conditionState, 0)
	for _, s := range ns {
		if !s.ActiveTime.IsZero() {
			if os, ok := om[s.Id]; ok {
				s.ActiveTime = os.ActiveTime
				cont = append(cont, s)
			} else {
				s.ActiveTime = now
				start = append(start, s)
			}
		} else {
			if os, ok := om[s.Id]; ok {
				s.ActiveTime = os.ActiveTime
				end = append(end, s)
			}
		}
	}

	as.ActiveConditions = append(start, cont...)
	as.LastEvalTime = now
	if _, err = datastore.Put(c, k, &as); err != nil {
		return nil, nil, nil, err
	}
	return start, cont, end, nil
}

func createAlertMessage(sender string, recipients []string, start, cont, end []conditionState) *mail.Message {
	// If nothing's changed, bail out.
	if len(start) == 0 && len(end) == 0 {
		return nil
	}

	fc := func(heading string, states []conditionState) string {
		strs := make([]string, len(states))
		for i, s := range states {
			strs[i] = s.Msg
		}
		return fmt.Sprintf("%s\n%s", heading, strings.Join(strs, "\n"))
	}

	lines := make([]string, 0)
	if len(start) > 0 {
		lines = append(lines, fc("New alerts:", start))
	}
	if len(end) > 0 {
		lines = append(lines, fc("Ended alerts:", end))
	}
	if len(cont) > 0 {
		lines = append(lines, fc("Continuing alerts:", cont))
	}
	body := strings.Join(lines, "\n\n")

	return &mail.Message{
		Sender:  sender,
		To:      recipients,
		Subject: "Alerts updated",
		Body:    body,
	}
}
