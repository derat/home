// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/appengine/mail"

	"erat.org/home/common"
)

func TestGetSamplesForConditions(t *testing.T) {
	c := initTest()
	samples := []common.Sample{
		common.Sample{lt(2015, 7, 1, 0, 0, 0), "a", "b", 1.0},
		common.Sample{lt(2015, 7, 1, 0, 1, 0), "a", "b", 2.0},
		common.Sample{lt(2015, 7, 1, 0, 2, 0), "a", "b", 3.0},
		common.Sample{lt(2015, 7, 1, 0, 0, 0), "a", "c", 4.0},
	}
	if err := WriteSamples(c, samples); err != nil {
		t.Fatalf("Failed inserting samples: %v", err)
	}

	m, err := getSamplesForConditions(c, []Condition{
		Condition{"a", "b", "gt", 1.0},
		Condition{"a", "c", "lt", 1.0},
		Condition{"a", "d", "eq", 1.0},
	})
	if err != nil {
		t.Fatalf("Failed to get recent samples: %v", err)
	}
	actual := make([]common.Sample, 0, len(m))
	for _, s := range m {
		if s != nil {
			actual = append(actual, *s)
		}
	}
	sort.Sort(common.SampleArray(actual))
	as := common.JoinSamples(actual)
	es := common.JoinSamples([]common.Sample{samples[3], samples[2]})
	if as != es {
		t.Errorf("Recent samples didn't match:\nexpected: %q\n  actual: %q", es, as)
	}
}

func TestGetConditionStates(t *testing.T) {
	ms := func(t time.Time, s, n string, v float32) common.Sample {
		return common.Sample{t, s, n, v}
	}
	mcs := func(cond Condition, at time.Time) conditionState {
		return conditionState{cond.id(), at, ""}
	}

	type as []common.Sample
	type ac []Condition
	type acs []conditionState

	const (
		a = "a"
		b = "b"
	)

	tz := time.Time{}
	t0 := time.Unix(0, 0)
	t4 := time.Unix(4, 0)
	t5 := time.Unix(5, 0)
	t6 := time.Unix(6, 0)

	ceq := Condition{a, b, "eq", 1}
	cne := Condition{a, b, "ne", 1}
	clt := Condition{a, b, "lt", 1}
	cgt := Condition{a, b, "gt", 1}
	cle := Condition{a, b, "le", 1}
	cge := Condition{a, b, "ge", 1}
	cot := Condition{a, b, "ot", 5}

	for i, tc := range []struct {
		now     time.Time
		conds   ac
		samples as
		states  acs
	}{
		// No conditions.
		{t0, ac{}, as{}, acs{}},

		// Basic comparisons with arithmetic operators.
		{t0, ac{ceq}, as{ms(t0, a, b, 1)}, acs{mcs(ceq, t0)}},
		{t0, ac{ceq}, as{ms(t0, a, b, 2)}, acs{mcs(ceq, tz)}},
		{t0, ac{cne}, as{ms(t0, a, b, 1)}, acs{mcs(cne, tz)}},
		{t0, ac{cne}, as{ms(t0, a, b, 2)}, acs{mcs(cne, t0)}},
		{t0, ac{clt}, as{ms(t0, a, b, 0)}, acs{mcs(clt, t0)}},
		{t0, ac{clt}, as{ms(t0, a, b, 1)}, acs{mcs(clt, tz)}},
		{t0, ac{clt}, as{ms(t0, a, b, 2)}, acs{mcs(clt, tz)}},
		{t0, ac{cgt}, as{ms(t0, a, b, 0)}, acs{mcs(cgt, tz)}},
		{t0, ac{cgt}, as{ms(t0, a, b, 1)}, acs{mcs(cgt, tz)}},
		{t0, ac{cgt}, as{ms(t0, a, b, 2)}, acs{mcs(cgt, t0)}},
		{t0, ac{cle}, as{ms(t0, a, b, 0)}, acs{mcs(cle, t0)}},
		{t0, ac{cle}, as{ms(t0, a, b, 1)}, acs{mcs(cle, t0)}},
		{t0, ac{cle}, as{ms(t0, a, b, 2)}, acs{mcs(cle, tz)}},
		{t0, ac{cge}, as{ms(t0, a, b, 0)}, acs{mcs(cge, tz)}},
		{t0, ac{cge}, as{ms(t0, a, b, 1)}, acs{mcs(cge, t0)}},
		{t0, ac{cge}, as{ms(t0, a, b, 2)}, acs{mcs(cge, t0)}},

		// Missing samples with arithmetic operators.
		{t0, ac{ceq}, as{}, acs{mcs(ceq, tz)}},
		{t0, ac{cne}, as{}, acs{mcs(cne, tz)}},
		{t0, ac{clt}, as{}, acs{mcs(clt, tz)}},
		{t0, ac{cgt}, as{}, acs{mcs(cgt, tz)}},
		{t0, ac{cle}, as{}, acs{mcs(cle, tz)}},
		{t0, ac{cge}, as{}, acs{mcs(cge, tz)}},

		// "Older than" operator.
		{t0, ac{cot}, as{}, acs{mcs(cot, t0)}},
		{t0, ac{cot}, as{ms(t0, a, b, 1)}, acs{mcs(cot, tz)}},
		{t4, ac{cot}, as{ms(t0, a, b, 1)}, acs{mcs(cot, tz)}},
		{t5, ac{cot}, as{ms(t0, a, b, 1)}, acs{mcs(cot, tz)}},
		{t6, ac{cot}, as{ms(t0, a, b, 1)}, acs{mcs(cot, t6)}},

		// Multiple conditions.
		{t0, ac{ceq, cne, cle}, as{ms(t0, a, b, 1)}, acs{mcs(ceq, t0), mcs(cne, tz), mcs(cle, t0)}},
	} {
		m := make(map[string]*common.Sample)
		for _, s := range tc.samples {
			m[s.Source+"|"+s.Name] = &s
		}
		states, err := getConditionStates([]Condition(tc.conds), m, tc.now)
		if err != nil {
			t.Errorf("Got error for case %v: %v", i, err)
		} else {
			e := joinConditionStates([]conditionState(tc.states))
			a := joinConditionStates(states)
			if a != e {
				t.Errorf("Didn't get expected condition states for case %v:\nexpected: %v\n  actual: %v",
					i, e, a)
			}
		}
	}
}

func TestUpdateAlertState(t *testing.T) {
	c := initTest()

	type acs []conditionState

	checkStates := func(now time.Time, states, expStart, expCont, expEnd acs) {
		start, cont, end, err := updateAlertState(c, []conditionState(states), now)
		if err != nil {
			t.Errorf("Got error at %v: %v", now.Unix(), err)
			return
		}
		as := joinConditionStates([]conditionState(start))
		ac := joinConditionStates([]conditionState(cont))
		ae := joinConditionStates([]conditionState(end))
		es := joinConditionStates([]conditionState(expStart))
		ec := joinConditionStates([]conditionState(expCont))
		ee := joinConditionStates([]conditionState(expEnd))
		if as != es || ac != ec || ae != ee {
			t.Errorf("Conditions don't match at %v:\n"+
				"expected started: %v\n  actual started: %v\n"+
				"expected continued: %v\n  actual continued: %v\n"+
				"expected ended: %v\n  actual ended: %v",
				now.Unix(), es, as, ec, ac, ee, ae)
		}
	}

	const (
		aid = "a"
		bid = "b"
		cid = "c"
	)

	tz := time.Time{}

	// At t0, a is active and b isn't.
	t0 := time.Unix(0, 0)
	a0 := conditionState{aid, t0, ""}
	b0 := conditionState{bid, tz, ""}
	checkStates(t0, acs{a0, b0}, acs{a0}, acs{}, acs{})

	// At t1, a remains active and b becomes active.
	t1 := time.Unix(1, 0)
	a1 := conditionState{aid, t1, ""}
	b1 := conditionState{bid, t1, ""}
	checkStates(t1, acs{a1, b1}, acs{b1}, acs{a0}, acs{})

	// At t2, a becomes inactive and b remains active.
	t2 := time.Unix(2, 0)
	a2 := conditionState{aid, tz, ""}
	b2 := conditionState{bid, t2, ""}
	checkStates(t2, acs{a2, b2}, acs{}, acs{b1}, acs{a0})

	// At t3, b also becomes inactive.
	t3 := time.Unix(3, 0)
	a3 := conditionState{aid, tz, ""}
	b3 := conditionState{bid, tz, ""}
	checkStates(t3, acs{a3, b3}, acs{}, acs{}, acs{b1})

	// At t4, both remain inactive.
	t4 := time.Unix(4, 0)
	a4 := conditionState{aid, tz, ""}
	b4 := conditionState{bid, tz, ""}
	checkStates(t4, acs{a4, b4}, acs{}, acs{}, acs{})

	// At t5, replace the existing conditions with a new one that's active.
	t5 := time.Unix(5, 0)
	c5 := conditionState{cid, t5, ""}
	checkStates(t5, acs{c5}, acs{c5}, acs{}, acs{})

	// At t6, remove the new condition.
	t6 := time.Unix(6, 0)
	checkStates(t6, acs{}, acs{}, acs{}, acs{})
}

func TestCreateAlertMessage(t *testing.T) {
	const (
		recipient = "recipiet@example.com"
		sender    = "sender@example.com"
		cm        = "foo"
	)

	recipients := []string{recipient}
	empty := []conditionState{}
	nonempty := []conditionState{conditionState{"", time.Time{}, cm}}

	if msg := createAlertMessage(sender, recipients, empty, empty, empty); msg != nil {
		t.Errorf("Created unexpected message")
	}
	if msg := createAlertMessage(sender, recipients, empty, nonempty, empty); msg != nil {
		t.Errorf("Created unexpected message")
	}

	checkMsg := func(start, cont, end []conditionState, body string) {
		var msg *mail.Message
		if msg = createAlertMessage(sender, recipients, start, cont, end); msg == nil {
			t.Errorf("Message wasn't created")
			return
		}
		if msg.Sender != sender {
			t.Errorf("Expected sender %q, got %q", sender, msg.Sender)
		}
		if strings.Join(msg.To, ",") != recipient {
			t.Errorf("Expected recipient %q, got %q", recipient, strings.Join(msg.To, ","))
		}
		if msg.Body != body {
			t.Errorf("Expected body %q, got %q", body, msg.Body)
		}
	}

	checkMsg(nonempty, empty, empty, "New alerts:\nfoo")
	checkMsg(empty, empty, nonempty, "Ended alerts:\nfoo")
	checkMsg(nonempty, nonempty, empty, "New alerts:\nfoo\n\nContinuing alerts:\nfoo")
	checkMsg(nonempty, nonempty, nonempty, "New alerts:\nfoo\n\nEnded alerts:\nfoo\n\nContinuing alerts:\nfoo")
}

func joinConditionStates(states []conditionState) string {
	as := make([]string, len(states))
	for i, state := range states {
		as[i] = fmt.Sprintf("%v|%v", state.Id, state.ActiveTime.Unix())
	}
	sort.Sort(sort.StringSlice(as))
	return strings.Join(as, ",")
}
