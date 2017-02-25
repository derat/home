// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"sort"
	"strings"
	"testing"
	"time"

	"erat.org/home/common"
)

func TestGetSamplesForConds(t *testing.T) {
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

	m, err := getSamplesForConds(c, []Condition{
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

func TestGetActiveConds(t *testing.T) {
	ceq := Condition{"a", "b", "eq", 1.0}
	cne := Condition{"a", "c", "ne", 2.0}
	clt := Condition{"a", "d", "lt", 3.0}
	cgt := Condition{"a", "e", "gt", 4.0}
	cle := Condition{"a", "f", "le", 5.0}
	cge := Condition{"a", "g", "ge", 6.0}
	conds := []Condition{ceq, cne, clt, cgt, cle, cge}

	cot := Condition{"a", "h", "ot", 300.0}

	now := lt(2017, 1, 2, 3, 0, 0)
	now4 := now.Add(time.Duration(-4) * time.Minute)
	now6 := now.Add(time.Duration(-6) * time.Minute)

	ms := func(t time.Time, s, n string, v float32) common.Sample {
		return common.Sample{t, s, n, v}
	}

	for i, tc := range []struct {
		cs []Condition
		s  []common.Sample
		ec []Condition
	}{
		{conds, []common.Sample{}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "b", 1.0)}, []Condition{ceq}},
		{conds, []common.Sample{ms(now, "a", "b", 1.5)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "b", 0.5)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "c", 2.0)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "c", 2.5)}, []Condition{cne}},
		{conds, []common.Sample{ms(now, "a", "d", 2.0)}, []Condition{clt}},
		{conds, []common.Sample{ms(now, "a", "d", 3.0)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "d", 3.1)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "e", 4.5)}, []Condition{cgt}},
		{conds, []common.Sample{ms(now, "a", "e", 4.0)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "e", 3.0)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "f", 5.0)}, []Condition{cle}},
		{conds, []common.Sample{ms(now, "a", "f", 4.8)}, []Condition{cle}},
		{conds, []common.Sample{ms(now, "a", "f", 5.5)}, []Condition{}},
		{conds, []common.Sample{ms(now, "a", "g", 6.0)}, []Condition{cge}},
		{conds, []common.Sample{ms(now, "a", "g", 6.5)}, []Condition{cge}},
		{conds, []common.Sample{ms(now, "a", "g", 5.5)}, []Condition{}},
		{[]Condition{cot}, []common.Sample{}, []Condition{cot}},
		{[]Condition{cot}, []common.Sample{ms(now, "a", "h", 1.0)}, []Condition{}},
		{[]Condition{cot}, []common.Sample{ms(now4, "a", "h", 1.0)}, []Condition{}},
		{[]Condition{cot}, []common.Sample{ms(now6, "a", "h", 1.0)}, []Condition{cot}},
	} {
		m := make(map[string]*common.Sample)
		for _, s := range tc.s {
			m[s.Source+"|"+s.Name] = &s
		}
		acs, err := getActiveConds(tc.cs, m, now)
		if err != nil {
			t.Errorf("Got error for case %v: %v", i, err)
		} else {
			a := joinActiveConds(acs)
			e := joinConds(tc.ec)
			if a != e {
				t.Errorf("Didn't get expected conditions for case %v:\nexpected: %v\n  actual: %v",
					i, e, a)
			}
		}
	}
}

func joinActiveConds(conds []activeCondition) string {
	cs := make([]string, len(conds))
	for i, c := range conds {
		cs[i] = c.id
	}
	return strings.Join(cs, ",")
}

func joinConds(conds []Condition) string {
	cs := make([]string, len(conds))
	for i, c := range conds {
		cs[i] = c.id()
	}
	return strings.Join(cs, ",")
}
