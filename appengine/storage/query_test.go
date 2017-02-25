// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"erat.org/home/common"
	"golang.org/x/net/context"
)

func makePoint(t int, value float32) point {
	return point{time.Unix(int64(t), 0), value, nil}
}

func floatSlicesEqual(a, b []float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		a_nan, b_nan := (a[i] != a[i]), (b[i] != b[i])
		if a_nan != b_nan {
			return false
		}
		if !a_nan && a[i] != b[i] {
			return false
		}
	}
	return true
}

type datarow struct {
	ts string
	v  []float64
}

func checkQuery(t *testing.T, c context.Context, p QueryParams, rows []datarow) {
	type col struct {
		Type  string `json:"type"`
		Label string `json:"label"`
	}
	type cell struct {
		Value interface{} `json:"v"`
	}
	type row struct {
		Cells []cell `json:"c"`
	}
	type table struct {
		Cols []col `json:"cols"`
		Rows []row `json:"rows"`
	}

	b := &bytes.Buffer{}
	if err := DoQuery(c, b, p); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	tb := table{}
	if err := json.Unmarshal(b.Bytes(), &tb); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	nc := len(p.SourceNames) + 1
	if len(tb.Cols) != nc {
		t.Errorf("Got %v column(s) instead of %v", len(tb.Cols), nc)
	} else {
		if tb.Cols[0].Type != "datetime" {
			t.Errorf("Column 0 has type %q instead of %q", tb.Cols[0].Type, "datetime")
		}
		for i := range p.SourceNames {
			if tb.Cols[i+1].Label != p.Labels[i] {
				t.Errorf("Column %i has label %q instead of %q", tb.Cols[i+1].Label, p.Labels[i])
			}
			if tb.Cols[i+1].Type != "number" {
				t.Errorf("Column %i has type %q instead of %q", tb.Cols[i+1].Type, "number")
			}
		}
	}

	if len(tb.Rows) != len(rows) {
		t.Errorf("Got %v row(s) instead of %v", len(tb.Rows), len(rows))
	} else {
		for i, exp := range rows {
			act := tb.Rows[i]
			if len(act.Cells) != len(exp.v)+1 {
				t.Errorf("Row %v has %v cell(s) instead of %v", i, len(act.Cells), len(exp.v)+1)
			} else {
				ts := act.Cells[0].Value.(string)
				if ts != exp.ts {
					t.Errorf("Row %v has timestamp %q instead of %q", i, ts, exp.ts)
				}
				for j, ev := range exp.v {
					var av float64
					if act.Cells[j+1].Value == nil {
						av = math.NaN()
					} else {
						av = act.Cells[j+1].Value.(float64)
					}
					av_nan := av != av
					ev_nan := ev != ev
					if av_nan != ev_nan || (!av_nan && av != ev) {
						t.Errorf("Row %v, col %v is %v instead of %v", i, j, av, ev)
					}
				}
			}
		}
	}
}

func TestAveragePoints(t *testing.T) {
	f := func(p point) string {
		return fmt.Sprintf("%v|%.1f", p.timestamp.Unix(), p.value)
	}

	for _, tc := range []struct {
		in  []point
		exp point
	}{
		{[]point{}, point{}},
		{[]point{point{time.Unix(10, 0), 5.0, nil}}, point{time.Unix(10, 0), 5.0, nil}},
		{[]point{
			point{time.Unix(10, 0), 1.0, nil},
			point{time.Unix(20, 0), 2.0, nil},
			point{time.Unix(30, 0), 3.0, nil},
			point{time.Unix(40, 0), 4.0, nil},
		}, point{time.Unix(25, 0), 2.5, nil}},
	} {
		a := f(averagePoints(tc.in))
		e := f(tc.exp)
		if a != e {
			t.Errorf("Expected %v, got %v", e, a)
		}
	}

}

func TestMergeQueryData(t *testing.T) {
	chans := make([]chan point, 6)
	chanData := [][]point{
		{makePoint(1, 0.1), makePoint(2, 0.2), makePoint(5, 0.5)},
		{makePoint(1, 1.1), makePoint(3, 1.3), makePoint(6, 1.6)},
		{makePoint(2, 2.2), makePoint(4, 2.4), makePoint(7, 2.7)},
		{makePoint(5, 3.5)},
		{makePoint(3, 4.3), makePoint(6, 4.6), makePoint(8, 4.8), makePoint(9, 4.9)},
		{},
	}
	for i := range chans {
		chans[i] = make(chan point)
		go func(ch chan point, points []point) {
			for _, p := range points {
				ch <- p
			}
			close(ch)
		}(chans[i], chanData[i])
	}

	out := make(chan timeData)
	go mergeQueryData(chans, out)

	nan := float32(math.NaN())
	for i, exp := range []timeData{
		{time.Unix(1, 0), []float32{0.1, 1.1, nan, nan, nan, nan}, nil},
		{time.Unix(2, 0), []float32{0.2, nan, 2.2, nan, nan, nan}, nil},
		{time.Unix(3, 0), []float32{nan, 1.3, nan, nan, 4.3, nan}, nil},
		{time.Unix(4, 0), []float32{nan, nan, 2.4, nan, nan, nan}, nil},
		{time.Unix(5, 0), []float32{0.5, nan, nan, 3.5, nan, nan}, nil},
		{time.Unix(6, 0), []float32{nan, 1.6, nan, nan, 4.6, nan}, nil},
		{time.Unix(7, 0), []float32{nan, nan, 2.7, nan, nan, nan}, nil},
		{time.Unix(8, 0), []float32{nan, nan, nan, nan, 4.8, nan}, nil},
		{time.Unix(9, 0), []float32{nan, nan, nan, nan, 4.9, nan}, nil},
	} {
		act, more := <-out
		if !more {
			t.Fatalf("Channel closed unexpectedly at index %v", i)
		}
		if act.err != nil {
			t.Fatalf("Got error at index %v: %v", i, act.err)
		}
		if act.timestamp != exp.timestamp {
			t.Errorf("Expected time %v at index %v; saw %v", exp.timestamp, i, act.timestamp)
		}
		if !floatSlicesEqual(exp.values, act.values) {
			t.Errorf("Expected values %v at index %v; saw %v", exp.values, i, act.values)
		}
	}
}

func TestRunQuery(t *testing.T) {
	c := initTest()

	t1 := time.Unix(1, 0).UTC()
	t2 := time.Unix(2, 0).UTC()
	t3 := time.Unix(3, 0).UTC()
	t4 := time.Unix(4, 0).UTC()
	t5 := time.Unix(5, 0).UTC()
	checkQuery(t, c,
		QueryParams{[]string{"B"}, []string{"a|b"}, t2, t4, IndividualSample, 1}, []datarow{})

	if err := WriteSamples(c, []common.Sample{
		common.Sample{t1, "a", "b", 0.25},
		common.Sample{t2, "a", "b", 0.5},
		common.Sample{t2, "a", "c", 0.75},
		common.Sample{t2, "a", "d", 0.8},
		common.Sample{t2, "b", "b", 0.9},
		common.Sample{t3, "a", "b", 1.0},
		common.Sample{t4, "a", "c", 1.25},
		common.Sample{t5, "a", "b", 1.5},
	}); err != nil {
		t.Fatalf("Failed inserting samples: %v", err)
	}
	checkQuery(t, c,
		QueryParams{[]string{"B", "C"}, []string{"a|b", "a|c"}, t2, t4, IndividualSample, 1},
		[]datarow{
			{"Date(1970,0,1,0,0,2)", []float64{0.5, 0.75}},
			{"Date(1970,0,1,0,0,3)", []float64{1.0}},
			{"Date(1970,0,1,0,0,4)", []float64{math.NaN(), 1.25}},
		})

	// The start time's location should be used to determine the output's time zone.
	checkQuery(t, c,
		QueryParams{[]string{"B", "C"}, []string{"a|b", "a|c"},
			t2.In(testLoc), t4.In(testLoc), IndividualSample, 1},
		[]datarow{
			{"Date(1969,11,31,16,0,2)", []float64{0.5, 0.75}},
			{"Date(1969,11,31,16,0,3)", []float64{1.0}},
			{"Date(1969,11,31,16,0,4)", []float64{math.NaN(), 1.25}},
		})
}

func TestRunQuerySummary(t *testing.T) {
	c := initTest()
	if err := WriteSamples(c, []common.Sample{
		common.Sample{lt(2015, 7, 1, 0, 0, 0), "a", "b", 1.0},
		common.Sample{lt(2015, 7, 2, 0, 0, 0), "a", "b", 2.0},
		common.Sample{lt(2015, 7, 3, 0, 0, 0), "a", "b", 3.0},
		common.Sample{lt(2015, 7, 3, 0, 30, 0), "a", "b", 4.0},
		common.Sample{lt(2015, 7, 3, 1, 0, 0), "a", "b", 5.0},
		common.Sample{lt(2015, 7, 3, 1, 30, 0), "a", "b", 6.0},
	}); err != nil {
		t.Fatalf("Failed inserting samples: %v", err)
	}
	if err := GenerateSummaries(c, lt(2015, 7, 4, 0, 0, 0), time.Hour); err != nil {
		t.Fatalf("Failed to generate summaries: %v", err)
	}

	checkQuery(t, c,
		QueryParams{
			[]string{"A"},
			[]string{"a|b"},
			lt(2015, 7, 3, 0, 0, 0),
			lt(2015, 7, 3, 2, 0, 0),
			IndividualSample,
			1,
		},
		[]datarow{
			{"Date(2015,6,3,0,0,0)", []float64{3.0}},
			{"Date(2015,6,3,0,30,0)", []float64{4.0}},
			{"Date(2015,6,3,1,0,0)", []float64{5.0}},
			{"Date(2015,6,3,1,30,0)", []float64{6.0}},
		})

	checkQuery(t, c,
		QueryParams{
			[]string{"A"},
			[]string{"a|b"},
			lt(2015, 7, 3, 0, 0, 0),
			lt(2015, 7, 3, 4, 0, 0),
			HourlyAverage,
			1,
		},
		[]datarow{
			{"Date(2015,6,3,0,0,0)", []float64{3.5}},
			{"Date(2015,6,3,1,0,0)", []float64{5.5}},
		})

	checkQuery(t, c,
		QueryParams{
			[]string{"A"},
			[]string{"a|b"},
			lt(2015, 7, 1, 0, 0, 0),
			lt(2015, 7, 4, 0, 0, 0),
			DailyAverage,
			1,
		},
		[]datarow{
			{"Date(2015,6,1,0,0,0)", []float64{1.0}},
			{"Date(2015,6,2,0,0,0)", []float64{2.0}},
			{"Date(2015,6,3,0,0,0)", []float64{4.5}},
		})
}

func TestRunQueryAggregation(t *testing.T) {
	c := initTest()

	if err := WriteSamples(c, []common.Sample{
		common.Sample{lt(2015, 7, 1, 0, 0, 0), "a", "b", 1.0},
		common.Sample{lt(2015, 7, 1, 0, 1, 0), "a", "b", 2.0},
		common.Sample{lt(2015, 7, 1, 0, 2, 0), "a", "b", 3.0},
		common.Sample{lt(2015, 7, 1, 0, 3, 0), "a", "b", 4.0},
		common.Sample{lt(2015, 7, 1, 0, 4, 0), "a", "b", 5.0},
		common.Sample{lt(2015, 7, 1, 0, 5, 0), "a", "b", 6.0},
	}); err != nil {
		t.Fatalf("Failed inserting samples: %v", err)
	}

	l := []string{"A"}
	sn := []string{"a|b"}
	start := lt(2015, 7, 1, 0, 0, 0)
	end := lt(2015, 7, 2, 0, 0, 0)

	checkQuery(t, c, QueryParams{l, sn, start, end, IndividualSample, 2},
		[]datarow{
			{"Date(2015,6,1,0,0,30)", []float64{1.5}},
			{"Date(2015,6,1,0,2,30)", []float64{3.5}},
			{"Date(2015,6,1,0,4,30)", []float64{5.5}},
		})
	checkQuery(t, c, QueryParams{l, sn, start, end, IndividualSample, 3},
		[]datarow{
			{"Date(2015,6,1,0,1,0)", []float64{2.0}},
			{"Date(2015,6,1,0,4,0)", []float64{5.0}},
		})
	checkQuery(t, c, QueryParams{l, sn, start, end, IndividualSample, 4},
		[]datarow{
			{"Date(2015,6,1,0,1,30)", []float64{2.5}},
			{"Date(2015,6,1,0,4,30)", []float64{5.5}},
		})
	checkQuery(t, c, QueryParams{l, sn, start, end, IndividualSample, 6},
		[]datarow{
			{"Date(2015,6,1,0,2,30)", []float64{3.5}},
		})
}

func TestQueryParamsUpdateGranularityAndAggregation(t *testing.T) {
	min := func(n int) time.Duration {
		return time.Minute * time.Duration(n)
	}

	for _, tc := range []struct {
		start, end     time.Time
		sampleStart    time.Time
		sampleInterval time.Duration
		expGranularity QueryGranularity
		expAggregation int
	}{
		{ld(2015, 1, 1), ld(2015, 1, 1), ld(2015, 1, 1), min(5), IndividualSample, 1},
		{ld(2015, 1, 1), ld(2015, 1, 2), ld(2015, 1, 1), min(5), IndividualSample, 2},
		{ld(2015, 1, 1), ld(2015, 1, 4), ld(2015, 1, 1), min(5), HourlyAverage, 1},
		{ld(2015, 1, 1), ld(2015, 1, 8), ld(2015, 1, 1), min(5), HourlyAverage, 1},
		{ld(2015, 1, 1), ld(2015, 1, 12), ld(2015, 1, 1), min(5), HourlyAverage, 2},
		{ld(2015, 1, 1), ld(2015, 1, 31), ld(2015, 1, 1), min(5), HourlyAverage, 7},
		{ld(2015, 1, 1), ld(2015, 3, 1), ld(2015, 1, 1), min(5), DailyAverage, 1},
		{ld(2015, 1, 1), ld(2015, 8, 1), ld(2015, 1, 1), min(5), DailyAverage, 2},
		{ld(2015, 1, 1), ld(2016, 1, 1), ld(2015, 1, 1), min(5), DailyAverage, 3},
	} {
		qp := QueryParams{Start: tc.start, End: tc.end}
		qp.UpdateGranularityAndAggregation(tc.sampleInterval, tc.sampleStart)
		if qp.Granularity != tc.expGranularity || qp.Aggregation != tc.expAggregation {
			t.Errorf("Bad result(s) for %v-%v: granularity %v (exp %v), aggregation %v (exp %v)",
				formatDate(tc.start), formatDate(tc.end), qp.Granularity, tc.expGranularity,
				qp.Aggregation, tc.expAggregation)
		}
	}
}
