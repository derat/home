// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"erat.org/home/common"
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
	c, done := initTest()
	defer done()

	type datarow struct {
		ts string
		v  []float64
	}

	checkQuery := func(labels, sourceNames []string, start, end time.Time, rows []datarow) {
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
		if err := RunQuery(c, b, QueryParams{labels, sourceNames, start, end}); err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		tb := table{}
		if err := json.Unmarshal(b.Bytes(), &tb); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		nc := len(sourceNames) + 1
		if len(tb.Cols) != nc {
			t.Errorf("Got %v column(s) instead of %v", len(tb.Cols), nc)
		} else {
			if tb.Cols[0].Type != "datetime" {
				t.Errorf("Column 0 has type %q instead of %q", tb.Cols[0].Type, "datetime")
			}
			for i := range sourceNames {
				if tb.Cols[i+1].Label != labels[i] {
					t.Errorf("Column %i has label %q instead of %q", tb.Cols[i+1].Label, labels[i])
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

	t1 := time.Unix(1, 0).UTC()
	t2 := time.Unix(2, 0).UTC()
	t3 := time.Unix(3, 0).UTC()
	t4 := time.Unix(4, 0).UTC()
	t5 := time.Unix(5, 0).UTC()
	checkQuery([]string{"B"}, []string{"a|b"}, t2, t4, []datarow{})

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
	checkQuery([]string{"B", "C"}, []string{"a|b", "a|c"}, t2, t4, []datarow{
		{"Date(1970,0,1,0,0,2)", []float64{0.5, 0.75}},
		{"Date(1970,0,1,0,0,3)", []float64{1.0}},
		{"Date(1970,0,1,0,0,4)", []float64{math.NaN(), 1.25}},
	})

	// The start time's location should be used to determine the output's time zone.
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatalf("Failed to load location: %v", err)
	}
	checkQuery([]string{"B", "C"}, []string{"a|b", "a|c"}, t2.In(loc), t4.In(loc), []datarow{
		{"Date(1969,11,31,16,0,2)", []float64{0.5, 0.75}},
		{"Date(1969,11,31,16,0,3)", []float64{1.0}},
		{"Date(1969,11,31,16,0,4)", []float64{math.NaN(), 1.25}},
	})
}
