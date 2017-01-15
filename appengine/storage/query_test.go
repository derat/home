// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"math"
	"testing"
	"time"
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
