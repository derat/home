// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"testing"
	"time"

	"erat.org/home/common"
)

func TestWriteSamples(t *testing.T) {
	c, done, _ := initTest()
	defer done()

	const (
		t1 = 123
		t2 = 456
		s  = "source"
		n1 = "name1"
		n2 = "name2"
	)

	s0 := common.Sample{time.Unix(t1, 0), s, n1, 1.0}
	s1 := common.Sample{time.Unix(t1, 0), s, n2, 2.0}
	if err := WriteSamples(c, []common.Sample{s0, s1}); err != nil {
		t.Errorf("failed to write samples: %v", err)
	}

	s0update := common.Sample{time.Unix(t1, 0), s, n1, 3.0}
	s2 := common.Sample{time.Unix(t2, 0), s, n1, 4.0}
	s3 := common.Sample{time.Unix(t2, 0), s, n2, 5.0}
	if err := WriteSamples(c, []common.Sample{s0update, s2, s3}); err != nil {
		t.Errorf("failed to write samples: %v", err)
	}
	checkSamples(t, c, []common.Sample{s0update, s1, s2, s3})
}
