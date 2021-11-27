// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/derat/home/common"

	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/aetest"
	"google.golang.org/appengine/v2/datastore"
)

var testLoc *time.Location
var testInst aetest.Instance

func initTest() context.Context {
	var err error
	if testInst == nil {
		testInst, err = aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
		if err != nil {
			panic(err)
		}
	}
	req, err := testInst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	c := appengine.NewContext(req)

	// Clear the datastore.
	keys, err := datastore.NewQuery("").KeysOnly().GetAll(c, nil)
	if err != nil {
		panic(err)
	}
	if err = datastore.DeleteMulti(c, keys); err != nil {
		panic(err)
	}

	return c
}

func lt(year, month, day, hour, min, sec int) time.Time {
	return time.Date(year, time.Month(month), day, hour, min, sec, 0, testLoc)
}

func ld(year, month, day int) time.Time {
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, testLoc)
}

func formatDate(t time.Time) string {
	return t.Format("2006-01-02")
}

func checkSamples(t *testing.T, c context.Context, expected []common.Sample) {
	q := datastore.NewQuery(sampleKind).Order("Timestamp").Order("Source").Order("Name")
	actual := make([]common.Sample, 0)
	if _, err := q.GetAll(c, &actual); err != nil {
		t.Fatalf("Failed to read samples: %v", err)
	}
	as := common.JoinSamples(actual)
	es := common.JoinSamples(expected)
	if as != es {
		t.Errorf("Don't have expected samples:\nexpected: %q\n  actual: %q", es, as)
	}
}

func TestMain(m *testing.M) {
	var err error
	testLoc, err = time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}

	defer func() {
		if testInst != nil {
			testInst.Close()
		}
	}()

	os.Exit(m.Run())
}
