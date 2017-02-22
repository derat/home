// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package test

import (
	"testing"
	"time"

	"erat.org/home/common"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

func InitTest() (c context.Context, done func(), loc *time.Location) {
	loc, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
	if err != nil {
		panic(err)
	}
	req, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	return appengine.NewContext(req), func() { inst.Close() }, loc
}

func CheckSamples(t *testing.T, c context.Context, kind string, expected []common.Sample) {
	q := datastore.NewQuery(kind).Order("Timestamp").Order("Source").Order("Name")
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
