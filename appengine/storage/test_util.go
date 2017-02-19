// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"erat.org/home/common"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/datastore"
)

func initTest() (c context.Context, done func()) {
	inst, err := aetest.NewInstance(&aetest.Options{StronglyConsistentDatastore: true})
	if err != nil {
		panic(err)
	}
	req, err := inst.NewRequest("GET", "/", nil)
	if err != nil {
		panic(err)
	}
	return appengine.NewContext(req), func() { inst.Close() }
}

func getAllSamples(c context.Context) ([]*common.Sample, error) {
	q := datastore.NewQuery(sampleKind).Order("Timestamp").Order("Source").Order("Name")
	samples := make([]*common.Sample, 0)
	if _, err := q.GetAll(c, &samples); err != nil {
		return nil, err
	}
	return samples, nil
}
