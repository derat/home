// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package test

import (
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
)

func InitTest() (c context.Context, done func()) {
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
