// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"fmt"

	"erat.org/home/common"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

func WriteSamples(c context.Context, samples []common.Sample) error {
	keys := make([]*datastore.Key, len(samples))
	for i, s := range samples {
		id := fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
		keys[i] = datastore.NewKey(c, sampleKind, id, 0, nil)
	}
	_, err := datastore.PutMulti(c, keys, samples)
	return err
}
