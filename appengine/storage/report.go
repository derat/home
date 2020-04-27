// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package storage

import (
	"context"
	"fmt"

	"github.com/derat/home/common"

	"google.golang.org/appengine/datastore"
)

// WriteSamples writes samples to datastore.
func WriteSamples(c context.Context, samples []common.Sample) error {
	keys := make([]*datastore.Key, len(samples))
	for i, s := range samples {
		keys[i] = datastore.NewKey(c, sampleKind, getSampleId(&s), 0, nil)
	}
	_, err := datastore.PutMulti(c, keys, samples)
	return err
}

// getSampleId returns the ID that should be used for inserting s into
// datastore. It cannot be changed.
func getSampleId(s *common.Sample) string {
	return fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
}
