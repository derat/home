// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package appengine

import (
	"appengine"
	"appengine/datastore"
	"erat.org/cloud"
	"erat.org/home/common"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	// Config path relative to base app directory.
	configPath = "config.json"

	// Hardcoded secret used when running dev app server.
	devSecret = "secret"

	// Datastore kind for sample entities.
	sampleKind = "Sample"
)

type config struct {
	// Secret used to sign reports.
	ReportSecret string
}

var cfg *config

func init() {
	cfg = &config{}
	if err := cloud.ReadJson(configPath, cfg); err != nil {
		panic(err)
	}
	if appengine.IsDevAppServer() {
		cfg.ReportSecret = devSecret
	}

	http.HandleFunc("/report", handleReport)
}

func handleReport(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if r.Method != "POST" {
		c.Warningf("Report has non-POST method %v", r.Method)
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	data := r.PostFormValue("d")
	sig := r.PostFormValue("s")
	if sig != common.HashStringWithSHA256(fmt.Sprintf("%s|%s", data, cfg.ReportSecret)) {
		c.Warningf("Report has bad signature %q", sig)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	now := time.Now()
	lines := strings.Split(data, "\n")
	samples := make([]common.Sample, len(lines))
	for i, line := range lines {
		s := common.Sample{}
		if err := s.Parse(line, now); err != nil {
			c.Warningf("Report has unparseable sample %q: %v", line, err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		samples[i] = s
	}

	c.Debugf("Got report with %v sample(s)", len(samples))

	keys := make([]*datastore.Key, len(samples))
	for i, s := range samples {
		id := fmt.Sprintf("%d|%s|%s", s.Timestamp.Unix(), s.Source, s.Name)
		keys[i] = datastore.NewKey(c, sampleKind, id, 0, nil)
	}
	if _, err := datastore.PutMulti(c, keys, samples); err != nil {
		c.Warningf("Failed to write %v sample(s) to datastore: %v", len(samples), err)
		http.Error(w, "Write failed", http.StatusInternalServerError)
		return
	}
}
