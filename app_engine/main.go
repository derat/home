// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package app

import (
	"erat.org/cloud"
	"erat.org/home/common"
	"erat.org/home/storage"
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"net/http"
	"strings"
	"time"
)

const (
	// Config path relative to base app directory.
	configPath = "config.json"

	// Hardcoded secret used when running dev app server.
	devSecret = "secret"
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

	http.HandleFunc("/query", handleReport)
	http.HandleFunc("/report", handleReport)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	// FIXME: run query
}

func handleReport(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	if r.Method != "POST" {
		log.Warningf(c, "Report has non-POST method %v", r.Method)
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	data := r.PostFormValue("d")
	sig := r.PostFormValue("s")
	if sig != common.HashStringWithSHA256(fmt.Sprintf("%s|%s", data, cfg.ReportSecret)) {
		log.Warningf(c, "Report has bad signature %q", sig)
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	now := time.Now()
	lines := strings.Split(data, "\n")
	samples := make([]common.Sample, len(lines))
	for i, line := range lines {
		s := common.Sample{}
		if err := s.Parse(line, now); err != nil {
			log.Warningf(c, "Report has unparseable sample %q: %v", line, err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
		samples[i] = s
	}

	log.Debugf(c, "Got report with %v sample(s)", len(samples))
	if err := storage.WriteSamples(c, samples); err != nil {
		log.Warningf(c, "Failed to write %v sample(s) to datastore: %v", len(samples), err)
		http.Error(w, "Write failed", http.StatusInternalServerError)
	}
}
