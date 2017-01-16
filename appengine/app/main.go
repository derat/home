// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package app

import (
	"erat.org/cloud"
	"erat.org/home/appengine/storage"
	"erat.org/home/common"
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"net/http"
	"strconv"
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

	// Time zone, e.g. "America/Los_Angeles".
	TimeZone string
}

var cfg *config
var location *time.Location

func init() {
	var err error
	cfg = &config{}
	if err = cloud.ReadJson(configPath, cfg); err != nil {
		panic(err)
	}
	if appengine.IsDevAppServer() {
		cfg.ReportSecret = devSecret
	}
	if location, err = time.LoadLocation(cfg.TimeZone); err != nil {
		panic(err)
	}

	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/report", handleReport)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	sourceNames := strings.Split(r.FormValue("names"), ",")

	parseTime := func(s string) (time.Time, error) {
		t, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Warningf(c, "Query has bad time %q", s)
			http.Error(w, "Bad time", http.StatusBadRequest)
			return time.Time{}, err
		}
		return time.Unix(t, 0), nil
	}
	var start, end time.Time
	var err error
	if start, err = parseTime(r.FormValue("start")); err != nil {
		return
	}
	if end, err = parseTime(r.FormValue("end")); err != nil {
		return
	}

	buf, err := storage.RunQuery(c, sourceNames, start, end, location)
	if err != nil {
		log.Warningf(c, "Query failed: %v", err)
		http.Error(w, "Query failed", http.StatusInternalServerError)
		return
	}
	w.Write(buf.Bytes())
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
