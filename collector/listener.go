// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"net/http"
	"strings"
	"time"

	"github.com/derat/home/common"
)

type listener struct {
	cfg *config
	rep *reporter
}

func (l *listener) run() error {
	http.HandleFunc("/report", l.handleReport)
	l.cfg.Logger.Printf("Listening at %v", l.cfg.ListenAddress)
	return http.ListenAndServe(l.cfg.ListenAddress, nil)
}

func (l *listener) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		l.cfg.Logger.Printf("Report has non-POST method %v", r.Method)
		http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
		return
	}

	now := time.Now()
	lines := strings.Split(r.PostFormValue("d"), "\n")
	samples := make([]common.Sample, len(lines))
	for i, line := range lines {
		if err := samples[i].Parse(line, now); err != nil {
			l.cfg.Logger.Printf("Report has unparseable sample %q: %v", line, err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
	}

	if len(samples) == 0 {
		l.cfg.Logger.Printf("Report doesn't contain any samples")
		http.Error(w, "Bad request", http.StatusBadRequest)
	}

	l.rep.reportSamples(samples)
	w.Write([]byte("LGTM"))
}
