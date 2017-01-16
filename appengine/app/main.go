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
	"html/template"
	"io/ioutil"
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

	templatePath = "template.html"

	graphSec = 3600
)

type graphLineConfig struct {
	Label  string
	Source string
	Name   string
}

type graphConfig struct {
	Title   string
	Units   string
	MinZero bool
	Short   bool
	Lines   []graphLineConfig
}

type templateGraph struct {
	Id        string
	Title     string
	Units     string
	MinZero   bool
	Short     bool
	QueryPath string
}

type config struct {
	// Secret used to sign reports.
	ReportSecret string

	// Time zone, e.g. "America/Los_Angeles".
	TimeZone string

	Graphs []graphConfig
}

var cfg *config
var location *time.Location
var tmpl *template.Template

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

	data, err := ioutil.ReadFile(templatePath)
	if err != nil {
		panic(err)
	}
	if tmpl, err = template.New(templatePath).Parse(string(data)); err != nil {
		panic(err)
	}

	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/report", handleReport)
	http.HandleFunc("/", handleIndex)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	labels := strings.Split(r.FormValue("labels"), ",")
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

	buf, err := storage.RunQuery(c, labels, sourceNames, start, end, location)
	if err != nil {
		log.Errorf(c, "Query failed: %v", err)
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
		log.Errorf(c, "Failed to write %v sample(s) to datastore: %v", len(samples), err)
		http.Error(w, "Write failed", http.StatusInternalServerError)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	d := struct {
		Graphs []templateGraph
	}{
		Graphs: make([]templateGraph, len(cfg.Graphs)),
	}
	for i, g := range cfg.Graphs {
		id := fmt.Sprintf("graph%d", i)
		sns := make([]string, len(g.Lines))
		labels := make([]string, len(g.Lines))
		for j, l := range g.Lines {
			sns[j] = fmt.Sprintf("%s|%s", l.Source, l.Name)
			labels[j] = l.Label
		}
		start := time.Now().Add(-graphSec * time.Second).Unix()
		end := time.Now().Unix()
		queryPath := fmt.Sprintf("/query?labels=%s&names=%s&start=%d&end=%d",
			strings.Join(labels, ","), strings.Join(sns, ","), start, end)
		d.Graphs[i] = templateGraph{
			Id:        id,
			Title:     g.Title,
			Units:     g.Units,
			MinZero:   g.MinZero,
			Short:     g.Short,
			QueryPath: queryPath,
		}
	}

	if err := tmpl.Execute(w, d); err != nil {
		log.Errorf(c, "Executing template failed: %v", err)
		http.Error(w, "Template failed", http.StatusInternalServerError)
	}
}
