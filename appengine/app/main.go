// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package app

import (
	"erat.org/home/appengine/storage"
	"erat.org/home/common"
	"fmt"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/user"
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

	// Path of template file relative to base app directory.
	templatePath = "template.html"

	// Hardcoded secret used when running dev app server.
	devSecret = "secret"

	// Duration of samples to display in graphs.
	defaultGraphSec = 7200
)

// graphLineConfig describes a line within a graph.
type graphLineConfig struct {
	// Label displayed on graph.
	Label string

	// Source and name associated with samples.
	Source string
	Name   string
}

// graphConfig holds configuration for an individual graph.
type graphConfig struct {
	// Graph title.
	Title string

	// Human-units used as label for vertical axis.
	Units string

	// Number of seconds of data to graph.
	Seconds int

	// If true, vertical axis doesn't go below zero.
	MinZero bool

	// If true, graph is shorter than usual.
	Short bool

	// Lines within the graph.
	Lines []graphLineConfig
}

// config holds user-configurable top-level settings.
type config struct {
	// Secret used by collector to sign reports.
	ReportSecret string

	// Email addresses of authorized users.
	Users []string

	// Time zone, e.g. "America/Los_Angeles".
	TimeZone string

	// Page title.
	Title string

	// Graphs to display on page.
	Graphs []graphConfig
}

// templateGraph is used to pass graph information to the template.
type templateGraph struct {
	Id        string
	Title     string
	Units     string
	MinZero   bool
	Short     bool
	QueryPath string
	Seconds   int
}

var cfg *config
var location *time.Location
var tmpl *template.Template

func loadConfig() error {
	var err error
	cfg = &config{}
	if err = common.ReadJson(configPath, cfg); err != nil {
		return err
	}
	if appengine.IsDevAppServer() {
		cfg.ReportSecret = devSecret
	}
	if cfg.TimeZone == "" {
		cfg.TimeZone = "America/Los_Angeles"
	}
	for i := range cfg.Graphs {
		if cfg.Graphs[i].Seconds <= 0 {
			cfg.Graphs[i].Seconds = defaultGraphSec
		}
	}
	if location, err = time.LoadLocation(cfg.TimeZone); err != nil {
		return err
	}
	return nil
}

func init() {
	var err error
	if err = loadConfig(); err != nil {
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

func checkAuth(w http.ResponseWriter, r *http.Request, redirect bool) bool {
	c := appengine.NewContext(r)
	u := user.Current(c)
	if u != nil {
		for _, e := range cfg.Users {
			if u.Email == e {
				return true
			}
		}
		log.Warningf(c, "Got request from invalid user %q", u.Email)
		http.Error(w, "Forbidden", http.StatusForbidden)
	} else {
		log.Warningf(c, "Got unauthorized request")
		if redirect {
			loginURL, _ := user.LoginURL(c, r.URL.String())
			http.Redirect(w, r, loginURL, http.StatusFound)
		} else {
			http.Error(w, "Request requires authorization", http.StatusUnauthorized)
		}
	}

	return false
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r, false) {
		return
	}

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
	if !checkAuth(w, r, true) {
		return
	}

	c := appengine.NewContext(r)
	d := struct {
		Title  string
		Graphs []templateGraph
	}{
		Title:  cfg.Title,
		Graphs: make([]templateGraph, len(cfg.Graphs)),
	}
	for i, g := range cfg.Graphs {
		sns := make([]string, len(g.Lines))
		labels := make([]string, len(g.Lines))
		for j, l := range g.Lines {
			sns[j] = fmt.Sprintf("%s|%s", l.Source, l.Name)
			labels[j] = l.Label
		}
		queryPath := fmt.Sprintf("/query?labels=%s&names=%s",
			strings.Join(labels, ","), strings.Join(sns, ","))

		d.Graphs[i] = templateGraph{
			Id:        fmt.Sprintf("graph%d", i),
			Title:     g.Title,
			Units:     g.Units,
			MinZero:   g.MinZero,
			Short:     g.Short,
			QueryPath: queryPath,
			Seconds:   g.Seconds,
		}
	}

	if err := tmpl.Execute(w, d); err != nil {
		log.Errorf(c, "Executing template failed: %v", err)
		http.Error(w, "Template failed", http.StatusInternalServerError)
	}
}
