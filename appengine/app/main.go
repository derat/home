// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package app

import (
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"erat.org/home/appengine/storage"
	"erat.org/home/common"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/user"
)

const (
	// Path of config file relative to base app directory.
	configPath = "config.json"

	// Path of template file relative to base app directory.
	templatePath = "template.html"
)

// templateGraph is used to pass graph information to the template.
type templateGraph struct {
	Id             string
	Title          string
	Units          string
	HasMin, HasMax bool
	Min, Max       float32
	Short          bool
	QueryPath      string
	Seconds        int
	ReportSeconds  int
}

var cfg *config
var location *time.Location
var tmpl *template.Template

func init() {
	var err error
	if cfg, location, err = loadConfig(configPath); err != nil {
		panic(err)
	}

	data, err := ioutil.ReadFile(templatePath)
	if err != nil {
		panic(err)
	}
	if tmpl, err = template.New(templatePath).Parse(string(data)); err != nil {
		panic(err)
	}

	http.HandleFunc("/purge", handlePurge)
	http.HandleFunc("/query", handleQuery)
	http.HandleFunc("/report", handleReport)
	http.HandleFunc("/summarize", handleSummarize)
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

func handlePurge(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if err := storage.DeleteSummarizedSamples(c, location, cfg.DaysToKeep); err != nil {
		log.Errorf(c, "Purging samples failed: %v", err)
		http.Error(w, "Purging samples failed", http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "purging done\n")
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if !checkAuth(w, r, false) {
		return
	}

	c := appengine.NewContext(r)

	p := storage.QueryParams{}
	p.Labels = strings.Split(r.FormValue("labels"), ",")
	p.SourceNames = strings.Split(r.FormValue("names"), ",")

	if r.FormValue("summary") == "day" {
		p.Granularity = storage.DailyAverage
	} else if r.FormValue("summary") == "hour" {
		p.Granularity = storage.HourlyAverage
	} else {
		p.Granularity = storage.IndividualSample
	}

	parseTime := func(s string) (time.Time, error) {
		t, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Warningf(c, "Query has bad time %q", s)
			http.Error(w, "Bad time", http.StatusBadRequest)
			return time.Time{}, err
		}
		return time.Unix(t, 0).In(location), nil
	}
	var err error
	if p.Start, err = parseTime(r.FormValue("start")); err != nil {
		return
	}
	if p.End, err = parseTime(r.FormValue("end")); err != nil {
		return
	}

	// TODO: Passing the ResponseWriter here will probably produce malformed
	// responses when errors are encountered mid-response. If that ends up being
	// a problem, either use an intermediate buffer or add a way to communicate
	// errors mid-response.
	if err = storage.RunQuery(c, w, p); err != nil {
		log.Errorf(c, "Query failed: %v", err)
		http.Error(w, "Query failed", http.StatusInternalServerError)
	}
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
		return
	}
	io.WriteString(w, "got it\n")
}

func handleSummarize(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	if err := storage.GenerateSummaries(c, time.Now().In(location),
		time.Duration(cfg.FullDayDelaySeconds)*time.Second); err != nil {
		log.Errorf(c, "Generating summaries failed: %v", err)
		http.Error(w, "Generating summaries failed", http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "summarizing done\n")
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
			Id:            fmt.Sprintf("graph%d", i),
			Title:         g.Title,
			Units:         g.Units,
			Short:         g.Short,
			QueryPath:     queryPath,
			Seconds:       g.Seconds,
			ReportSeconds: g.ReportSeconds,
		}

		if g.Range != nil && len(g.Range) > 0 {
			d.Graphs[i].HasMin = true
			d.Graphs[i].Min = g.Range[0]
		}
		if g.Range != nil && len(g.Range) > 1 {
			d.Graphs[i].HasMax = true
			d.Graphs[i].Max = g.Range[1]
		}
	}

	if err := tmpl.Execute(w, d); err != nil {
		log.Errorf(c, "Executing template failed: %v", err)
		http.Error(w, "Template failed", http.StatusInternalServerError)
	}
}
