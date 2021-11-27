// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/derat/home/appengine/storage"
	"github.com/derat/home/common"

	"google.golang.org/appengine/v2"
	"google.golang.org/appengine/v2/log"
	"google.golang.org/appengine/v2/user"
)

const (
	// Path of config file relative to base app directory.
	configPath = "config.json"

	// Path of template file relative to base app directory.
	templatePath = "appengine/template.html"
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

func main() {
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

	http.HandleFunc("/eval", wrapError(handleEval))
	http.HandleFunc("/purge", wrapError(handlePurge))
	http.HandleFunc("/query", wrapError(handleQuery))
	http.HandleFunc("/report", wrapError(handleReport))
	http.HandleFunc("/summarize", wrapError(handleSummarize))
	http.HandleFunc("/", wrapError(handleIndex))

	appengine.Main()
}

// checkAuth verifies that r is from an authorized user. If redirect is true,
// requests lacking any user info are redirected to the login URL. Returns false
// and writes an error/redirect to w if the request is not allowed.
// Returns true without writing anything to w if the request is allowed.
func checkAuth(c context.Context, w http.ResponseWriter, r *http.Request, redirect bool) bool {
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

type handlerError struct {
	// HTTP status code.
	status int
	// Message to return in reply.
	msg string
	// More-detailed error to log, or nil.
	err error
}

// wrapError wraps an HTTP handler and handles logging an error and sending an
// HTTP reply if the handler reports an error. If the handler doesn't report an
// error, it is responsible for sending the reply itself before returning.
func wrapError(f func(c context.Context, w http.ResponseWriter,
	r *http.Request) *handlerError) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)
		if herr := f(c, w, r); herr != nil {
			log.Errorf(c, "%s: %v", herr.msg, herr.err)
			http.Error(w, herr.msg, herr.status)
		}
	}
}

func handleEval(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if err := storage.EvaluateConds(c, cfg.AlertConditions, time.Now().In(location),
		cfg.AlertSender, cfg.AlertRecipients); err != nil {
		return &handlerError{500, "Evaluating alert conditions failed", err}
	}
	return nil
}

func handlePurge(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if err := storage.DeleteSummarizedSamples(c, location, cfg.DaysToKeep); err != nil {
		return &handlerError{500, "Purging samples failed", err}
	}
	io.WriteString(w, "purging done\n")
	return nil
}

func handleQuery(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if !checkAuth(c, w, r, false) {
		return nil
	}

	p := storage.QueryParams{}
	p.Labels = strings.Split(r.FormValue("labels"), ",")
	p.SourceNames = strings.Split(r.FormValue("names"), ",")

	var herr *handlerError
	parseTime := func(s string) time.Time {
		if t, err := strconv.ParseInt(s, 10, 64); err != nil {
			herr = &handlerError{400, "Bad time", err}
			return time.Time{}
		} else {
			return time.Unix(t, 0).In(location)
		}
	}
	p.Start = parseTime(r.FormValue("start"))
	p.End = parseTime(r.FormValue("end"))
	if herr != nil {
		return herr
	}

	is := r.FormValue("interval")
	if is != "" {
		if d, err := strconv.ParseInt(is, 10, 64); err != nil || d <= 0 {
			return &handlerError{400, "Bad interval", err}
		} else {
			// This is an pessimistic approximation since we're not checking how
			// far summarization has actually progressed.
			st := time.Now().In(location).AddDate(0, 0, -1*cfg.DaysToKeep)
			p.UpdateGranularityAndAggregation(
				time.Duration(d)*time.Second,
				time.Date(st.Year(), st.Month(), st.Day(), 0, 0, 0, 0, location))
		}
	}

	var b bytes.Buffer
	if err := storage.DoQuery(c, &b, p); err != nil {
		return &handlerError{500, "Query failed", err}
	}
	if _, err := io.Copy(w, &b); err != nil {
		return &handlerError{500, "Failed copying query results", err}
	}
	return nil
}

func handleReport(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if r.Method != "POST" {
		return &handlerError{405, "Invalid method", nil}
	}

	data := r.PostFormValue("d")
	if !appengine.IsDevAppServer() {
		sig := r.PostFormValue("s")
		if sig != common.HashStringWithSHA256(fmt.Sprintf("%s|%s", data, cfg.ReportSecret)) {
			return &handlerError{400, "Bad signature", nil}
		}
	}

	now := time.Now()
	lines := strings.Split(data, "\n")
	samples := make([]common.Sample, len(lines))
	for i, line := range lines {
		s := common.Sample{}
		if err := s.Parse(line, now); err != nil {
			return &handlerError{400, "Bad sample", err}
		}
		samples[i] = s
	}

	log.Debugf(c, "Got report with %v sample(s)", len(samples))
	if err := storage.WriteSamples(c, samples); err != nil {
		return &handlerError{500, "Write failed", err}
	}
	io.WriteString(w, "got it\n")
	return nil
}

func handleSummarize(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if err := storage.GenerateSummaries(c, time.Now().In(location),
		time.Duration(cfg.FullDayDelaySeconds)*time.Second); err != nil {
		return &handlerError{500, "Generating summaries failed", err}
	}
	io.WriteString(w, "summarizing done\n")
	return nil
}

func handleIndex(c context.Context, w http.ResponseWriter, r *http.Request) *handlerError {
	if !checkAuth(c, w, r, true) {
		return nil
	}

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
		return &handlerError{500, "Template failed", err}
	}
	return nil
}
