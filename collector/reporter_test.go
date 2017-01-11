// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"erat.org/home/common"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

const reportPath = "/report"
const reportChannelSize = 10
const reportTimeoutMs = 5000

type testServer struct {
	listener net.Listener
	ch       chan string
}

func (ts *testServer) getReportURL() string {
	return "http://" + ts.listener.Addr().String() + reportPath
}

func (ts *testServer) start(t *testing.T) {
	var err error
	ts.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(fmt.Sprintf("Unable to listen: %v", err))
	}
	go http.Serve(ts.listener, ts)
}

func (ts *testServer) stop() {
	ts.listener.Close()
}

func (ts *testServer) waitForReport(t *testing.T) string {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(reportTimeoutMs) * time.Millisecond)
		timeout <- true
	}()

	select {
	case s := <-ts.ch:
		return s
	case <-timeout:
		t.Errorf("Timed out waiting for report")
	}
	return ""
}

func (ts *testServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/report":
		ts.ch <- r.PostFormValue("d")
	default:
		http.NotFound(w, r)
	}
}

func initTest(t *testing.T) (*testServer, *reporter) {
	ts := &testServer{
		ch: make(chan string, reportChannelSize),
	}
	ts.start(t)
	cfg, _ := readConfig("", log.New(os.Stderr, "", log.LstdFlags))
	cfg.ReportURL = ts.getReportURL()
	return ts, newReporter(cfg)
}

func TestReport(t *testing.T) {
	ts, r := initTest(t)
	defer ts.stop()

	r.start()
	defer r.stop()

	s := &common.Sample{time.Now(), "SOURCE", "NAME", 10.0}
	r.reportSample(s)
	str := ts.waitForReport(t)
	if str != s.String() {
		t.Errorf("Expected %q to be reported; saw %q", s.String, str)
	}
}
