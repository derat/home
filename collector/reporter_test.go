// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"erat.org/home/common"
	"fmt"
	"log"
	"math"
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
	listener      net.Listener
	ch            chan string
	responseCode  int
	responseDelay time.Duration
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
		if ts.responseDelay > 0 {
			time.Sleep(ts.responseDelay)
		}
		w.WriteHeader(ts.responseCode)
	default:
		http.NotFound(w, r)
	}
}

func createConfig() *config {
	cfg, _ := readConfig("", log.New(os.Stderr, "", log.LstdFlags))
	return cfg
}

func initTest(t *testing.T, cfg *config) (*testServer, *reporter) {
	ts := &testServer{
		ch:           make(chan string, reportChannelSize),
		responseCode: 200,
	}
	ts.start(t)

	cfg.ReportURL = ts.getReportURL()
	r := newReporter(cfg)
	r.start()

	return ts, r
}

func cleanUpTest(ts *testServer, r *reporter) {
	ts.stop()
	r.stop()
}

func TestReport(t *testing.T) {
	ts, r := initTest(t, createConfig())
	defer cleanUpTest(ts, r)

	s := &common.Sample{time.Unix(123, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s)
	str := ts.waitForReport(t)
	if str != s.String() {
		t.Errorf("Expected %q to be reported; saw %q", s.String(), str)
	}

	samples := []*common.Sample{
		&common.Sample{time.Unix(123, 0), "INSIDE", "HUMIDITY", 35.5},
		&common.Sample{time.Unix(456, 0), "OUTSIDE", "TEMP", 65.0},
	}
	r.reportSamples(samples)
	str = ts.waitForReport(t)
	if str != common.JoinSamples(samples) {
		t.Errorf("Expected %q to be reported; saw %q", common.JoinSamples(samples), str)
	}
}

func TestBatching(t *testing.T) {
	cfg := createConfig()
	cfg.ReportBatchSize = 3
	ts, r := initTest(t, cfg)
	defer cleanUpTest(ts, r)

	samples := make([]*common.Sample, cfg.ReportBatchSize*3+1)
	for i := range samples {
		samples[i] = &common.Sample{time.Unix(int64(i), 0), "SOURCE", "NAME", 10.0}
	}

	r.reportSamples(samples)
	numBatches := int(math.Ceil(float64(len(samples)) / float64(cfg.ReportBatchSize)))
	for i := 0; i < numBatches; i++ {
		start := cfg.ReportBatchSize * i
		end := int(math.Min(float64(len(samples)), float64(cfg.ReportBatchSize*(i+1))))
		exp := common.JoinSamples(samples[start:end])
		str := ts.waitForReport(t)
		if str != exp {
			t.Errorf("Expected %q for batch %v; saw %q", exp, i, str)
		}
	}
}

func TestRetry(t *testing.T) {
	ts, r := initTest(t, createConfig())
	defer cleanUpTest(ts, r)

	ts.responseCode = 500
	s0 := &common.Sample{time.Unix(0, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s0)
	ts.waitForReport(t)

	ts.responseCode = 200
	s1 := &common.Sample{time.Unix(1, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s1)
	r.triggerRetryTimeout()
	str := ts.waitForReport(t)
	exp := common.JoinSamples([]*common.Sample{s0, s1})
	if str != exp {
		t.Errorf("Expected %q on retry; saw %q", exp, str)
	}
}

func TestTimeout(t *testing.T) {
	cfg := createConfig()
	cfg.ReportTimeoutMs = 100
	ts, r := initTest(t, cfg)
	defer cleanUpTest(ts, r)
	ts.responseDelay = time.Duration(cfg.ReportTimeoutMs+50) * time.Millisecond

	s := &common.Sample{time.Unix(1, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s)
	ts.waitForReport(t)

	ts.responseDelay = 0
	r.triggerRetryTimeout()
	str := ts.waitForReport(t)
	if str != s.String() {
		t.Errorf("Expected %q on retry; saw %q", s.String(), str)
	}
}
