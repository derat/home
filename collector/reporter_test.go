// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"erat.org/home/common"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	testReportPath        = "/report"
	testReportSecret      = "this is the secret"
	testReportChannelSize = 10
	testReportTimeoutMs   = 5000
)

type testServer struct {
	listener      net.Listener
	ch            chan string
	responseCode  int
	responseDelay time.Duration
}

func (ts *testServer) getReportURL() string {
	return "http://" + ts.listener.Addr().String() + testReportPath
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
		time.Sleep(time.Duration(testReportTimeoutMs) * time.Millisecond)
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
		data := r.PostFormValue("d")
		if r.PostFormValue("s") != common.HashStringWithSHA256(fmt.Sprintf("%s|%s", data, testReportSecret)) {
			http.Error(w, "Bad signature", http.StatusBadRequest)
			return
		}

		ts.ch <- data
		if ts.responseDelay > 0 {
			time.Sleep(ts.responseDelay)
		}
		w.WriteHeader(ts.responseCode)
	default:
		http.NotFound(w, r)
	}
}

func createConfig() *config {
	out := ioutil.Discard
	if testVerbose {
		out = os.Stderr
	}

	cfg, _ := readConfig("", log.New(out, "", log.LstdFlags))
	cfg.ReportSecret = testReportSecret
	return cfg
}

func createTempFile() string {
	f, err := ioutil.TempFile("", "reporter_test.")
	if err != nil {
		panic(err)
	}
	f.Close()
	return f.Name()
}

func getFileSize(p string) int64 {
	fi, err := os.Stat(p)
	if err != nil {
		panic(err)
	}
	return fi.Size()
}

func initTest(t *testing.T, cfg *config) (*testServer, *reporter) {
	ts := &testServer{
		ch:           make(chan string, testReportChannelSize),
		responseCode: http.StatusOK,
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

	ts.responseCode = http.StatusInternalServerError
	s0 := &common.Sample{time.Unix(0, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s0)
	ts.waitForReport(t)

	ts.responseCode = http.StatusOK
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

func TestBackingFile(t *testing.T) {
	cfg := createConfig()
	cfg.BackingFile = createTempFile()
	defer os.Remove(cfg.BackingFile)
	ts, r := initTest(t, cfg)
	defer ts.stop()

	ts.responseCode = http.StatusInternalServerError
	s0 := &common.Sample{time.Unix(0, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s0)
	ts.waitForReport(t)
	r.triggerRetryTimeout()
	ts.waitForReport(t)
	if getFileSize(cfg.BackingFile) == 0 {
		t.Errorf("Backing file not written immediately after failure")
	}
	r.stop()

	// A new reporter should load the backing file and try to report the sample
	// again immediately.
	r = newReporter(cfg)
	r.start()
	str := ts.waitForReport(t)
	if str != s0.String() {
		t.Errorf("Expected %q after restart; saw %q", s0.String(), str)
	}

	// Add a second sample and check that the two are reported in-order next
	// time.
	s1 := &common.Sample{time.Unix(1, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s1)
	r.triggerRetryTimeout()
	str = ts.waitForReport(t)
	exp := common.JoinSamples([]*common.Sample{s0, s1})
	if str != exp {
		t.Errorf("Expected %q on retry; saw %q", exp, str)
	}

	// Add a third sample and stop the reporter before it gets a chance to
	// retry.
	s2 := &common.Sample{time.Unix(2, 0), "SOURCE", "NAME", 10.0}
	r.reportSample(s2)
	r.stop()

	// A new reporter should report all three samples.
	ts.responseCode = http.StatusOK
	r = newReporter(cfg)
	r.start()
	str = ts.waitForReport(t)
	exp = common.JoinSamples([]*common.Sample{s0, s1, s2})
	if str != exp {
		t.Errorf("Expected %q on retry; saw %q", exp, str)
	}
	r.stop()
	if getFileSize(cfg.BackingFile) != 0 {
		t.Errorf("Backing file not cleared after successful write")
	}
}
