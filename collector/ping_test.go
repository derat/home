// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func getConfig(host string, count, delayMs, timeoutSec int) *config {
	out := ioutil.Discard
	if testVerbose {
		out = os.Stderr
	}
	return &config{
		PingHost:       host,
		PingCount:      count,
		PingDelayMs:    delayMs,
		PingTimeoutSec: timeoutSec,
		Logger:         log.New(out, "", log.LstdFlags),
	}
}

func TestPing(t *testing.T) {
	s := getPingStats(getConfig("localhost", 3, 200, 10))
	if s.commandFailed {
		t.Errorf("Ping command failed")
	}
	if s.packetLoss != 0.0 {
		t.Errorf("Got nonzero packet loss %f", s.packetLoss)
	}
	if s.minReplyMs <= 0.0 || s.minReplyMs > s.avgReplyMs || s.avgReplyMs > s.maxReplyMs {
		t.Errorf("Got invalid-seeming ping times (min=%f avg=%f max=%f)", s.minReplyMs, s.avgReplyMs, s.maxReplyMs)
	}
}

func TestPingTimeout(t *testing.T) {
	// 203.0.113.0/24 is assigned as "TEST-NET-3" per RFC 5737.
	s := getPingStats(getConfig("203.0.113.0", 3, 200, 1))
	if s.commandFailed {
		t.Errorf("Ping command failed")
	}
	if s.packetLoss != 1.0 {
		t.Errorf("Got non-1.0 packet loss %f", s.packetLoss)
	}
	if s.minReplyMs != 0.0 || s.avgReplyMs != 0.0 || s.maxReplyMs != 0.0 {
		t.Errorf("Got nonzero ping time(s) (min=%f avg=%f max=%f)", s.minReplyMs, s.avgReplyMs, s.maxReplyMs)
	}
}
