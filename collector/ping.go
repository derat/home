// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"erat.org/home/common"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const pingPath = "/bin/ping"

// Matches "3 packets transmitted, 3 received, 0% packet loss, time 401ms"
var countRegexp *regexp.Regexp = regexp.MustCompile("(?m)^(\\d+) packets transmitted, (\\d+) received")

// Matches "rtt min/avg/max/mdev = 10.694/13.969/17.825/2.941 ms".
var timeRegexp *regexp.Regexp = regexp.MustCompile("(?m)^rtt min/avg/max/mdev = (\\S+)\\s+(\\S+)")

type pingStats struct {
	// True if the command failed to produce usable output.
	commandFailed bool

	// Minimum, average, and maximum RTT, in milliseconds.
	minReplyMs, avgReplyMs, maxReplyMs float64

	// Fraction of pings not receiving responses in the range [0.0, 1.0].
	packetLoss float64
}

func parseFloats(s []string) ([]float64, error) {
	f := make([]float64, len(s))
	for i := range s {
		var err error
		if f[i], err = strconv.ParseFloat(s[i], 64); err != nil {
			return nil, err
		}
	}
	return f, nil
}

func getPingStats(cfg *config) *pingStats {
	count := strconv.FormatInt(int64(cfg.PingCount), 10)
	delaySec := strconv.FormatFloat(float64(cfg.PingDelayMs)/1000.0, 'f', 3, 32)
	deadlineSec := strconv.FormatInt(int64(cfg.PingTimeoutSec), 10)
	cmd := exec.Command(pingPath, "-c", count, "-i", delaySec, "-w", deadlineSec, "-q", cfg.PingHost)
	out, _ := cmd.CombinedOutput()

	s := &pingStats{}

	var tx, rx float64
	if cm := countRegexp.FindStringSubmatch(string(out)); cm == nil {
		cfg.Logger.Printf("Didn't find ping count in %q", string(out))
		s.commandFailed = true
		return s
	} else if counts, err := parseFloats(cm[1:]); err != nil {
		cfg.Logger.Printf("Failed to parse ping counts from %q: %v", cm[0], err)
		s.commandFailed = true
		return s
	} else {
		tx, rx = counts[0], counts[1]
		if tx > 0 {
			s.packetLoss = (tx - rx) / tx
		}
	}

	// The line with times only shows up if at least one reply was received.
	if rx > 0.0 {
		if tm := timeRegexp.FindStringSubmatch(string(out)); tm == nil {
			cfg.Logger.Printf("Didn't find ping times in %q", string(out))
			s.commandFailed = true
			return s
		} else if times, err := parseFloats(strings.Split(tm[1], "/")); err != nil {
			cfg.Logger.Printf("Failed to parse ping times from %q: %v", tm[1], err)
			s.commandFailed = true
			return s
		} else if len(times) != 4 {
			cfg.Logger.Printf("Expected 4 ping times from %q; got %v", tm[1], len(times))
			s.commandFailed = true
			return s
		} else {
			s.minReplyMs, s.avgReplyMs, s.maxReplyMs = times[0], times[1], times[2]
		}
	}

	return s
}

func runPingLoop(cfg *config, r *reporter) {
	for {
		start := time.Now()
		stats := getPingStats(cfg)

		failedVal := 0.0
		if stats.commandFailed {
			failedVal = 1.0
		}
		r.reportSamples([]*common.Sample{
			{start, cfg.Source, samplePingFailed, failedVal},
			{start, cfg.Source, samplePingMin, stats.minReplyMs},
			{start, cfg.Source, samplePingAvg, stats.avgReplyMs},
			{start, cfg.Source, samplePingMax, stats.maxReplyMs},
			{start, cfg.Source, samplePingPacketLoss, stats.packetLoss},
		})

		next := start.Add(time.Duration(cfg.PingSampleIntervalSec) * time.Second)
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}
	}
}