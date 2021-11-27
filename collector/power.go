// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/derat/home/common"
)

type powerStats struct {
	// True if the system is currently on line power.
	onLine bool
	// Line power voltage.
	lineVoltage float32
	// Percent load capacity in the range [0.0, 100.0].
	loadPercent float32
	// Battery charge percent in the range [0.0, 100.0].
	batteryPercent float32
}

func parsePowerCommandOutput(cfg *config, out string, stats *powerStats) {
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Fields(line)
		if len(parts) != 2 {
			if len(parts) != 0 {
				cfg.logger.Printf("Skipping bad power stats line %q", line)
			}
			continue
		}
		key := parts[0]
		val, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			cfg.logger.Printf("Unable to parse value %q for power stat %q", parts[1], key)
		}
		if key == "on_line" {
			stats.onLine = val > 0.0
		} else if key == "line_voltage" {
			stats.lineVoltage = float32(val)
		} else if key == "load_percent" {
			stats.loadPercent = float32(val)
		} else if key == "battery_percent" {
			stats.batteryPercent = float32(val)
		} else {
			cfg.logger.Printf("Ignoring unknown power stat %q", key)
		}
	}
}

func runPowerLoop(cfg *config, r *reporter) {
	// TODO: Listen to a socket to hear about changes.
	for {
		start := time.Now()

		stats := powerStats{}
		// TODO: Split into arguments?
		cmd := exec.Command(cfg.PowerCommand)
		out, err := cmd.CombinedOutput()
		if err != nil {
			cfg.logger.Printf("Power command %q failed", cfg.PowerCommand)
		} else {
			parsePowerCommandOutput(cfg, string(out), &stats)
			onLineVal := float32(0.0)
			if stats.onLine {
				onLineVal = 1.0
			}
			r.reportSamples([]common.Sample{
				{start, cfg.Source, samplePowerOnLine, onLineVal},
				{start, cfg.Source, samplePowerLineVoltage, stats.lineVoltage},
				{start, cfg.Source, samplePowerLoadPercent, stats.loadPercent},
				{start, cfg.Source, samplePowerBatteryPercent, stats.batteryPercent},
			})
		}

		next := start.Add(time.Duration(cfg.PowerSampleIntervalSec) * time.Second)
		now := time.Now()
		if now.Before(next) {
			time.Sleep(next.Sub(now))
		}
	}
}
