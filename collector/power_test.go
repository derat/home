// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func getPowerStatsJSON(t *testing.T, stats *powerStats) string {
	b, err := json.Marshal(stats)
	if err != nil {
		t.Errorf("Failed to marshal to JSON")
	}
	return string(b)
}

func TestParsePowerCommandOutput(t *testing.T) {
	lo := ioutil.Discard
	if testVerbose {
		lo = os.Stderr
	}
	cfg := &config{
		Logger: log.New(lo, "", log.LstdFlags),
	}

	o := `
on_line 1
line_voltage 119.5
load_percent 17.2
battery_percent 100.0
`
	e := &powerStats{
		OnLine:         true,
		LineVoltage:    119.5,
		LoadPercent:    17.2,
		BatteryPercent: 100.0,
	}
	a := &powerStats{}
	parsePowerCommandOutput(cfg, o, a)
	ej := getPowerStatsJSON(t, e)
	aj := getPowerStatsJSON(t, a)
	if ej != aj {
		t.Errorf("Expected %v; got %v", ej, aj)
	}

	o = `
on_line 0
line_voltage 0.0
load_percent 21.5
battery_percent 65.5
`
	e = &powerStats{
		OnLine:         false,
		LineVoltage:    0.0,
		LoadPercent:    21.5,
		BatteryPercent: 65.5,
	}
	a = &powerStats{}
	parsePowerCommandOutput(cfg, o, a)
	ej = getPowerStatsJSON(t, e)
	aj = getPowerStatsJSON(t, a)
	if ej != aj {
		t.Errorf("Expected %v; got %v", ej, aj)
	}

	o = `
foo 2
blah blah 5

abc
`
	e = &powerStats{
		OnLine:         false,
		LineVoltage:    0.0,
		LoadPercent:    0.0,
		BatteryPercent: 0.0,
	}
	a = &powerStats{}
	parsePowerCommandOutput(cfg, o, a)
	ej = getPowerStatsJSON(t, e)
	aj = getPowerStatsJSON(t, a)
	if ej != aj {
		t.Errorf("Expected %v; got %v", ej, aj)
	}
}
