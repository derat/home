// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package common

import (
	"fmt"
	"testing"
	"time"
)

const DefaultTime int64 = 12345

func parseString(str string, ut int64, name string, value float64) error {
	var s Sample
	if err := s.Parse(str, time.Unix(DefaultTime, 0)); err != nil {
		return err
	} else if s.Timestamp.Unix() != ut {
		return fmt.Errorf("Expected timestamp %v; got %v", ut, s.Timestamp.Unix())
	} else if s.Name != name {
		return fmt.Errorf("Expected name %q; got %q", name, s.Name)
	} else if s.Value != value {
		return fmt.Errorf("Expected value %v; got %v", value, s.Value)
	}
	return nil
}

func createString(ut int64, name string, value float64) string {
	s := Sample{time.Unix(ut, 0), name, value}
	return s.String()
}

func TestParse(t *testing.T) {
	if err := parseString("123|TEMPERATURE|55.5", 123, "TEMPERATURE", 55.5); err != nil {
		t.Error(err)
	}
	if err := parseString("HUMIDITY|35", DefaultTime, "HUMIDITY", 35); err != nil {
		t.Error(err)
	}

	for _, str := range []string{
		"",
		"NAME",
		"123|NAME",
		"123|NAME|100.0|5",
		"FOO|NAME|100.0",
		"123|NAME|FOO",
	} {
		var s Sample
		if err := s.Parse(str, time.Unix(DefaultTime, 0)); err == nil {
			t.Errorf("Didn't get expected error when parsing %q", str)
		}
	}
}

func TestString(t *testing.T) {
	const exp = "890|NAME|75.5"
	s := Sample{time.Unix(890, 0), "NAME", 75.5}
	if str := s.String(); str != exp {
		t.Errorf("Expected %q; got %q", exp, str)
	}
}
