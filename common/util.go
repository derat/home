// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package common

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
)

func HashStringWithSHA256(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func ReadJson(path string, out interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	d := json.NewDecoder(f)
	if err = d.Decode(out); err != nil {
		return err
	}
	return nil
}
