// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package common

import (
	"crypto/sha256"
	"encoding/hex"
)

func HashStringWithSHA256(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}
