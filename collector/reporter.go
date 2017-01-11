// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package collector

import (
	"erat.org/home/common"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type reporter struct {
	cfg *config

	// Samples that have not yet been sent to the server.
	samples []*common.Sample

	// Used to signal the reporter goroutine when samples is non-empty.
	// Protects samples.
	cond *sync.Cond

	retryTimeout chan bool

	stopping bool
}

func newReporter(cfg *config) *reporter {
	r := &reporter{
		cfg:          cfg,
		samples:      make([]*common.Sample, 0),
		cond:         sync.NewCond(new(sync.Mutex)),
		retryTimeout: make(chan bool, 2),
	}

	if _, err := os.Stat(cfg.BackingPath); err == nil {
		// FIXME: Read queued samples.
	}

	return r
}

func (r *reporter) start() {
	go r.processSamples()
}

func (r *reporter) stop() {
	r.cond.L.Lock()
	r.stopping = true
	r.cond.L.Unlock()
	r.cond.Signal()
	r.triggerRetryTimeout()
}

func (r *reporter) reportSample(s *common.Sample) {
	r.cfg.Logger.Printf("Queuing %v", s.String())
	r.cond.L.Lock()
	r.samples = append(r.samples, s)
	r.cond.L.Unlock()
	r.cond.Signal()
}

func (r *reporter) triggerRetryTimeout() {
	// Create a new channel so the real timeout is ignored later.
	ch := r.retryTimeout
	r.retryTimeout = make(chan bool, 2)
	ch <- true
}

func (r *reporter) processSamples() {
	for {
		r.cond.L.Lock()
		for len(r.samples) == 0 && !r.stopping {
			r.cond.Wait()
		}
		if r.stopping {
			r.cfg.Logger.Printf("Reporter loop exiting")
			// FIXME: Rewrite backing file?
			return
		}
		samples := r.samples
		r.samples = make([]*common.Sample, 0)
		r.cond.L.Unlock()

		r.cfg.Logger.Printf("Took %v sample(s) from queue", len(samples))

		gotError := false
		for len(samples) > 0 {
			n := int(math.Min(float64(len(samples)), float64(r.cfg.ReportBatchSize)))
			s := samples[:n]
			if err := r.sendSamplesToServer(s); err != nil {
				r.cfg.Logger.Printf("Got error when reporting samples: %v", err)
				gotError = true
				break
			}
			r.cfg.Logger.Printf("Successfully reported %v sample(s)", len(s))
			samples = samples[n:]
		}

		r.cond.L.Lock()
		if gotError {
			// Return any samples that weren't forwarded successfully back to the
			// beginning of the queue.
			r.cfg.Logger.Printf("Returning %v unreported sample(s) to queue", len(samples))
			r.samples = append(samples, r.samples...)
		}
		// FIXME: Rewrite the backing file if needed.
		r.cond.L.Unlock()

		if gotError {
			r.cfg.Logger.Printf("Sleeping for %v ms after failure", r.cfg.ReportRetryDelayMs)
			go func(ch chan bool) {
				time.Sleep(time.Duration(r.cfg.ReportRetryDelayMs) * time.Millisecond)
				ch <- true
			}(r.retryTimeout)

			select {
			case <-r.retryTimeout:
			}
		}
	}

}

func (r *reporter) sendSamplesToServer(samples []*common.Sample) error {
	data := make([]string, len(samples))
	for i, s := range samples {
		data[i] = s.String()
	}
	resp, err := http.PostForm(r.cfg.ReportURL, url.Values{"d": {strings.Join(data, "\n")}})
	if err != nil {
		return err
	} else if resp.StatusCode != 200 {
		return fmt.Errorf("Got %v", resp.Status)
	}
	return nil
}
