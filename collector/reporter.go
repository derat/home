// Copyright 2017 Daniel Erat <dan@erat.org>
// All rights reserved.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/derat/home/common"
)

const tempBackingFileExtension = ".new"

type reporter struct {
	cfg *config

	client *http.Client

	// Samples that have not yet been sent to the server.
	queuedSamples []common.Sample

	// Samples that are listed in the backing file.
	backingFileSamples []common.Sample

	// Used to signal the reporter goroutine when samples is non-empty.
	// Protects samples and stopping.
	cond *sync.Cond

	// Used by the reporter goroutine to delay retries after errors.
	retryTimeout chan bool

	// Set to true to tell the reporter goroutine should exit.
	stopping bool

	// Used to wait for the reporter goroutine to exit when stop is called.
	wg sync.WaitGroup
}

func newReporter(cfg *config) *reporter {
	r := &reporter{
		cfg:                cfg,
		client:             &http.Client{Timeout: time.Duration(cfg.ReportTimeoutMs) * time.Millisecond},
		queuedSamples:      make([]common.Sample, 0),
		backingFileSamples: make([]common.Sample, 0),
		cond:               sync.NewCond(new(sync.Mutex)),
		retryTimeout:       make(chan bool, 2),
	}

	if _, err := os.Stat(cfg.BackingFile); err == nil {
		samples, err := r.readSamplesFromBackingFile()
		if err != nil {
			r.cfg.logger.Printf("Failed to read samples from %v: %v", cfg.BackingFile, err)
		} else {
			r.queuedSamples = samples
			r.backingFileSamples = samples
		}
	}

	return r
}

func (r *reporter) start() {
	r.wg.Add(1)
	go r.processSamples()
}

func (r *reporter) stop() {
	r.cond.L.Lock()
	r.stopping = true
	r.cond.L.Unlock()
	r.cond.Signal()
	r.triggerRetryTimeout()
	r.wg.Wait()
}

func (r *reporter) reportSample(s common.Sample) {
	r.reportSamples([]common.Sample{s})
}

func (r *reporter) reportSamples(samples []common.Sample) {
	for _, s := range samples {
		r.cfg.logger.Printf("Queuing %v", s.String())
	}
	r.cond.L.Lock()
	r.queuedSamples = append(r.queuedSamples, samples...)
	r.cond.L.Unlock()
	r.cond.Signal()
}

func (r *reporter) triggerRetryTimeout() {
	r.retryTimeout <- true
}

func (r *reporter) processSamples() {
	for {
		r.cond.L.Lock()
		for len(r.queuedSamples) == 0 && !r.stopping {
			r.cond.Wait()
		}
		if r.stopping {
			r.cfg.logger.Printf("Reporter loop exiting")
			if err := r.writeSamplesToBackingFile(r.queuedSamples); err != nil {
				r.cfg.logger.Printf("Failed to write samples: %v", err)
			}
			r.wg.Done()
			return
		}
		samples := r.queuedSamples
		r.queuedSamples = make([]common.Sample, 0)
		r.cond.L.Unlock()

		r.cfg.logger.Printf("Took %v sample(s) from queue", len(samples))

		gotError := false
		for len(samples) > 0 {
			n := int(math.Min(float64(len(samples)), float64(r.cfg.ReportBatchSize)))
			s := samples[:n]
			if err := r.sendSamplesToServer(s); err != nil {
				r.cfg.logger.Printf("Got error when reporting samples: %v", err)
				gotError = true
				break
			}
			r.cfg.logger.Printf("Successfully reported %v sample(s)", len(s))
			samples = samples[n:]
		}

		r.cond.L.Lock()
		if gotError {
			// Return any samples that weren't forwarded successfully back to the
			// beginning of the queue.
			r.cfg.logger.Printf("Returning %v unreported sample(s) to queue", len(samples))
			r.queuedSamples = append(samples, r.queuedSamples...)
		}
		var newBackingFileSamples []common.Sample
		if !reflect.DeepEqual(r.backingFileSamples, r.queuedSamples) {
			newBackingFileSamples = r.queuedSamples
		}
		r.cond.L.Unlock()

		if newBackingFileSamples != nil {
			r.cfg.logger.Printf("Writing %v sample(s) to backing file", len(newBackingFileSamples))
			if err := r.writeSamplesToBackingFile(newBackingFileSamples); err != nil {
				r.cfg.logger.Printf("Failed to write samples: %v", err)
			}
		}

		if gotError {
			r.cfg.logger.Printf("Sleeping for %v ms after failure", r.cfg.ReportRetryMs)
			go func(ch chan bool) {
				time.Sleep(time.Duration(r.cfg.ReportRetryMs) * time.Millisecond)
				ch <- true
			}(r.retryTimeout)

			select {
			case <-r.retryTimeout:
			}
		}
	}

}

func (r *reporter) sendSamplesToServer(samples []common.Sample) error {
	data := common.JoinSamples(samples)
	sig := common.HashStringWithSHA256(fmt.Sprintf("%s|%s", data, r.cfg.ReportSecret))
	resp, err := r.client.PostForm(r.cfg.ReportURL, url.Values{"d": {data}, "s": {sig}})
	if err != nil {
		return err
	} else if resp.StatusCode != 200 {
		return fmt.Errorf("Got %v", resp.Status)
	}
	return nil
}

func (r *reporter) readSamplesFromBackingFile() ([]common.Sample, error) {
	f, err := os.Open(r.cfg.BackingFile)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	samples := make([]common.Sample, 0)
	d := json.NewDecoder(f)
	for {
		var s common.Sample
		if err = d.Decode(&s); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		samples = append(samples, s)
	}

	return samples, nil
}

func (r *reporter) writeSamplesToBackingFile(samples []common.Sample) error {
	if r.cfg.BackingFile == "" {
		return nil
	}

	p := r.cfg.BackingFile + tempBackingFileExtension
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	defer f.Close()

	e := json.NewEncoder(f)
	for _, s := range samples {
		if err = e.Encode(s); err != nil {
			return err
		}
	}
	if err = os.Rename(p, r.cfg.BackingFile); err != nil {
		return err
	}

	r.backingFileSamples = samples
	return nil
}
