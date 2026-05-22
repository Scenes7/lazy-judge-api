package main

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	cacheRefreshInterval = 10 * time.Minute
	// S3 key prefix that contains all problems.
	problemsPrefix = "problems/"
)

// problemCache holds the list of known problem IDs refreshed from S3.
type problemCache struct {
	mu         sync.RWMutex
	ids        []string       // snapshot of available problem IDs
	ready      chan struct{}   // closed once after the first successful load
	readyOnce  sync.Once

	s3Client *s3.Client
	bucket   string
}

// newProblemCache creates the cache and starts the background refresh loop.
// The first refresh runs immediately in a goroutine; callers can wait on
// cache.WaitReady() before using the list.
func newProblemCache(client *s3.Client, bucket string) *problemCache {
	c := &problemCache{
		s3Client: client,
		bucket:   bucket,
		ready:    make(chan struct{}),
	}
	go c.loop()
	return c
}

// loop runs an immediate refresh then repeats every cacheRefreshInterval.
func (c *problemCache) loop() {
	c.refresh()
	ticker := time.NewTicker(cacheRefreshInterval)
	defer ticker.Stop()
	for range ticker.C {
		c.refresh()
	}
}

// refresh lists all problem IDs from S3 and updates the cache.
// It uses ListObjectsV2 with a delimiter to enumerate "directories" under
// problems/ without reading individual files — one API call per refresh.
func (c *problemCache) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var ids []string
	var contToken *string

	for {
		out, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucket),
			Prefix:            aws.String(problemsPrefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: contToken,
		})
		if err != nil {
			log.Printf("[cache] S3 list error: %v", err)
			return // keep the stale list
		}

		for _, cp := range out.CommonPrefixes {
			if cp.Prefix == nil {
				continue
			}
			// cp.Prefix looks like "problems/sum_1_to_n/" — strip the surrounding parts
			trimmed := strings.TrimPrefix(*cp.Prefix, problemsPrefix)
			trimmed  = strings.TrimSuffix(trimmed, "/")
			if trimmed != "" {
				ids = append(ids, trimmed)
			}
		}

		if out.IsTruncated == nil || !*out.IsTruncated || out.NextContinuationToken == nil {
			break
		}
		contToken = out.NextContinuationToken
	}

	c.mu.Lock()
	c.ids = ids
	c.mu.Unlock()

	// Signal first-load waiters exactly once
	c.readyOnce.Do(func() { close(c.ready) })
	log.Printf("[cache] refreshed: %d problems", len(ids))
}

// WaitReady blocks until the first successful S3 list completes (or ctx is done).
func (c *problemCache) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot returns a copy of the current problem ID list (safe for mutation).
func (c *problemCache) Snapshot() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, len(c.ids))
	copy(out, c.ids)
	return out
}
