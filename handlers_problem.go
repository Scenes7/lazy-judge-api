package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

// ─── S3 helpers ───────────────────────────────────────────────────────────────

// s3GetString fetches a single S3 key and returns its body as a string.
func s3GetString(ctx context.Context, client *s3.Client, bucket, key string) (string, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		return "", fmt.Errorf("s3 read %s: %w", key, err)
	}
	return string(b), nil
}

// fetchProblem retrieves meta.json, description.md, and starter_code.json for
// one problem in parallel. It never fetches hidden test cases.
func fetchProblem(ctx context.Context, client *s3.Client, bucket, problemID string) (*ProblemData, error) {
	prefix := fmt.Sprintf("problems/%s/", problemID)

	type result struct {
		data string
		err  error
	}
	metaCh    := make(chan result, 1)
	descCh    := make(chan result, 1)
	starterCh := make(chan result, 1)

	go func() { d, err := s3GetString(ctx, client, bucket, prefix+"meta.json");        metaCh <- result{d, err} }()
	go func() { d, err := s3GetString(ctx, client, bucket, prefix+"description.md");   descCh <- result{d, err} }()
	go func() { d, err := s3GetString(ctx, client, bucket, prefix+"starter_code.json"); starterCh <- result{d, err} }()

	metaRes    := <-metaCh
	descRes    := <-descCh
	starterRes := <-starterCh

	if metaRes.err != nil    { return nil, metaRes.err }
	if descRes.err != nil    { return nil, descRes.err }
	if starterRes.err != nil { return nil, starterRes.err }

	var meta ProblemMeta
	if err := json.Unmarshal([]byte(metaRes.data), &meta); err != nil {
		return nil, fmt.Errorf("parse meta.json for %s: %w", problemID, err)
	}

	var starter StarterCode
	if err := json.Unmarshal([]byte(starterRes.data), &starter); err != nil {
		return nil, fmt.Errorf("parse starter_code.json for %s: %w", problemID, err)
	}

	return &ProblemData{
		ID:          problemID,
		Meta:        meta,
		Description: descRes.data,
		StarterCode: starter,
	}, nil
}

// ─── GET /problem/:id ─────────────────────────────────────────────────────────

func handleGetProblem(s3Client *s3.Client, bucket string, stats *statsClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		problemID := c.Param("id")
		if problemID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "missing problem id"})
			return
		}

		ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
		defer cancel()

		data, err := fetchProblem(ctx, s3Client, bucket, problemID)
		if err != nil {
			log.Printf("fetchProblem %s: %v", problemID, err)
			c.JSON(http.StatusNotFound, gin.H{"error": "problem not found"})
			return
		}
		c.JSON(http.StatusOK, data)
		stats.asyncAdd(map[string]int64{"TotalProblemsStarted": 1})
	}
}

// ─── GET /api/problems/random?count=N ────────────────────────────────────────

const (
	defaultRandomCount = 5
	maxRandomCount     = 20
)

func handleRandomProblems(s3Client *s3.Client, bucket string, cache *problemCache, stats *statsClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Parse optional count param
		count := defaultRandomCount
		if raw := c.Query("count"); raw != "" {
			fmt.Sscanf(raw, "%d", &count)
		}
		if count < 1 { count = 1 }
		if count > maxRandomCount { count = maxRandomCount }

		// Wait for cache to be ready (should already be after startup, just a safety net)
		waitCtx, waitCancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer waitCancel()
		if err := cache.WaitReady(waitCtx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "problem list not yet available, retry in a moment"})
			return
		}

		ids := cache.Snapshot()
		if len(ids) == 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no problems available"})
			return
		}

		// Clamp count to actual available IDs
		if count > len(ids) {
			count = len(ids)
		}

		// Fisher-Yates partial shuffle to pick `count` unique IDs without
		// allocating a full copy — we only shuffle the first `count` positions.
		picked := make([]string, len(ids))
		copy(picked, ids)
		for i := 0; i < count; i++ {
			j := i + rand.IntN(len(picked)-i)
			picked[i], picked[j] = picked[j], picked[i]
		}
		selected := picked[:count]

		// Fetch all selected problems concurrently
		type fetchResult struct {
			data *ProblemData
			err  error
		}
		ch := make(chan fetchResult, count)

		fetchCtx, fetchCancel := context.WithTimeout(c.Request.Context(), 15*time.Second)
		defer fetchCancel()

		for _, id := range selected {
			id := id // capture
			go func() {
				d, err := fetchProblem(fetchCtx, s3Client, bucket, id)
				ch <- fetchResult{d, err}
			}()
		}

		problems := make([]*ProblemData, 0, count)
		var fetchErrors []string
		for range selected {
			r := <-ch
			if r.err != nil {
				log.Printf("random fetch error: %v", r.err)
				fetchErrors = append(fetchErrors, r.err.Error())
			} else {
				problems = append(problems, r.data)
			}
		}

		if len(problems) == 0 {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "failed to fetch any problems",
				"detail": fetchErrors,
			})
			return
		}

		// Shuffle the result slice so ordering doesn't leak the S3 iteration order
		rand.Shuffle(len(problems), func(i, j int) { problems[i], problems[j] = problems[j], problems[i] })

		c.JSON(http.StatusOK, problems)
		// Each successfully fetched problem counts as one problem started;
		// the batch itself counts as one sprint started.
		stats.asyncAdd(map[string]int64{
			"TotalProblemsStarted": int64(len(problems)),
			"TotalSprintsStarted":  1,
		})
	}
}
