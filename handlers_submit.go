package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const TIMEOUT = 60

// handleSubmit creates a result entry, fires invokeLambda in a goroutine,
// and immediately returns the submission_id. Sprint-mode clients can submit
// rapidly; each call is fully independent — no shared mutable state between
// submissions beyond the results map (protected by resultsMu).
//
// Rate-limiting is applied *after* body parsing so we can inspect problem_id:
//   - a_plus_b (intro):  immediate reject if no token (allow)
//   - all other problems: queue up to 5 extra requests per IP (waitForToken)
func handleSubmit(client *lambda.Client, funcName string, limiter *limiterPool, stats *statsClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req SubmitRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		const maxCodeLen = 1000
		if len([]rune(req.Code)) > maxCodeLen {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":  "submission_too_long",
				"detail": "Code exceeds the 1000 character limit.",
			})
			return
		}

		// Rate-limit: intro problem rejects instantly; sprint problems queue.
		ip := realIP(c)
		if req.ProblemID == "a_plus_b" {
			if !limiter.allow(ip) {
				c.JSON(http.StatusTooManyRequests, gin.H{
					"error":  "rate_limited",
					"detail": "Too many requests — please slow down.",
				})
				return
			}
		} else {
			if !limiter.waitForToken(ip, 5, 60*time.Second) {
				c.JSON(http.StatusTooManyRequests, gin.H{
					"error":  "rate_limited",
					"detail": "Too many requests — please slow down.",
				})
				return
			}
		}

		id := newSubmissionID(req)
		entry := newEntry()
		storeEntry(id, entry)

		// Log the submission to judge-submissions-sorted table immediately (fire-and-forget).
		stats.recordSubmission(id, req)

		// Prime the stream with a queued event so the WS client sees something
		// immediately, even before the Lambda cold-start.
		entry.appendEvent(WSEvent{
			Status:  "queued",
			Message: "Submission queued, waiting for worker…",
		})

		// Each goroutine holds its own context and result entry; there is no
		// shared state between concurrent submissions.
		go invokeLambda(client, funcName, req, entry, stats)

		c.JSON(http.StatusOK, gin.H{"submission_id": id})
	}
}

// ─── Lambda Invoker ───────────────────────────────────────────────────────────

// invokeLambda is run in a goroutine per submission. It pushes status events
// into entry as they occur, calls finalise() exactly once, and then records
// lifetime stats in DynamoDB.
func invokeLambda(client *lambda.Client, funcName string, req SubmitRequest, entry *resultEntry, stats *statsClient) {
	defer entry.finalise()

	entry.appendEvent(WSEvent{
		Status:  "running",
		Message: "Executing code on judge cluster…",
	})

	body, err := json.Marshal(req)
	if err != nil {
		entry.appendEvent(WSEvent{Status: "error", Message: "Failed to serialise request", Detail: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT*time.Second)
	defer cancel()

	out, err := client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   aws.String(funcName),
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
		Payload:        body,
	})
	if err != nil {
		entry.appendEvent(WSEvent{Status: "error", Message: "Lambda invocation failed", Detail: err.Error()})
		return
	}

	if out.FunctionError != nil {
		entry.appendEvent(WSEvent{
			Status:  "error",
			Message: fmt.Sprintf("Lambda function error: %s", *out.FunctionError),
			Detail:  string(out.Payload),
		})
		return
	}

	var lr LambdaResponse
	if err := json.Unmarshal(out.Payload, &lr); err != nil {
		entry.appendEvent(WSEvent{Status: "error", Message: "Malformed Lambda response", Detail: string(out.Payload)})
		return
	}

	msg := lr.Verdict
	if msg == "" {
		msg = "Unknown verdict"
	}
	entry.appendEvent(WSEvent{
		Status:      verdictToStatus(lr.Verdict),
		Message:     msg,
		Output:      lr.Output,
		Detail:      lr.Error,
		TestResults: lr.TestResults,
	})

	// ── Lifetime stats ────────────────────────────────────────────────────────
	// Record every completed submission (terminal verdict reached).
	updates := map[string]int64{
		"TotalSubmissions": 1,
	}
	if lr.Verdict == "Accepted" {
		updates["TotalSubmissionsAccepted"] = 1
	}
	if req.ProblemMs > 0 {
		updates["TotalTimeSpentMs"] = req.ProblemMs
	}
	// A sprint is considered complete when the final slot's verdict arrives.
	if req.SprintSize > 0 && req.SlotIndex == req.SprintSize-1 {
		updates["TotalSprintsCompleted"] = 1
	}
	stats.asyncAdd(updates)
}

// ─── WebSocket upgrader ───────────────────────────────────────────────────────

var upgrader = websocket.Upgrader{
	HandshakeTimeout: 5 * time.Second,
	CheckOrigin:      func(r *http.Request) bool { return true },
}

// ─── GET /ws/results?id=<submission_id> ──────────────────────────────────────

// handleWSResults upgrades to WebSocket and streams all queued events for a
// submission. It blocks efficiently using a notify-channel pattern; the Lambda
// goroutine wakes it on every new event rather than polling.
//
// Sprint-mode note: multiple WebSocket connections for *different* submission
// IDs are fully independent. A connection for a now-finished submission will
// see the buffered events immediately and close cleanly.
func handleWSResults(c *gin.Context) {
	id := c.Query("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing id query param"})
		return
	}

	// Allow up to 2s for the entry to appear (handles the tiny race between
	// POST /submit returning and the WS being opened by the client).
	var entry *resultEntry
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var ok bool
		entry, ok = getEntry(id)
		if ok {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if entry == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "submission not found"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("WS upgrade error: %v", err)
		return
	}
	defer conn.Close()

	cursor := 0
	for {
		entry.mu.Lock()
		newEvents := entry.events[cursor:]
		done := entry.done
		notify := entry.notify
		entry.mu.Unlock()

		for _, ev := range newEvents {
			data, _ := json.Marshal(ev)
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("WS write error (sub %s): %v", id, err)
				return
			}
			cursor++
		}

		if done {
			_ = conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"),
			)
			return
		}

		select {
		case <-notify:
			// new event or finalise() — loop back and drain
		case <-time.After(TIMEOUT * time.Second):
			data, _ := json.Marshal(WSEvent{
				Status:  "error",
				Message: "Judge timed out",
				Detail:  "No response from Lambda",
			})
			_ = conn.WriteMessage(websocket.TextMessage, data)
			return
		}
	}
}

// ─── GET /health ──────────────────────────────────────────────────────────────

func handleHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "ok",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}
