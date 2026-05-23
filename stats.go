package main

// ─── Lifetime stats (DynamoDB) ────────────────────────────────────────────────
//
// All counters live in a single DynamoDB item:
//
//   Table: judge-stats
//   PK:    LIFETIME_STATS
//
//   TotalProblemsStarted  — incremented each time a problem is fetched by a client
//   TotalSubmissions      — incremented each time a submission completes judging
//   TotalTimeSpentMs      — cumulative milliseconds clients spent on problems
//   TotalSprintsCompleted — incremented when the last slot of a sprint is judged
//
// Every update uses a DynamoDB ADD expression so increments are atomic even
// under concurrent API instances.

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─── Feature flags ────────────────────────────────────────────────────────────
// Set to true to enable writes to the corresponding DynamoDB table.
const (
	enableStatsWrites       = true // judge-stats  (lifetime counters)
	enableSubmissionLogging = true // judge-submissions-sorted (per-submission records)
)

const (
	statsTableName = "judge-stats"
	statsPartKey   = "LIFETIME_STATS"
)

// statsClient wraps a DynamoDB client and the target table/key config.
type statsClient struct {
	db      *dynamodb.Client
	table   string
	pkAttr  string // partition key attribute name (e.g. "pk", "id", "STAT_ID")
	pkValue string // partition key value for the single stats item
}

// newStatsClient creates a statsClient, reading the key schema from env vars:
//
//	DYNAMO_STATS_TABLE    — table name (default: judge-stats)
//	DYNAMO_STATS_PK_ATTR  — partition key attribute name (default: pk)
//	DYNAMO_STATS_PK_VALUE — partition key value  (default: LIFETIME_STATS)
func newStatsClient(db *dynamodb.Client) *statsClient {
	table := env("DYNAMO_STATS_TABLE", statsTableName)
	pkAttr := env("DYNAMO_STATS_PK_ATTR", "LIFETIME_STATS")
	pkValue := env("DYNAMO_STATS_PK_VALUE", "stats")
	log.Printf("[stats] DynamoDB table=%s pk_attr=%s pk_value=%s", table, pkAttr, pkValue)
	return &statsClient{db: db, table: table, pkAttr: pkAttr, pkValue: pkValue}
}

// add atomically increments one or more numeric attributes on the lifetime
// stats item. keys and values must have equal length. The call is fire-and-
// forget safe — errors are logged but never returned to callers.
func (s *statsClient) add(ctx context.Context, fields map[string]int64) {
	if s == nil || s.db == nil || len(fields) == 0 {
		return
	}

	// Build: ADD #f0 :v0, #f1 :v1, …
	expr := "ADD "
	names := make(map[string]string, len(fields))
	vals := make(map[string]types.AttributeValue, len(fields))
	i := 0
	for k, v := range fields {
		if i > 0 {
			expr += ", "
		}
		nk := fmt.Sprintf("#f%d", i)
		vk := fmt.Sprintf(":v%d", i)
		expr += nk + " " + vk
		names[nk] = k
		vals[vk] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", v)}
		i++
	}

	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := s.db.UpdateItem(tctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			s.pkAttr: &types.AttributeValueMemberS{Value: s.pkValue},
		},
		UpdateExpression:          aws.String(expr),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: vals,
	})
	if err != nil {
		log.Printf("[stats] DynamoDB update error: %v", err)
	}
}

// asyncAdd fires add in a goroutine so callers are never blocked by DynamoDB.
func (s *statsClient) asyncAdd(fields map[string]int64) {
	if !enableStatsWrites || s == nil {
		return
	}
	go s.add(context.Background(), fields)
}

// ─── judge-submissions-sorted table ─────────────────────────────────────────

const submissionsTableName = "judge-submissions-sorted"

// recordSubmission writes one item to judge-submissions-sorted:
//
//	submission_id   (PK, S) → the generated ID
//	submission_date (SK, S) → UTC date "YYYY-MM-DD" (enables date-range queries via GSI)
//	submitted_at            → ISO-8601 UTC timestamp (full precision)
//	problem_id              → problem slug
//	problem_title           → human-readable name (may be empty)
//	code                    → submitted source code
func (s *statsClient) recordSubmission(submissionID string, req SubmitRequest) {
	if !enableSubmissionLogging || s == nil || s.db == nil {
		return
	}
	go func() {
		now := time.Now().UTC()
		item := map[string]types.AttributeValue{
			"submission_id":   &types.AttributeValueMemberS{Value: submissionID},
			"submission_date": &types.AttributeValueMemberS{Value: now.Format("2006-01-02")},
			"submitted_at":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339)},
			"problem_id":      &types.AttributeValueMemberS{Value: req.ProblemID},
			"code":            &types.AttributeValueMemberS{Value: req.Code},
		}
		if req.ProblemTitle != "" {
			item["problem_title"] = &types.AttributeValueMemberS{Value: req.ProblemTitle}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := s.db.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(submissionsTableName),
			Item:      item,
		})
		if err != nil {
			log.Printf("[stats] recordSubmission error: %v", err)
		}
	}()
}
