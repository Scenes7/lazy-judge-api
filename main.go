// FastJudge API Gateway
//
// File layout:
//   main.go              — config, AWS clients, router wiring
//   models.go            — shared request/response types
//   store.go             — in-memory submission result store
//   problem_cache.go     — S3 problem-list cache (10-min refresh)
//   handlers_problem.go  — GET /problem/:id, GET /api/problems/random
//   handlers_submit.go   — POST /submit, GET /ws/results, GET /health
//   stats.go             — DynamoDB lifetime stats (judge-stats table)
//   ratelimit.go         — IP-based token-bucket rate limiting
//
// Environment variables:
//   AWS_REGION            — default: ca-central-1
//   AWS_ACCESS_KEY_ID     — IAM access key
//   AWS_SECRET_ACCESS_KEY — IAM secret key
//   S3_BUCKET             — default: judge-test-cases (sprint problems)
//   INTRO_S3_BUCKET       — default: judge-introduction (warm-up problem)
//   LAMBDA_FUNCTION_NAME  — default: fastjudge-runner
//   PORT                  — default: 8080
//   FRONTEND_ORIGIN       — default: http://localhost:3000
//
// DynamoDB:
//   Table:         judge-stats
//   Partition key: pk (String) = "LIFETIME_STATS"
//   Attributes:    TotalProblemsStarted, TotalSubmissions,
//                  TotalTimeSpentMs, TotalSprintsCompleted

package main

import (
	"context"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	port := env("PORT", "8080")
	region := env("AWS_REGION", "ca-central-1")
	funcName := env("LAMBDA_FUNCTION_NAME", "fastjudge-runner")
	s3Bucket := env("S3_BUCKET", "judge-test-cases")
	introS3Bucket := env("INTRO_S3_BUCKET", "judge-introduction")
	// FRONTEND_ORIGIN may be a comma-separated list of allowed origins, e.g.:
	//   http://localhost:3000,https://lazyjudge.com,https://www.lazyjudge.com
	frontendOriginEnv := env("FRONTEND_ORIGIN", "http://localhost:3000")
	allowedOrigins := strings.Split(frontendOriginEnv, ",")
	for i := range allowedOrigins {
		allowedOrigins[i] = strings.TrimSpace(allowedOrigins[i])
	}

	log.Printf("FastJudge API Gateway starting on :%s", port)
	log.Printf("Lambda function: %s (region: %s)", funcName, region)
	log.Printf("S3 bucket:       %s", s3Bucket)
	log.Printf("Intro S3 bucket: %s", introS3Bucket)
	log.Printf("CORS origins:    %v", allowedOrigins)

	// ── Rate-limit pools (IP-based token buckets) ──
	fetchLimiter := newPool(10, 0.5) // 10 burst, refill 0.5/sec
	submitLimiter := newPool(5, 0.1) // 5 burst, refill 1/10sec

	// ── Build a single shared AWS config ──
	// Prefer explicit env-var credentials; fall back to the default chain
	// (instance role, ~/.aws/credentials, etc.) if not set.
	var credOpts []func(*config.LoadOptions) error
	credOpts = append(credOpts, config.WithRegion(region))
	if ak := os.Getenv("AWS_ACCESS_KEY_ID"); ak != "" {
		credOpts = append(credOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				ak,
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"",
			),
		))
	}
	awsCfg, err := config.LoadDefaultConfig(context.Background(), credOpts...)
	if err != nil {
		log.Fatalf("Failed to build AWS config: %v", err)
	}

	lambdaClient := lambda.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)
	dbClient := dynamodb.NewFromConfig(awsCfg)
	stats := newStatsClient(dbClient)

	// ── Start the S3 problem-list cache ──
	// Refreshes every 10 min in the background; first load is async, but the
	// /random endpoint will wait up to 5 s for it before returning 503.
	cache := newProblemCache(s3Client, s3Bucket)

	// ── Router ──
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	// Trust only the local Nginx reverse proxy; Gin will then read X-Forwarded-For
	// forwarded by Nginx. The rate limiter reads CF-Connecting-IP directly so this
	// is a belt-and-suspenders safeguard for Gin's own ClientIP().
	if err := r.SetTrustedProxies([]string{"127.0.0.1", "::1"}); err != nil {
		log.Fatalf("SetTrustedProxies: %v", err)
	}

	// WebSocket endpoint — registered BEFORE the CORS middleware group.
	// gin-contrib/cors writes Access-Control-* headers eagerly, which commits
	// the response writer and prevents gorilla/websocket from hijacking the
	// connection for the HTTP→WS upgrade. Origin validation is handled by
	// gorilla's CheckOrigin (set to permissive; tighten in production).
	r.GET("/ws/results", handleWSResults)

	// All standard HTTP routes get CORS headers via a sub-group.
	corsConfig := cors.Config{
		AllowOrigins:     allowedOrigins,
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}
	api := r.Group("/")
	api.Use(cors.New(corsConfig))
	{
		// OPTIONS catch-all: Gin must match the route before middleware can run.
		// gin-contrib/cors intercepts OPTIONS, responds 204, and aborts —
		// the empty handler is never reached.
		api.OPTIONS("/*path", func(c *gin.Context) {})

		api.GET("/health", handleHealth)
		// /problem/:id — intro problem (a_plus_b) lives in a separate bucket
		api.GET("/problem/:id", rateLimitMiddleware(fetchLimiter), func(c *gin.Context) {
			bucket := s3Bucket
			if c.Param("id") == "a_plus_b" {
				bucket = introS3Bucket
			}
			handleGetProblem(s3Client, bucket, stats)(c)
		})
		api.GET("/api/problems/random", rateLimitMiddleware(fetchLimiter), handleRandomProblems(s3Client, s3Bucket, cache, stats))
		api.POST("/submit", handleSubmit(lambdaClient, funcName, submitLimiter, stats))
	}

	if err := r.Run(":" + port); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
