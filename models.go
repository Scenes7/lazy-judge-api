package main

// ─── Submission types ─────────────────────────────────────────────────────────

// SubmitRequest is the JSON body for POST /submit.
type SubmitRequest struct {
	Lang         string `json:"lang"          binding:"required,oneof=python cpp"`
	Code         string `json:"code"          binding:"required"`
	ProblemID    string `json:"problem_id"    binding:"required"`
	ProblemTitle string `json:"problem_title,omitempty"` // human-readable name for the submissions log
	// Sprint-mode metadata — omitted for intro submissions.
	SprintID   string `json:"sprint_id,omitempty"`
	SprintSize int    `json:"sprint_size,omitempty"`
	SlotIndex  int    `json:"slot_index,omitempty"`
	ProblemMs  int64  `json:"problem_ms,omitempty"`
}

// LambdaResponse mirrors the JSON returned by the judge Lambda.
type LambdaResponse struct {
	Verdict     string       `json:"verdict"`
	Output      string       `json:"output"`
	Error       string       `json:"error"`
	TestResults []TestResult `json:"test_results"`
}

// TestResult is the per-test-case breakdown returned by the judge Lambda.
type TestResult struct {
	Case    int    `json:"case"`
	Verdict string `json:"verdict"`
	Output  string `json:"output"`
	Error   string `json:"error"`
}

// WSEvent is streamed over WebSocket to the frontend.
type WSEvent struct {
	Status      string       `json:"status"`
	Message     string       `json:"message"`
	Output      string       `json:"output,omitempty"`
	Detail      string       `json:"detail,omitempty"`
	TestResults []TestResult `json:"test_results,omitempty"`
}

// ─── Problem types ────────────────────────────────────────────────────────────

// ProblemMeta mirrors meta.json (only fields safe to expose to the client).
type ProblemMeta struct {
	Title         string   `json:"title"`
	Difficulty    int      `json:"difficulty"`
	Tags          []string `json:"tags"`
	TimeLimitSec  int      `json:"time_limit_seconds"`
	MemoryLimitMB int      `json:"memory_limit_mb"`
}

// StarterCode maps language key → starter code string (mirrors starter_code.json).
type StarterCode map[string]string

// ProblemData is the combined payload for a single problem.
// The raw Markdown description is included; test-case paths are never exposed.
type ProblemData struct {
	ID          string      `json:"id"`
	Meta        ProblemMeta `json:"meta"`
	Description string      `json:"description"` // raw Markdown
	StarterCode StarterCode `json:"starter_code"`
}

// verdictToStatus maps Lambda verdict strings → WebSocket status tokens.
func verdictToStatus(verdict string) string {
	switch verdict {
	case "Accepted":
		return "success"
	case "Wrong Answer":
		return "wrong_answer"
	case "Runtime Error":
		return "runtime_error"
	case "Compile Error":
		return "compile_error"
	case "TLE":
		return "tle"
	default:
		return "error"
	}
}
