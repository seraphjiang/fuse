// SPDX-License-Identifier: Apache-2.0

package fuse

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func mockServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/fuse/query" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"columns":  []string{"id", "name"},
				"rows":     [][]interface{}{{1, "alice"}, {2, "bob"}},
				"metadata": map[string]interface{}{"total_rows": 2, "format": "sql", "trace_id": "t-1"},
			})
		case r.URL.Path == "/api/fuse/health":
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "healthy"})
		case r.URL.Path == "/api/fuse/datasources":
			json.NewEncoder(w).Encode([]map[string]string{{"id": "pg", "type": "postgres"}})
		case r.URL.Path == "/api/fuse/query/explain":
			json.NewEncoder(w).Encode(map[string]interface{}{"plan": "scan"})
		case r.URL.Path == "/api/fuse/webhooks" && r.Method == "GET":
			json.NewEncoder(w).Encode([]map[string]interface{}{{"id": "w1", "name": "alert"}})
		case r.URL.Path == "/api/fuse/webhooks" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]interface{}{"id": "w-new"})
		case r.Method == "DELETE" && len(r.URL.Path) > len("/api/fuse/webhooks/"):
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": true})
		case r.URL.Path == "/api/fuse/relationships":
			json.NewEncoder(w).Encode([]map[string]interface{}{{"left_datasource": "a", "confidence": 0.8}})
		case r.URL.Path == "/api/fuse/cdc/status":
			json.NewEncoder(w).Encode(map[string]interface{}{"enabled": true, "tracked_views": 3})
		case r.URL.Path == "/api/fuse/cdc/events":
			json.NewEncoder(w).Encode(map[string]interface{}{"accepted": true, "affected_views": []string{"v1"}})
		case r.URL.Path == "/api/fuse/predict":
			json.NewEncoder(w).Encode(map[string]interface{}{"estimated_ms": 150, "confidence": "medium"})
		case r.URL.Path == "/api/fuse/query/async" && r.Method == "POST":
			json.NewEncoder(w).Encode(map[string]interface{}{"job_id": "j-123"})
		case r.Method == "GET" && r.URL.Path == "/api/fuse/query/async/j-123":
			json.NewEncoder(w).Encode(map[string]interface{}{"job_id": "j-123", "status": "completed"})
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestNewClient(t *testing.T) {
	c := NewClient("http://localhost:9400")
	if c.BaseURL != "http://localhost:9400" {
		t.Fatalf("expected base URL http://localhost:9400, got %s", c.BaseURL)
	}
	if c.APIKey != "" {
		t.Fatal("expected empty API key")
	}
}

func TestQuery(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	result, err := c.Query("SELECT 1", "sql")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(result.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Metadata.TraceID != "t-1" {
		t.Fatalf("expected trace_id t-1, got %s", result.Metadata.TraceID)
	}
}

func TestHealth(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	h, err := c.Health()
	if err != nil {
		t.Fatalf("health error: %v", err)
	}
	if h.Status != "healthy" {
		t.Fatalf("expected healthy, got %s", h.Status)
	}
}

func TestDatasources(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	ds, err := c.Datasources()
	if err != nil {
		t.Fatalf("datasources error: %v", err)
	}
	if len(ds) != 1 || ds[0].ID != "pg" {
		t.Fatalf("unexpected datasources: %+v", ds)
	}
}

func TestExplain(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	plan, err := c.Explain("SELECT 1", "sql")
	if err != nil {
		t.Fatalf("explain error: %v", err)
	}
	if plan["plan"] != "scan" {
		t.Fatalf("unexpected plan: %v", plan)
	}
}

func TestToDicts(t *testing.T) {
	r := &QueryResult{
		Columns: []string{"a", "b"},
		Rows:    [][]interface{}{{1, "x"}, {2, "y"}},
	}
	dicts := r.ToDicts()
	if len(dicts) != 2 {
		t.Fatalf("expected 2 dicts, got %d", len(dicts))
	}
	if dicts[0]["a"] != 1 || dicts[0]["b"] != "x" {
		t.Fatalf("unexpected dict: %v", dicts[0])
	}
}

func TestAPIKeyHeader(t *testing.T) {
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("x-api-key")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "ok"})
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	c.APIKey = "secret-key"
	c.Health()
	if gotKey != "secret-key" {
		t.Fatalf("expected x-api-key secret-key, got %s", gotKey)
	}
}

func TestHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"bad"}`, 400)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	_, err := c.Query("bad", "sql")
	if err == nil {
		t.Fatal("expected error for 400 response")
	}
}

func TestWebhooks(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	ws, err := c.Webhooks()
	if err != nil {
		t.Fatalf("webhooks error: %v", err)
	}
	if len(ws) != 1 || ws[0]["id"] != "w1" {
		t.Fatalf("unexpected webhooks: %+v", ws)
	}
}

func TestCreateWebhook(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	id, err := c.CreateWebhook("alert", "SELECT 1", map[string]interface{}{"gt": 10}, "https://hook.example.com")
	if err != nil {
		t.Fatalf("create webhook error: %v", err)
	}
	if id != "w-new" {
		t.Fatalf("expected id w-new, got %s", id)
	}
}

func TestDeleteWebhook(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	err := c.DeleteWebhook("w-1")
	if err != nil {
		t.Fatalf("delete webhook error: %v", err)
	}
}

func TestRelationships(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	rels, err := c.Relationships()
	if err != nil {
		t.Fatalf("relationships error: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("expected 1 relationship, got %d", len(rels))
	}
}

func TestCdcStatus(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	s, err := c.CdcStatus()
	if err != nil {
		t.Fatalf("cdc status error: %v", err)
	}
	if s["enabled"] != true {
		t.Fatalf("expected enabled=true, got %v", s["enabled"])
	}
}

func TestCdcEvent(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	resp, err := c.CdcEvent("ds1", "users", "update")
	if err != nil {
		t.Fatalf("cdc event error: %v", err)
	}
	if resp["accepted"] != true {
		t.Fatalf("expected accepted=true, got %v", resp["accepted"])
	}
}

func TestPredict(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	p, err := c.Predict("SELECT * FROM ds.logs")
	if err != nil {
		t.Fatalf("predict error: %v", err)
	}
	if p["confidence"] != "medium" {
		t.Fatalf("expected confidence=medium, got %v", p["confidence"])
	}
}

func TestSubmitAsync(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	jobID, err := c.SubmitAsync("SELECT 1", "sql")
	if err != nil {
		t.Fatalf("submit async error: %v", err)
	}
	if jobID != "j-123" {
		t.Fatalf("expected job_id j-123, got %s", jobID)
	}
}

func TestPollAsync(t *testing.T) {
	srv := mockServer()
	defer srv.Close()
	c := NewClient(srv.URL)
	status, err := c.PollAsync("j-123")
	if err != nil {
		t.Fatalf("poll async error: %v", err)
	}
	if status.Status != "completed" {
		t.Fatalf("expected completed, got %s", status.Status)
	}
}
