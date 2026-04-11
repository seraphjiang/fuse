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
		switch r.URL.Path {
		case "/api/fuse/query":
			json.NewEncoder(w).Encode(map[string]interface{}{
				"columns":  []string{"id", "name"},
				"rows":     [][]interface{}{{1, "alice"}, {2, "bob"}},
				"metadata": map[string]interface{}{"total_rows": 2, "format": "sql", "trace_id": "t-1"},
			})
		case "/api/fuse/health":
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "healthy"})
		case "/api/fuse/datasources":
			json.NewEncoder(w).Encode([]map[string]string{{"id": "pg", "type": "postgres"}})
		case "/api/fuse/query/explain":
			json.NewEncoder(w).Encode(map[string]interface{}{"plan": "scan"})
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
