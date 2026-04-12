// SPDX-License-Identifier: Apache-2.0

// Package fuse provides a Go client for the Fuse federated query engine.
package fuse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client for the Fuse REST API.
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// NewClient creates a Fuse client with the given base URL.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL:    baseURL,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// QueryResult holds the response from a query.
type QueryResult struct {
	Columns    []string        `json:"columns"`
	Rows       [][]interface{} `json:"rows"`
	Metadata   Metadata        `json:"metadata"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

// Metadata from a query response.
type Metadata struct {
	TotalRows          int      `json:"total_rows"`
	Format             string   `json:"format"`
	TraceID            string   `json:"trace_id"`
	DatasourcesQueried []string `json:"datasources_queried,omitempty"`
}

// HealthResult from the health endpoint.
type HealthResult struct {
	Status     string                 `json:"status"`
	Connectors map[string]interface{} `json:"connectors,omitempty"`
}

// Datasource info from the datasources endpoint.
type Datasource struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

func (c *Client) do(method, path string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.BaseURL+path, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("x-api-key", c.APIKey)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(data))
	}
	return data, nil
}

// Query executes a SQL or PPL query.
func (c *Client) Query(sql, format string) (*QueryResult, error) {
	body := map[string]interface{}{"query": sql, "format": format}
	data, err := c.do("POST", "/api/fuse/query", body)
	if err != nil {
		return nil, err
	}
	var result QueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &result, nil
}

// QueryWithCursor executes a query with pagination.
func (c *Client) QueryWithCursor(sql, format string, pageSize int, cursor string) (*QueryResult, error) {
	body := map[string]interface{}{"query": sql, "format": format, "page_size": pageSize}
	if cursor != "" {
		body["cursor"] = cursor
	}
	data, err := c.do("POST", "/api/fuse/query", body)
	if err != nil {
		return nil, err
	}
	var result QueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &result, nil
}

// Explain returns the query plan.
func (c *Client) Explain(sql, format string) (map[string]interface{}, error) {
	body := map[string]interface{}{"query": sql, "format": format}
	data, err := c.do("POST", "/api/fuse/query/explain", body)
	if err != nil {
		return nil, err
	}
	var result map[string]interface{}
	return result, json.Unmarshal(data, &result)
}

// Health checks connector health.
func (c *Client) Health() (*HealthResult, error) {
	data, err := c.do("GET", "/api/fuse/health", nil)
	if err != nil {
		return nil, err
	}
	var result HealthResult
	return &result, json.Unmarshal(data, &result)
}

// Datasources lists connected datasources.
func (c *Client) Datasources() ([]Datasource, error) {
	data, err := c.do("GET", "/api/fuse/datasources", nil)
	if err != nil {
		return nil, err
	}
	var result []Datasource
	return result, json.Unmarshal(data, &result)
}


// AsyncSubmitResponse from submitting an async query.
type AsyncSubmitResponse struct {
	JobID string `json:"job_id"`
}

// AsyncJobStatus from polling an async query.
type AsyncJobStatus struct {
	JobID  string      `json:"job_id"`
	Status string      `json:"status"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// SubmitAsync submits a query for async execution. Returns job ID.
func (c *Client) SubmitAsync(sql, format string) (string, error) {
	body := map[string]interface{}{"query": sql, "format": format}
	data, err := c.do("POST", "/api/fuse/query/async", body)
	if err != nil {
		return "", err
	}
	var resp AsyncSubmitResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", err
	}
	return resp.JobID, nil
}

// PollAsync polls the status of an async query.
func (c *Client) PollAsync(jobID string) (*AsyncJobStatus, error) {
	data, err := c.do("GET", "/api/fuse/query/async/"+jobID, nil)
	if err != nil {
		return nil, err
	}
	var status AsyncJobStatus
	return &status, json.Unmarshal(data, &status)
}

// CancelAsync cancels an async query.
func (c *Client) CancelAsync(jobID string) error {
	_, err := c.do("DELETE", "/api/fuse/query/async/"+jobID, nil)
	return err
}

// ToDicts converts rows to a slice of maps keyed by column name.
func (r *QueryResult) ToDicts() []map[string]interface{} {
	out := make([]map[string]interface{}, len(r.Rows))
	for i, row := range r.Rows {
		m := make(map[string]interface{}, len(r.Columns))
		for j, col := range r.Columns {
			if j < len(row) {
				m[col] = row[j]
			}
		}
		out[i] = m
	}
	return out
}

// ── Sprint 18: Webhooks (#1811) ──

// Webhooks lists all webhook subscriptions.
func (c *Client) Webhooks() ([]map[string]interface{}, error) {
	data, err := c.do("GET", "/api/fuse/webhooks", nil)
	if err != nil {
		return nil, err
	}
	var out []map[string]interface{}
	return out, json.Unmarshal(data, &out)
}

// CreateWebhook registers a new webhook subscription.
func (c *Client) CreateWebhook(name, query string, condition map[string]interface{}, callbackURL string) (string, error) {
	body := map[string]interface{}{
		"name": name, "query": query, "format": "sql",
		"condition": condition, "callback_url": callbackURL,
	}
	data, err := c.do("POST", "/api/fuse/webhooks", body)
	if err != nil {
		return "", err
	}
	var result struct{ ID string `json:"id"` }
	return result.ID, json.Unmarshal(data, &result)
}

// DeleteWebhook removes a webhook subscription.
func (c *Client) DeleteWebhook(id string) error {
	_, err := c.do("DELETE", "/api/fuse/webhooks/"+id, nil)
	return err
}

// TestWebhook test-fires a webhook.
func (c *Client) TestWebhook(id string) (map[string]interface{}, error) {
	data, err := c.do("POST", "/api/fuse/webhooks/"+id+"/test", nil)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(data, &out)
}

// ── Sprint 18: Schema Relationships (#1831) ──

// Relationships discovers cross-datasource foreign key relationships.
func (c *Client) Relationships() ([]map[string]interface{}, error) {
	data, err := c.do("GET", "/api/fuse/relationships", nil)
	if err != nil {
		return nil, err
	}
	var out []map[string]interface{}
	return out, json.Unmarshal(data, &out)
}

// ── Sprint 18: CDC (#1852) ──

// CdcStatus returns CDC tracker stats and pending views.
func (c *Client) CdcStatus() (map[string]interface{}, error) {
	data, err := c.do("GET", "/api/fuse/cdc/status", nil)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(data, &out)
}

// CdcEvent ingests a change event to trigger materialized view refresh.
func (c *Client) CdcEvent(datasource, table, changeType string) (map[string]interface{}, error) {
	body := map[string]interface{}{
		"datasource": datasource, "table": table,
		"change_type": changeType, "timestamp": time.Now().Unix(),
	}
	data, err := c.do("POST", "/api/fuse/cdc/events", body)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(data, &out)
}

// ── Predictive Performance ──

// Predict estimates query latency based on historical data.
func (c *Client) Predict(query string) (map[string]interface{}, error) {
	data, err := c.do("GET", "/api/fuse/predict?query="+query, nil)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	return out, json.Unmarshal(data, &out)
}
