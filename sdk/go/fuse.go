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
