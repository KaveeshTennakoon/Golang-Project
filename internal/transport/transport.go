package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type RPC string // RPC identifies the Raft method.

const (
	RPCRequestVote   RPC = "request_vote"
	RPCAppendEntries RPC = "append_entries"
)

// HandlerFunc handles an inbound RPC and returns response or error.
type HandlerFunc func(method RPC, body io.Reader, w http.ResponseWriter)

// HTTPTransport routes JSON‑encoded RPCs over http.Client.
type HTTPTransport struct {
	client  *http.Client
	handler HandlerFunc
}

func New(handler HandlerFunc) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{
			Timeout: 3 * time.Second, // Increased timeout for better reliability
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				// Add more resilient connection settings
				DisableKeepAlives: false,
				MaxConnsPerHost:   100,
				ForceAttemptHTTP2: false,
			},
		},
		handler: handler,
	}
}

func (t *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.handler(RPC(r.URL.Path[1:]), r.Body, w)
}

func (t *HTTPTransport) Call(addr string, method RPC, req, resp any) error {
	// Marshal the request to JSON
	buf, _ := json.Marshal(req)

	// Create a new request with retry logic
	maxRetries := 3
	var httpResp *http.Response
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// If this is a retry, add a small delay with exponential backoff
		if attempt > 0 {
			backoff := time.Duration(attempt*50) * time.Millisecond
			time.Sleep(backoff)
		}

		// Make the HTTP request
		httpResp, err = t.client.Post(
			"http://"+addr+"/"+string(method),
			"application/json",
			bytes.NewReader(buf))

		// If successful, break out of the retry loop
		if err == nil {
			break
		}
	}

	// If all retries failed, return the last error
	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	// Check for non-200 status codes
	if httpResp.StatusCode != http.StatusOK {
		return io.ErrUnexpectedEOF
	}

	// Decode the response
	return json.NewDecoder(httpResp.Body).Decode(resp)
}

// Utility to reply JSON.
func ReplyJSON(w http.ResponseWriter, v any) {
	data, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
