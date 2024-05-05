package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	cdc "github.com/kevinconway/sqlite-cdc"
)

// HTTPBasicPOST implements the cdc.ChangesHandler interface by making POST
// requests to an HTTP endpoint.
//
// Targets of this handler will receive HTTP POST requests with the following
// format:
//
//	{
//	  "changes": [
//	    {
//	      "table": "table_name",
//	      "timestamp": "2022-01-01T00:00:00Z",
//	      "operation": "INSERT", // or "UPDATE" or "DELETE"
//	      "before": { // present for updates and deletes
//	        "key": "value",
//	        ...
//	      },
//	      "after": { // present for inserts and updates
//	        "key": "value",
//	        ...
//	      }
//	    },
//	    ...
//	  ]
//	}
type HTTPBasicPOST struct {
	Client   *http.Client
	Endpoint string
}

func (h *HTTPBasicPOST) HandleChanges(ctx context.Context, changes cdc.Changes) error {
	cs := jsonChanges{Changes: make([]jsonChange, len(changes))}
	for offset, change := range changes {
		cs.Changes[offset] = jsonChange(change)
	}
	b, err := json.Marshal(cs)
	if err != nil {
		return fmt.Errorf("%w: failed to marshal changes for POST", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.Endpoint, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("%w: failed to create POST request", err)
	}
	resp, err := h.Client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: failed to POST changes", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("%w: failed to read error response body", err)
		}
		return fmt.Errorf("%w: HTTP status %d: %s", err, resp.StatusCode, string(b))
	}
	return nil
}

type jsonChanges struct {
	Changes []jsonChange `json:"changes"`
}

type jsonChange struct {
	Table     string                 `json:"table"`
	Timestamp time.Time              `json:"timestamp"`
	Operation cdc.Operation          `json:"operation"`
	Before    map[string]interface{} `json:"before"`
	After     map[string]interface{} `json:"after"`
}
