package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	cdc "github.com/kevinconway/sqlite-cdc"
)

type Debug struct {
	Output io.Writer
}

func (d *Debug) HandleChanges(ctx context.Context, changes cdc.Changes) error {
	for _, change := range changes {
		b, err := json.Marshal(change)
		if err != nil {
			return fmt.Errorf("%w: failed to marshal changes to JSON", err)
		}
		fmt.Fprintln(d.Output, string(b))
	}
	return nil
}
