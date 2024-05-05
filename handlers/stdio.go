package handlers

import (
	"context"
	"fmt"
	"io"

	cdc "github.com/kevinconway/sqlite-cdc"
)

type STDIO struct {
	Output io.Writer
}

func (s *STDIO) HandleChanges(ctx context.Context, changes cdc.Changes) error {
	for _, change := range changes {
		fmt.Fprintln(s.Output, change.String())
	}
	return nil
}
