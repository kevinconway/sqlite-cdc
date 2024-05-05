// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"context"
)

type CDC interface {
	// Start the engine in CDC-only mode. This mode collects only captured
	// changes and does not collect any existing records that are not changed.
	// CDC mode runs until stopped or it encounters an error.
	CDC(ctx context.Context) error
	// Start the engine in bootstrap-only mode. This mode collects all existing
	// records and emits them as if they were inserts. Bootstrap mode runs until
	// it completes a scan of each table and then exits.
	Bootstrap(ctx context.Context) error
	// Start the engine in bootstrap and CDC mode. This mode starts with a
	// bootstrap and then enters CDC mode once it is complete. Any changes that
	// occur during the bootstrap are emitted once the engine enters CDC mode.
	BootstrapAndCDC(ctx context.Context) error

	// Perform any setup necessary to support CDC.
	Setup(ctx context.Context) error
	// Perform any teardown necessary to remove CDC.
	Teardown(ctx context.Context) error

	// Stop any ongoing CDC operations and shut down.
	Close(ctx context.Context) error
}
