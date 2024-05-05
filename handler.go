// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import "context"

// ChangesHandler implementations are given batches of database changes. Each
// batch of changes is in the same order as the CDC log table. Batches may
// contain changes from multiple tables.
//
// If a handler returns an error then the entire batch is considered failed and
// retried. If a handler returns nil then the entire batch is considered
// successful and the relevant entries are removed from the CDC log table.
type ChangesHandler interface {
	HandleChanges(ctx context.Context, changes Changes) error
}

// ChangesHandlerFunc is an adaptor to allow the use of ordinary functions as
// ChangesHandler implementations. Note that you should not use this type
// directly and should instead always target the ChangesHandler type. For
// example, the appropriate use of this adaptor is:
//
//	var handler ChangesHandler = ChangesHandlerFunc(func(changes Changes) error {
//		// handle changes
//	})
type ChangesHandlerFunc func(ctx context.Context, changes Changes) error

func (fn ChangesHandlerFunc) HandleChanges(ctx context.Context, changes Changes) error {
	return fn(ctx, changes)
}
