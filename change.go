// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"fmt"
	"time"
)

type Change struct {
	Table     string
	Timestamp time.Time
	Operation Operation
	Before    map[string]interface{}
	After     map[string]interface{}
}

type Operation string

const (
	Insert Operation = "INSERT"
	Update Operation = "UPDATE"
	Delete Operation = "DELETE"
)

func (c Change) String() string {
	return fmt.Sprintf("%s: %s %s", c.Timestamp.Format(time.RFC3339Nano), c.Table, c.Operation)
}

type Changes []Change
