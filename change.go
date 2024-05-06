// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"encoding/json"
	"fmt"
	"time"
)

type Change struct {
	Table     string          `json:"table"`
	Timestamp time.Time       `json:"timestamp"`
	Operation Operation       `json:"operation"`
	Before    json.RawMessage `json:"before"`
	After     json.RawMessage `json:"after"`
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
