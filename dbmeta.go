// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"database/sql"
	"fmt"
	"strings"
)

type dbMeta struct {
	Filename string
	WAL      bool
	Tables   map[string]tableMeta
}

func newDBMeta(db *sql.DB) (*dbMeta, error) {
	var filename string
	if err := db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&filename); err != nil {
		return nil, fmt.Errorf("%w: failed to determine database filename", err)
	}

	var pragmaWAL string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&pragmaWAL); err != nil {
		return nil, fmt.Errorf("%w: failed to determine database journal mode", err)
	}
	wal := strings.ToLower(pragmaWAL) == "wal"

	tables := make(map[string]tableMeta)
	rows, err := db.Query("SELECT name, wr FROM pragma_table_list WHERE schema='main' AND type='table'")
	if err != nil {
		return nil, fmt.Errorf("%w: failed to list tables", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var wr bool
		if err := rows.Scan(&name, &wr); err != nil {
			return nil, fmt.Errorf("%w: failed to read table metadata", err)
		}
		table := tableMeta{
			Name:         name,
			WithoutRowID: wr,
		}
		cRows, err := db.Query("SELECT name, type, pk FROM pragma_table_info(?)", table.Name)
		if err != nil {
			return nil, fmt.Errorf("%w: failed to list table columns for %s", err, table.Name)
		}
		defer cRows.Close()
		for cRows.Next() {
			var name string
			var type_ string
			var pk int
			if err := cRows.Scan(&name, &type_, &pk); err != nil {
				return nil, fmt.Errorf("%w: failed to read table column metadata for %s", err, table.Name)
			}
			table.Columns = append(table.Columns, columnMeta{
				Name: name,
				Type: type_,
				PK:   pk,
			})
		}
		if cRows.Err() != nil {
			return nil, fmt.Errorf("%w: failed to iterate table column metadata for %s", err, table.Name)
		}
		_ = cRows.Close()
		tables[table.Name] = table
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: failed to iterate table metadata entries", err)
	}

	return &dbMeta{
		Filename: filename,
		WAL:      wal,
		Tables:   tables,
	}, nil
}

type tableMeta struct {
	Name         string
	WithoutRowID bool
	Columns      []columnMeta
}

type columnMeta struct {
	Name string
	Type string
	PK   int
}
