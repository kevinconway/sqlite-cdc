package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	defaultLogTableName = "__cdc_log"
	defaultMaxBatchSize = 50
)

type Option func(*triggers) error

func WithLogTableName(name string) Option {
	return func(t *triggers) error {
		t.logTableName = name
		return nil
	}
}

func WithMaxBatchSize(size int) Option {
	return func(t *triggers) error {
		t.maxBatchSize = size
		return nil
	}
}

func WithoutSubsecondTime(v bool) Option {
	return func(t *triggers) error {
		t.subsec = !v
		return nil
	}
}

// New returns a CDC implementation based on triggers.
//
// This implementation works with any SQLite driver and uses only SQL operations
// to implement CDC. For each specified table to monitor, the implementation
// creates triggers for AFTER INSERT, AFTER UPDATE, and AFTER DELETE. These
// triggers populate a log table, named __cdc_log by default. The log table
// entries contain effectively identical information as the Change struct.
//
// The before and after images are stored as JSON objects in the log table. The
// JSON objects are generated from the column names and values in the table.
// Currently, only non-BLOB data types are supported in the before and after
// images. Any BLOB type column is left out of the image. Note that the
// limitation is associated with BLOB type data and not specifically BLOB type
// columns. You are recommended to use strict tables to avoid non-BLOB columns
// from containing BLOB data.
func New(db *sql.DB, handler ChangesHandler, tables []string, options ...Option) (CDC, error) {
	meta, err := newDBMeta(db)
	if err != nil {
		return nil, err
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("%w: failed to create fsnotify watcher", err)
	}
	result := &triggers{
		db:           db,
		meta:         meta,
		handler:      handler,
		tables:       tables,
		fnOnce:       &sync.Once{},
		watcher:      watcher,
		closed:       make(chan any),
		closeOnce:    &sync.Once{},
		logTableName: defaultLogTableName,
		maxBatchSize: defaultMaxBatchSize,
		subsec:       true,
	}
	for _, opt := range options {
		if err := opt(result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

type triggers struct {
	db           *sql.DB
	meta         *dbMeta
	handler      ChangesHandler
	tables       []string
	fnOnce       *sync.Once
	watcher      *fsnotify.Watcher
	closed       chan any
	closeOnce    *sync.Once
	logTableName string
	maxBatchSize int
	subsec       bool
}

func (c *triggers) CDC(ctx context.Context) error {
	var err error
	c.fnOnce.Do(func() {
		err = c.cdc(ctx)
	})
	return err
}

func (c *triggers) cdc(ctx context.Context) error {
	if err := c.watcher.Add(filepath.Dir(c.meta.Filename)); err != nil {
		return fmt.Errorf("%w: failed to add %q to fsnotify watcher", err, filepath.Dir(c.meta.Filename))
	}
	watchTargets := make(map[string]bool)
	watchTargets[c.meta.Filename] = true
	if c.meta.WAL {
		watchTargets[c.meta.Filename+"-wal"] = true
		watchTargets[c.meta.Filename+"-shm"] = true
	}
	for {
		select {
		case <-c.closed:
			return nil
		case <-ctx.Done():
			return c.Close(ctx)
		case event, ok := <-c.watcher.Events:
			if !ok {
				// The watcher was closed, most likely from an explicit call to
				// CDC.Close.
				return c.Close(ctx)
			}
			if event.Op == fsnotify.Chmod {
				continue
			}
			if !watchTargets[event.Name] {
				continue
			}
			if err := c.drainChanges(ctx); err != nil {
				return fmt.Errorf("%w: failed to process changes from the log", err)
			}
		case err, ok := <-c.watcher.Errors:
			if !ok {
				// The watcher was closed, most likely from an explicit call to
				// CDC.Close.
				return c.Close(ctx)
			}
			return fmt.Errorf("%w: fsnotify watcher error", err)
		}
	}
}

func (c *triggers) drainChanges(ctx context.Context) error {
	changes := make(Changes, 0, c.maxBatchSize)
	for {
		rows, err := c.db.QueryContext(ctx, `SELECT id, timestamp, tablename, operation, before, after FROM `+c.logTableName+` ORDER BY id ASC LIMIT ?`, c.maxBatchSize)
		if err != nil {
			return fmt.Errorf("%w: failed to select changes from the log", err)
		}
		defer rows.Close()
		maxID := new(int64)
		for rows.Next() {
			timestamp := new(string)
			table := new(string)
			operation := new(string)
			before := &sql.NullString{}
			after := &sql.NullString{}
			if err := rows.Scan(maxID, timestamp, table, operation, before, after); err != nil {
				return fmt.Errorf("%w: failed to read change record from the log", err)
			}
			var beforeJSON map[string]any
			if before.Valid {
				if err := json.Unmarshal([]byte(before.String), &beforeJSON); err != nil {
					return fmt.Errorf("%w: failed to unmarshal before json for %s", err, *table)
				}
			}
			var afterJSON map[string]any
			if after.Valid {
				if err := json.Unmarshal([]byte(after.String), &afterJSON); err != nil {
					return fmt.Errorf("%w: failed to unmarshal after json for %s", err, *table)
				}
			}
			ts, err := time.Parse("2006-01-02 15:04:05.999999999", *timestamp)
			if err != nil {
				return fmt.Errorf("%w: failed to parse timestamp %s from the log", err, *timestamp)
			}
			changes = append(changes, Change{
				Timestamp: ts,
				Table:     *table,
				Operation: strToOperation(*operation),
				Before:    beforeJSON,
				After:     afterJSON,
			})
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("%w: failed to read changes from the log", err)
		}
		if len(changes) < 1 {
			return nil
		}
		if err := c.handle(ctx, changes); err != nil {
			return fmt.Errorf("%w: failed to handle changes", err)
		}
		changes = changes[:0]
		tx, err := c.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: failed to create transaction to delete logs", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, `DELETE FROM `+c.logTableName+` WHERE id <= ?`, *maxID)
		if err != nil {
			return fmt.Errorf("%w: failed to delete handled logs", err)
		}
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("%w: failed to commit deletion of logs", err)
		}
	}
}

func (c *triggers) Bootstrap(ctx context.Context) error {
	var err error
	c.fnOnce.Do(func() {
		err = c.bootstrap(ctx)
	})
	return err
}

func (c *triggers) bootstrap(ctx context.Context) error {
	for _, table := range c.tables {
		if err := c.bootstrapTable(ctx, table); err != nil {
			return fmt.Errorf("%w: failed to bootstrap table %s", err, table)
		}
	}
	return nil
}

func (c *triggers) bootstrapTable(ctx context.Context, table string) error {
	t, ok := c.meta.Tables[table]
	if !ok {
		return fmt.Errorf("table %q not found in database", table)
	}
	q := sqlSelectFirst(t)
	rows, err := c.db.QueryContext(ctx, q, c.maxBatchSize)
	if err != nil {
		return fmt.Errorf("%w: failed to select first bootstrap rows for %s", err, table)
	}
	defer rows.Close()
	chs := make(Changes, 0, c.maxBatchSize)
	selections := append(sqlKeyValuesForTable(t), new(string))
	for rows.Next() {
		if err := rows.Scan(selections...); err != nil {
			return fmt.Errorf("%w: failed to read bootstrap row for %s", err, table)
		}
		body := selections[len(selections)-1].(*string)
		bodyJSON := make(map[string]any)
		if err := json.Unmarshal([]byte(*body), &bodyJSON); err != nil {
			return fmt.Errorf("%w: failed to unmarshal json for %s", err, table)
		}
		chs = append(chs, Change{
			Table:     table,
			Timestamp: time.Now(),
			Operation: Insert,
			After:     bodyJSON,
		})
	}
	if rows.Err() != nil {
		return fmt.Errorf("%w: failed to read bootstrap rows for %s", rows.Err(), table)
	}
	_ = rows.Close()
	if len(chs) < 1 {
		return nil
	}
	if len(chs) < c.maxBatchSize {
		return c.handle(ctx, chs)
	}
	if err := c.handle(ctx, chs); err != nil {
		return fmt.Errorf("%w: failed to handle bootstrap changes for %s", err, table)
	}
	keys := make([]any, len(selections)-1)
	copy(keys, selections[:len(selections)-1])
	for {
		q = sqlSelectNext(t)
		params := append(keys, c.maxBatchSize)
		rows, err = c.db.QueryContext(ctx, q, params...)
		if err != nil {
			return fmt.Errorf("%w: failed to select bootstrap rows for %s", err, table)
		}
		chs = chs[:0]
		for rows.Next() {
			selections = append(sqlKeyValuesForTable(t), new(string))
			if err := rows.Scan(selections...); err != nil {
				return fmt.Errorf("%w: failed to read bootstrap row for %s", err, table)
			}
			body := selections[len(selections)-1].(*string)
			bodyJSON := make(map[string]any)
			if err := json.Unmarshal([]byte(*body), &bodyJSON); err != nil {
				return fmt.Errorf("%w: failed to unmarshal json for %s", err, table)
			}
			chs = append(chs, Change{
				Table:     table,
				Timestamp: time.Now(),
				Operation: Insert,
				After:     bodyJSON,
			})
			copy(keys, selections[:len(selections)-1])
		}
		if rows.Err() != nil {
			return fmt.Errorf("%w: failed to read bootstrap rows for %s", rows.Err(), table)
		}
		_ = rows.Close()
		if len(chs) < 1 {
			return nil
		}
		if len(chs) < c.maxBatchSize {
			return c.handle(ctx, chs)
		}
		if err := c.handle(ctx, chs); err != nil {
			return fmt.Errorf("%w: failed to handle bootstrap changes for %s", err, table)
		}
	}
}

func (c *triggers) BootstrapAndCDC(ctx context.Context) error {
	var err error
	c.fnOnce.Do(func() {
		err = c.bootstrap(ctx)
		if err != nil {
			return
		}
		err = c.cdc(ctx)
	})
	return err
}
func (c *triggers) Setup(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: failed to create setup transaction", err)
	}
	defer tx.Rollback()

	logSQL := sqlCreateLogTable(c.logTableName)
	if _, err = tx.Exec(logSQL); err != nil {
		return fmt.Errorf("%w: failed to create log table", err)
	}
	for _, table := range c.tables {
		t, ok := c.meta.Tables[table]
		if !ok {
			return fmt.Errorf("table %q not found in database", table)
		}
		if _, err = tx.Exec(sqlCreateTableTriggerInsert(c.logTableName, t, c.subsec)); err != nil {
			return fmt.Errorf("%w: failed to create table trigger for inserts on %s", err, table)
		}
		if _, err = tx.Exec(sqlCreateTableTriggerUpdate(c.logTableName, t, c.subsec)); err != nil {
			return fmt.Errorf("%w: failed to create table trigger for updates on %s", err, table)
		}
		if _, err = tx.Exec(sqlCreateTableTriggerDelete(c.logTableName, t, c.subsec)); err != nil {
			return fmt.Errorf("%w: failed to create table trigger for deletes on %s", err, table)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("%w: failed to commit setup transaction", err)
	}
	return nil
}
func (c *triggers) Teardown(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w: failed to create teardown transaction", err)
	}
	defer tx.Rollback()

	for _, table := range c.tables {
		t, ok := c.meta.Tables[table]
		if !ok {
			return fmt.Errorf("table %q not found in database", table)
		}
		if _, err = tx.Exec(sqlDeleteTableTriggerInsert(t)); err != nil {
			return fmt.Errorf("%w: failed to delete table trigger for inserts on %s", err, table)
		}
		if _, err = tx.Exec(sqlDeleteTableTriggerUpdate(t)); err != nil {
			return fmt.Errorf("%w: failed to delete table trigger for updates on %s", err, table)
		}
		if _, err = tx.Exec(sqlDeleteTableTriggerDelete(t)); err != nil {
			return fmt.Errorf("%w: failed to delete table trigger for deletes on %s", err, table)
		}
	}
	logSQL := sqlDeleteLogTable(c.logTableName)
	if _, err = tx.Exec(logSQL); err != nil {
		return fmt.Errorf("%w: failed to delete log table", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("%w: failed to commit teardown transaction", err)
	}
	return nil
}
func (c *triggers) Close(ctx context.Context) error {
	c.closeOnce.Do(func() {
		close(c.closed)
	})
	if err := c.watcher.Close(); err != nil {
		return fmt.Errorf("%w: failed to close fsnotify watcher", err)
	}
	return c.db.Close()
}

func (c *triggers) handle(ctx context.Context, changes Changes) error {
	return c.handler.HandleChanges(ctx, changes)
}

func sqlCreateLogTable(name string) string {
	return `CREATE TABLE IF NOT EXISTS ` + name + ` (
		id INTEGER PRIMARY KEY,
		timestamp TEXT NOT NULL,
		tablename TEXT NOT NULL,
		operation TEXT NOT NULL,
		before TEXT,
		after TEXT
	)`
}
func sqlCreateTableTriggerInsert(logTable string, table tableMeta, subsec bool) string {
	return `CREATE TRIGGER IF NOT EXISTS ` + table.Name + `__cdc_insert AFTER INSERT ON ` + table.Name + ` BEGIN
		INSERT INTO ` + logTable + ` (timestamp, tablename, operation, before, after) VALUES
			(` + sqlDateTimeNow(subsec) + `, '` + table.Name + `', 'INSERT', NULL, ` + sqlJsonObject("NEW.", table.Columns) + `);
	END`
}
func sqlCreateTableTriggerUpdate(logTable string, table tableMeta, subsec bool) string {
	return `CREATE TRIGGER IF NOT EXISTS ` + table.Name + `__cdc_update AFTER UPDATE ON ` + table.Name + ` BEGIN
		INSERT INTO ` + logTable + ` (timestamp, tablename, operation, before, after) VALUES
			(` + sqlDateTimeNow(subsec) + `, '` + table.Name + `', 'UPDATE', ` + sqlJsonObject("OLD.", table.Columns) + `, ` + sqlJsonObject("NEW.", table.Columns) + `);
	END`
}
func sqlCreateTableTriggerDelete(logTable string, table tableMeta, subsec bool) string {
	return `CREATE TRIGGER IF NOT EXISTS ` + table.Name + `__cdc_delete AFTER DELETE ON ` + table.Name + ` BEGIN
		INSERT INTO ` + logTable + ` (timestamp, tablename, operation, before, after) VALUES
			(` + sqlDateTimeNow(subsec) + `, '` + table.Name + `', 'DELETE', ` + sqlJsonObject("OLD.", table.Columns) + `, NULL);
	END`
}
func sqlDateTimeNow(subsec bool) string {
	if subsec {
		return "datetime('now', 'subsec')"
	}
	return "datetime('now')"
}
func sqlDeleteTableTriggerInsert(table tableMeta) string {
	return `DROP TRIGGER IF EXISTS ` + table.Name + `__cdc_insert`
}
func sqlDeleteTableTriggerUpdate(table tableMeta) string {
	return `DROP TRIGGER IF EXISTS ` + table.Name + `__cdc_update`
}
func sqlDeleteTableTriggerDelete(table tableMeta) string {
	return `DROP TRIGGER IF EXISTS ` + table.Name + `__cdc_delete`
}
func sqlDeleteLogTable(table string) string {
	return `DROP TABLE IF EXISTS ` + table
}

func sqlJsonObject(prefix string, columns []columnMeta) string {
	var b strings.Builder
	b.WriteString("json_object(")
	for offset, column := range columns {
		if strings.ToUpper(column.Type) == "BLOB" {
			// TODO: Add some kind of support for blobs. Likely by using base64
			// encoding if it is available.
			continue
		}
		b.WriteString(fmt.Sprintf("'%s'", column.Name))
		b.WriteString(", ")
		b.WriteString(prefix + column.Name)
		if offset < len(columns)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	return b.String()
}

func sqlSelectFirst(table tableMeta) string {
	if !table.WithoutRowID {
		// panic(`SELECT rowid, ` + sqlJsonObject("", table.Columns) + ` AS body FROM ` + table.Name + `ORDER BY rowid LIMIT ?`)
		return `SELECT rowid, ` + sqlJsonObject("", table.Columns) + ` AS body FROM ` + table.Name + ` ORDER BY rowid LIMIT ?`
	}
	var keyCount int
	for _, column := range table.Columns {
		if column.PK != 0 {
			keyCount = keyCount + 1
		}
	}
	keyColumns := make([]string, keyCount)
	for _, column := range table.Columns {
		if column.PK != 0 {
			keyColumns[column.PK-1] = column.Name
		}
	}
	return `SELECT ` + strings.Join(keyColumns, ", ") + `, ` + sqlJsonObject("", table.Columns) + ` AS body FROM ` + table.Name + ` ORDER BY ` + strings.Join(keyColumns, ", ") + ` LIMIT ?`
}

func sqlSelectNext(table tableMeta) string {
	if !table.WithoutRowID {
		return `SELECT rowid, ` + sqlJsonObject("", table.Columns) + ` AS body FROM ` + table.Name + ` WHERE rowid > ? ORDER BY rowid LIMIT ?`
	}
	var keyCount int
	for _, column := range table.Columns {
		if column.PK != 0 {
			keyCount = keyCount + 1
		}
	}
	keyColumns := make([]string, keyCount)
	for _, column := range table.Columns {
		if column.PK != 0 {
			keyColumns[column.PK-1] = column.Name
		}
	}
	var b strings.Builder
	b.WriteString(`SELECT ` + strings.Join(keyColumns, ", ") + `, ` + sqlJsonObject("", table.Columns) + ` AS body FROM ` + table.Name)
	b.WriteString(` WHERE `)
	for offset, column := range keyColumns {
		b.WriteString(column)
		b.WriteString(" > ?")
		if offset < keyCount-1 {
			b.WriteString(" AND ")
		}
	}
	b.WriteString(` ORDER BY ` + strings.Join(keyColumns, ", ") + ` LIMIT ?`)

	return b.String()
}

func sqlKeyValuesForTable(table tableMeta) []any {
	if !table.WithoutRowID {
		return []any{new(int64)}
	}
	var keyCount int
	for _, column := range table.Columns {
		if column.PK != 0 {
			keyCount = keyCount + 1
		}
	}
	keyValues := make([]any, keyCount)
	for offset, column := range table.Columns {
		if column.PK != 0 {
			keyValues[offset] = new(any)
		}
	}
	return keyValues
}

func strToOperation(operation string) Operation {
	switch strings.ToUpper(operation) {
	case "INSERT":
		return Insert
	case "UPDATE":
		return Update
	case "DELETE":
		return Delete
	}
	return Operation("UNKNOWN")
}
