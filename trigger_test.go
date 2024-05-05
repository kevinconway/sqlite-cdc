// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestBootstrapWithRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTable(t, db)
	generateRecords(t, db, count, 0)

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, c.Bootstrap(ctx))
	require.NoError(t, c.Close(ctx))

	expectedBatches := count / batchSize
	if count%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}

	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count, totalChanges)
}

func TestCDCWithRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTable(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)
	defer c.Close(ctx)

	require.NoError(t, c.Setup(ctx))

	go func() {
		require.NoError(t, c.CDC(ctx))
	}()
	time.Sleep(5 * time.Millisecond) // force a scheduler break to get CDC going
	generateRecords(t, db, count, 0)
	time.Sleep(time.Second) // wait some time for CDC to finish

	expectedBatches := count / batchSize
	if count%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}
	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count, totalChanges)
}

func TestBootstrapWithoutRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTableWithoutRowID(t, db)
	generateRecords(t, db, count, 0)

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, c.Bootstrap(ctx))
	require.NoError(t, c.Close(ctx))

	expectedBatches := count / batchSize
	if count%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}

	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count, totalChanges)
}

func TestCDCWithoutRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTableWithoutRowID(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)
	defer c.Close(ctx)

	require.NoError(t, c.Setup(ctx))

	go func() {
		require.NoError(t, c.CDC(ctx))
	}()
	time.Sleep(5 * time.Millisecond) // force a scheduler break to get CDC going
	generateRecords(t, db, count, 0)
	time.Sleep(time.Second) // wait some time for CDC to finish

	expectedBatches := count / batchSize
	if count%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}
	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count, totalChanges)
}

func TestBootstrapAndCDCWithRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTable(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)
	defer c.Close(ctx)

	require.NoError(t, c.Setup(ctx))
	generateRecords(t, db, count, 0)
	go func() {
		require.NoError(t, c.CDC(ctx))
	}()
	time.Sleep(5 * time.Millisecond) // force a scheduler break to get CDC going
	generateRecords(t, db, count, count)
	time.Sleep(time.Second) // wait some time for CDC to finish

	expectedBatches := (count * 2) / batchSize
	if (count*2)%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}
	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count*2, totalChanges)
}

func TestBootstrapAndCDCWithoutRowID(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	count := 1024
	createTableWithoutRowID(t, db)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize))
	require.NoError(t, err)
	defer c.Close(ctx)

	require.NoError(t, c.Setup(ctx))
	generateRecords(t, db, count, 0)
	go func() {
		require.NoError(t, c.CDC(ctx))
	}()
	time.Sleep(5 * time.Millisecond) // force a scheduler break to get CDC going
	generateRecords(t, db, count, count)
	time.Sleep(time.Second) // wait some time for CDC to finish

	expectedBatches := (count * 2) / batchSize
	if (count*2)%batchSize != 0 {
		expectedBatches = expectedBatches + 1
	}
	results := h.Changes()
	require.Equal(t, expectedBatches, len(results))
	require.Len(t, results, expectedBatches)
	totalChanges := 0
	for _, changes := range results {
		totalChanges = totalChanges + len(changes)
	}
	require.Equal(t, count*2, totalChanges)
}

func generateRecords(t *testing.T, db *sql.DB, n int, offset int) {
	t.Helper()

	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	textValue := "foo"
	blobValue := []byte{0xDE, 0xAD, 0xBE, 0xAF}
	realValue := 3.14
	numericValue := 1
	for x := 0; x < n; x = x + 1 {
		intValue := x + offset
		_, err := tx.Exec(
			`INSERT INTO `+testTableName+` VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?
			)`,
			intValue, intValue, intValue, intValue, intValue, intValue, intValue, intValue, intValue,
			textValue, textValue, textValue, textValue, textValue, textValue, textValue, textValue,
			blobValue,
			realValue, realValue, realValue, realValue,
			numericValue, numericValue, numericValue, numericValue, numericValue,
		)
		require.NoError(t, err)
	}

	require.NoError(t, tx.Commit())
}

func createTable(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(sqlCreateTestTable)
	require.NoError(t, err)
}

func createTableWithoutRowID(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(sqlCreateTestTable + " WITHOUT ROWID")
	require.NoError(t, err)
}

const testTableName = "test"
const sqlCreateTestTable = `CREATE TABLE ` + testTableName + ` (
	a INT,
	b INTEGER,
	c TINYINT,
	d SMALLINT,
	e MEDIUMINT,
	f BIGINT,
	g UNSIGNED BIG INT,
	h INT2,
	i INT8,

	j CHARACTER(20),
	k VARCHAR(255),
	l VARYING CHARACTER(255),
	m NCHAR(55),
	n NATIVE CHARACTER(70),
	o NVARCHAR(100),
	p TEXT,
	q CLOB,

	r BLOB,

	s REAL,
	t DOUBLE,
	u DOUBLE PRECISION,
	v FLOAT,

	w NUMERIC,
	x DECIMAL(10,5),
	y BOOLEAN,
	z DATE,
	aa DATETIME,

	PRIMARY KEY (a,b,c)
)`

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()

	db, err := sql.Open("sqlite", filepath.Join(dir, "test.sqlite")+"?_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)")
	require.NoError(t, err)
	return db
}

type handler struct {
	changes []Changes
	lock    sync.Locker
}

func newHandler() *handler {
	return &handler{
		lock: &sync.Mutex{},
	}
}

func (h *handler) Changes() []Changes {
	h.lock.Lock()
	defer h.lock.Unlock()

	changes := h.changes
	h.changes = nil
	return changes
}

func (h *handler) HandleChanges(ctx context.Context, changes Changes) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.changes = append(h.changes, changes)
	return nil
}

type handlerNull struct{}

func (h *handlerNull) HandleChanges(ctx context.Context, changes Changes) error {
	return nil
}
