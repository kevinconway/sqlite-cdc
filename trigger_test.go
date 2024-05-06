// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package cdc

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
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

func TestWideTables(t *testing.T) {
	t.Parallel()
	db := testDB(t)
	defer db.Close()

	columnCount := 1000 // This is the default max stack depth in SQLite
	var b strings.Builder
	b.WriteString("CREATE TABLE test (")
	for x := 0; x < columnCount; x = x + 1 {
		b.WriteString(fmt.Sprintf("col%d INT", x))
		if x < columnCount-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	_, err := db.Exec(b.String())
	require.NoError(t, err)

	b.Reset()
	params := make([]any, columnCount)
	b.WriteString("INSERT INTO test VALUES (")
	for x := 0; x < columnCount; x = x + 1 {
		params[x] = x
		b.WriteString("?")
		if x < columnCount-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString(")")
	_, err = db.Exec(b.String(), params...)
	require.NoError(t, err)

	h := newHandler()
	batchSize := defaultMaxBatchSize
	c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, c.Bootstrap(ctx))
	require.NoError(t, c.Close(ctx))

	results := h.Changes()
	require.Len(t, results, 1)
	require.Len(t, results[0], 1)
	ch := results[0][0]
	afterMap := make(map[string]any)
	require.NoError(t, json.Unmarshal(ch.After, &afterMap))
	require.Len(t, afterMap, columnCount)
}

func BenchmarkTableSizes(b *testing.B) {
	columnCounts := []int{1, 5, 10, 20, 30, 40, 50, 100, 200, 400, 800, 1000}
	for _, columnCount := range columnCounts {
		b.Run(fmt.Sprintf("columns=%d", columnCount), func(b *testing.B) {
			db := testDB(b)
			defer db.Close()

			var builder strings.Builder
			builder.WriteString("CREATE TABLE test (")
			for x := 0; x < columnCount; x = x + 1 {
				builder.WriteString(fmt.Sprintf("col%d INT", x))
				if x < columnCount-1 {
					builder.WriteString(", ")
				}
			}
			builder.WriteString(")")
			_, err := db.Exec(builder.String())
			require.NoError(b, err)

			builder.Reset()
			params := make([]any, columnCount)
			builder.WriteString("INSERT INTO test VALUES (")
			for x := 0; x < columnCount; x = x + 1 {
				params[x] = x
				builder.WriteString("?")
				if x < columnCount-1 {
					builder.WriteString(", ")
				}
			}
			builder.WriteString(")")
			_, err = db.Exec(builder.String(), params...)
			require.NoError(b, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &handlerNull{}
			batchSize := defaultMaxBatchSize
			c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
			require.NoError(b, err)
			defer c.Close(ctx)
			trig := c.(*triggers)

			b.ResetTimer()
			for n := 0; n < b.N; n = n + 1 {
				require.NoError(b, trig.bootstrap(ctx))
			}
		})
	}
}

func BenchmarkBootstrapSizes(b *testing.B) {
	columnCounts := []int{1, 5, 10, 20}
	rowCounts := []int{1, 10, 100, 1000, 10000}
	batchSizes := []int{1, 10, 100, 1000, 10000}
	for _, columnCount := range columnCounts {
		for _, rowCount := range rowCounts {
			for _, batchSize := range batchSizes {
				b.Run(fmt.Sprintf("columns=%d, rows=%d, batch=%d", columnCount, rowCount, batchSize), func(b *testing.B) {
					db := testDB(b)
					defer db.Close()

					var builder strings.Builder
					builder.WriteString("CREATE TABLE test (")
					for x := 0; x < columnCount; x = x + 1 {
						builder.WriteString(fmt.Sprintf("col%d INT", x))
						if x < columnCount-1 {
							builder.WriteString(", ")
						}
					}
					builder.WriteString(")")
					_, err := db.Exec(builder.String())
					require.NoError(b, err)

					builder.Reset()
					params := make([]any, columnCount)
					builder.WriteString("INSERT INTO test VALUES (")
					for x := 0; x < columnCount; x = x + 1 {
						params[x] = x
						builder.WriteString("?")
						if x < columnCount-1 {
							builder.WriteString(", ")
						}
					}
					builder.WriteString(")")
					for x := 0; x < rowCount; x = x + 1 {
						_, err = db.Exec(builder.String(), params...)
						require.NoError(b, err)
					}

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					h := &handlerNull{}
					c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
					require.NoError(b, err)
					defer c.Close(ctx)
					trig := c.(*triggers)

					b.ResetTimer()
					for n := 0; n < b.N; n = n + 1 {
						require.NoError(b, trig.bootstrap(ctx))
					}
				})
			}
		}
	}
}

func BenchmarkBlobSizes(b *testing.B) {
	blobSizes := []int{16, 64, 256, 1024, 4096, 16384, 32768, 65536, 131072, 262144, 524288, 1048576}
	for _, blobSize := range blobSizes {
		b.Run(fmt.Sprintf("blob=%d", blobSize), func(b *testing.B) {
			db := testDB(b)
			defer db.Close()

			_, err := db.Exec(`CREATE TABLE test (col BLOB)`)
			require.NoError(b, err)

			blobBody := make([]byte, blobSize)
			for x := 0; x < blobSize; x = x + 1 {
				blobBody[x] = byte(x % 256)
			}
			_, err = db.Exec(`INSERT INTO test VALUES (?)`, blobBody)
			require.NoError(b, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			h := &handlerNull{}
			batchSize := defaultMaxBatchSize
			c, err := New(db, h, []string{testTableName}, WithMaxBatchSize(batchSize), WithBlobSupport(true))
			require.NoError(b, err)
			defer c.Close(ctx)
			trig := c.(*triggers)

			b.ResetTimer()
			for n := 0; n < b.N; n = n + 1 {
				require.NoError(b, trig.bootstrap(ctx))
			}
		})
	}
}

func generateRecords(t tOrB, db *sql.DB, n int, offset int) {
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

func createTable(t tOrB, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(sqlCreateTestTable)
	require.NoError(t, err)
}

func createTableWithoutRowID(t tOrB, db *sql.DB) {
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

type tOrB interface {
	Errorf(format string, args ...interface{})
	FailNow()
	Helper()
	TempDir() string
}

func testDB(t tOrB) *sql.DB {
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
