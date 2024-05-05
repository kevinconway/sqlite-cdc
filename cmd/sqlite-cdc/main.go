// SPDX-FileCopyrightText: © 2024 Kevin Conway <kevin@conway0.com>
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"

	_ "modernc.org/sqlite"

	cdc "github.com/kevinconway/sqlite-cdc"
	"github.com/kevinconway/sqlite-cdc/handlers"
)

type strList []string

func (l strList) String() string {
	return strings.Join(l, ",")
}

func (l *strList) Set(s string) error {
	*l = append(*l, s)
	return nil
}

type flags struct {
	dbFile        string
	dbParams      string
	tables        strList
	logTableName  string
	setup         bool
	teardown      bool
	cdc           bool
	bootstrap     bool
	destination   string
	batchSize     int
	disableSubsec bool
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx, _ = signal.NotifyContext(ctx, os.Interrupt)

	f := flags{}
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	fs.StringVar(&f.dbFile, "db", "", "SQLite file path")
	fs.StringVar(&f.dbParams, "db-params", "_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)", "SQLite connection parameters. See <https://pkg.go.dev/modernc.org/sqlite#Driver.Open> for parameter syntax")
	fs.StringVar(&f.logTableName, "log-table", "__cdc_log", "Name of the table to store CDC log entries")
	fs.Var(&f.tables, "table", "A table name to monitor. Can be specified multiple times")
	fs.BoolVar(&f.setup, "setup", false, "Perform initial setup of the database for CDC before starting in any mode")
	fs.BoolVar(&f.teardown, "teardown", false, "Perform teardown of the CDC tables and triggers. Setting the teardown flag prevents any other action. The process will perform the teardown and then exit")
	fs.BoolVar(&f.bootstrap, "bootstrap", false, "Read all existing records as if they are inserts and then exit. If this flag is set in addition to the cdc flag the cdc mode will begin after the bootstrap is complete")
	fs.BoolVar(&f.cdc, "cdc", false, "Run a continuous extraction of the CDC log.")
	fs.StringVar(&f.destination, "output", "-", "Write destination for log entries. Valid options are - for simplified stdout, json for full JSON stdout, or an HTTP URL that will receive POST requests containing batches of log entries. See <pkg.go.dev/github.com/kevinconway/sqlite-cdc/handlers#HTTP> for more.")
	fs.IntVar(&f.batchSize, "batch-size", 256, "The max number of log entries to collect in each batch")
	fs.BoolVar(&f.disableSubsec, "disable-subsec", false, "Disable subsecond time resolution to support old clients")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatalln(err)
	}

	dsn := fmt.Sprintf("%s?%s", f.dbFile, f.dbParams)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	var handler cdc.ChangesHandler
	switch f.destination {
	case "json":
		handler = &handlers.Debug{Output: os.Stdout}
	case "-":
		handler = &handlers.STDIO{Output: os.Stdout}
	default:
		handler = &handlers.HTTPBasicPOST{
			Client:   http.DefaultClient,
			Endpoint: f.destination,
		}
	}

	c, err := cdc.New(db, handler, f.tables,
		cdc.WithMaxBatchSize(f.batchSize),
		cdc.WithLogTableName(f.logTableName),
		cdc.WithoutSubsecondTime(f.disableSubsec),
	)
	if err != nil {
		log.Fatalln(err)
	}

	if f.setup {
		if err = c.Setup(ctx); err != nil {
			log.Fatalln(err)
		}
	}

	if f.teardown {
		if err = c.Teardown(ctx); err != nil {
			log.Fatalln(err)
		}
		return
	}

	switch {
	case f.bootstrap && !f.cdc:
		if err = c.Bootstrap(ctx); err != nil {
			log.Fatalln(err)
		}
		return
	case f.cdc && !f.bootstrap:
		go func() {
			defer cancel()
			if err = c.CDC(ctx); err != nil {
				log.Fatalln(err)
			}
		}()
	case f.cdc && f.bootstrap:
		go func() {
			defer cancel()
			if err = c.BootstrapAndCDC(ctx); err != nil {
				log.Fatalln(err)
			}
		}()
	case !f.cdc && !f.bootstrap:
		log.Fatalln("at least one of cdc or bootstrap must be set")
	default:
		panic("unreachable")
	}

	<-ctx.Done()
}
