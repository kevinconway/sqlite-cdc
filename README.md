# SQLite-CDC

**A change-data-capture engine for SQLite.**

## Overview

This project implements change-data-capture, or CDC, for SQLite databases. The
current implementation works by installing triggers on target tables that record
the before and after states of a table modification to a change log table. The
engine then watches the SQLite files for changes, reads batches of change
records, and then sends them to a configurable destination for processing.

Each entry in the log contains the following information:

- Timestamp of change
- Type of change (INSERT, UPDATE, or DELETE)
- Name of source table
- Before (when type is UPDATE or DELETE)
- After (when type is INSERT or UPDATE)

The before and after fields contain the values of all columns from the source
table before and after the row was modified.

The engine can read up to a configurable number of records as a batch and
deliver these changes to some processing logic or destination. These change
records can then be used to build higher level replication systems such as
read-only replicas of SQLite data in a networked database such as MySQL,
client-side caches of data with automated invalidation, real-time ETL and data
warehousing, or even more advanced systems such as bidirectional replication
between independent databases.

## Project Status

Consider this project alpha or a work-in-progress.

My plan is to get this running in a real system by the end of 2024 and collect
some data on performance and reliability, especially in a highly concurrent
read/write environment.

## Running The Example Executable

The quickest way to try things out is to use the example executable in
`cmd/sqlite-cdc`. Point it to any existing SQLite database that you're
comfortable testing with and then make changes to your selected tables to see
CDC events stream to the console:
```bash
go run cmd/sqlite-cdc/main.go --output json --db test.sqlite --table my_table --setup --bootstrap --cdc
```
```json
{
    "table":"my_table",
    "timestamp":"2024-05-05T19:03:11.5681173-05:00",
    "operation":"INSERT",
    "before":null,
    "after":{"email":"noreply@example.com","favorite_color":"blue","id":1,"username":"example"}
}

{
    "table":"my_table",
    "timestamp":"2024-05-06T00:03:34Z",
    "operation":"UPDATE",
    "before":{"email":"noreply@example.com","favorite_color":"blue","id":1,"username":"example"},
    "after":{"email":"noreply@example.com","favorite_color":"green","id":1,"username":"example"}
}

{
    "table":"my_table",
    "timestamp":"2024-05-06T00:04:07Z",
    "operation":"DELETE",
    "before":{"email":"noreply@example.com","favorite_color":"green","id":1,"username":"example"},
    "after":null
}
```

When you're done testing you can run the following to clean up all CDC artifacts
from your database:
```bash
go run cmd/sqlite-cdc/main.go --db test.sqlite --table my_table --teardown
```

Use the `--help` flag to see all the available options. Here's a snapshot of
what's available:
```
Usage of sqlite-cdc:
  --batch-size int
        The max number of log entries to collect in each batch (default 256)
  --bootstrap
        Read all existing records as if they are inserts and then exit. If this flag is set in addition to the cdc flag the cdc mode will begin after the bootstrap is complete
  --cdc
        Run a continuous extraction of the CDC log.
  --db string
        SQLite file path
  --db-params string
        SQLite connection parameters. See <https://pkg.go.dev/modernc.org/sqlite#Driver.Open> for parameter syntax (default "_pragma=journal_mode(wal)&_pragma=busy_timeout(5000)")
  --disable-subsec
        Disable subsecond time resolution to support old clients
  --log-table string
        Name of the table to store CDC log entries (default "__cdc_log")
  --output string
        Write destination for log entries. Valid options are - for simplified stdout, json for full JSON stdout, or an HTTP URL that will receive POST requests containing batches of log entries. See <pkg.go.dev/github.com/kevinconway/sqlite-cdc/handlers#HTTP> for more. (default "-")
  --setup
        Perform initial setup of the database for CDC before starting in any mode
  --table value
        A table name to monitor. Can be specified multiple times
  --teardown
        Perform teardown of the CDC tables and triggers. Setting the teardown flag prevents any other action. The process will perform the teardown and then exit
```

### Limits Of The Executable

The example exists primarily for demonstrations and for quickly performing an
initial test of the system. In most cases, I expect that you will use the
example as a template for creating your own custom build that integrates your
own event handling logic and better integrates with your runtime environment by,
for example, adding instrumentation.

As is, the example executable can perform all the CDC responsibilities but
supports only three outputs:

- Simplified logging to STDOUT (ex: `<timestamp>: <table> <operation>`)
- JSON logging to STDOUT (shown in the previous section)
- HTTP POST

If you absolutely must use the example as-is then the HTTP POST output enables
you to redirect batches of CDC events to a specified URL. That receiver can then
implement any logic you need. Note that the current version of the project does
not include any form of built-in retries so your POST endpoint must implement
those internally. See <pkg.go.dev/github.com/kevinconway/sqlite-cdc/handlers#HTTP>
for details on the API contract.

## Extending The Engine

Custom builds need to provide two components: an event handler and a database
client.

The database client can be any SQLite driver for Go that implements the standard
library `databases/sql` driver interface. See
<https://github.com/cvilsmeier/go-sqlite-bench> for benchmarks covering most of
the popular drivers available for Go.

The event handler is anything that implements:
```golang
type ChangesHandler interface {
	HandleChanges(ctx context.Context, changes Changes) error
}
```
Each batch of changes given to your event handler will be in change order. The
engine makes only one call to the handler at a time so that batches are also
strictly processed in change order. If the handler returns an error then the
engine considers this a critical fault and shuts down. If the handler returns
success then the entire batch is considered successful and the relevant changes
are removed from the log. Your handler is responsible for durability or
reliability behaviors such as retries with backoff.

### API Documentation

<https://pkg.go.dev/github.com/kevinconway/sqlite-cdc>

## Limitations

### No BLOB Support

There is currently no support for BLOB columns or BLOB data. I expect to add
some kind of BLOB column support in the future but there is no practical way to
support BLOB data within non-BLOB columns in the current design.

In SQLite, there is a distinction between column type and data type. By default,
column types are not enforced and SQLite is capable of storing any data type
within any column. For example, a column may be defined with type INTEGER but
that does not prevent a client from inserting values that are of type TEXT or
BLOB.

The issue is that the triggers used to populate the change log table need to
specially handle BLOB data and the column type is the only available indicator
of what a value's type might be. As a result, any BLOB value in a non-BLOB
column will result in failure. I suggest using
[STRICT tables](https://www.sqlite.org/stricttables.html) to avoid this issue.

### Max 63 Columns Per Table

The engine cannot be used on tables with more than 63 columns. I plan to support
larger tables in the future.

The underlying issue is that the triggers used to populate the change log call a
function that takes the name and value of each column as an argument. SQLite has
a hard limit of 127 arguments per function call. For now that means a 63 column
limit because that results in 126 arguments.

### WAL Mode Required

Running `sqlite-cdc` on your database necessarily adds reads and writes that are
concurrent with your existing usage. Practically, enabling WAL mode is the only
way for this to work.

### Clients With Version Less Than 3.42.0

The log table contains a timestamp that represents the time of change. SQLite
datetime resolution is second precision by default. Version 3.42.0 added a
`subsecond` modifier to time functions that enables millisecond precision.

If any of your clients are older than 3.42.0 then you must use the
`WithoutSubsecondTime` option when constructing an engine or the
`--disable-subsec` flag in the example executable.

It's important to note that the version is associated with the client and not
the database file. It is possible to have both old and new clients operating
on the same database file.

## Why Not Use The Session API?

SQLite can be built with an extension called
[session](https://sqlite.org/sessionintro.html) that provides an API for
recording database changes and formatting them into change sets. The resulting
change sets contain very similar information to the current records in the
change log.

On the surface, the sessions extension appears to be a great fit for CDC.
Starting a session begins a capture of all changes to a select set of tables.
Each change record describes modifications to a table row. Those change records
accumulate in a log that can be fetched at any time. This is conceptually the
same as what this project does.

However, the sessions extension has three notable behaviors that make it the
wrong choice for general purpose CDC. The first is that session change records
are only materialized when reading the log and then only one is produced per row
that was modified. As a result, sessions is a poor way to receive real-time
change notifications. The second behavior is that the session log can only be
fetched as a whole but never resets. As a result, the session log grows
unbounded and there is no mechanism to filter out already handled changes. The
third behavior is that a session is not necessarily persisted beyond the
lifetime of the database connection it's associated with. As a result, there is
no way to restore CDC from the last known position if the system exited for any
reason.

The sessions extension appears to be designed to operate in a specific kind of
environment where SQLite is being used as a data structure rather than a typical
SQL database. As such, sessions work best when more tightly integrated into
application logic and aren't well suited for this kind of "bolt-on" or sidecar
model.

## Compatibility With Other Replication Tools

I have not yet tested with any of the SQLite replication tools I know about:

- <https://github.com/superfly/litefs>
- <https://github.com/benbjohnson/litestream>
- <https://github.com/maxpert/marmot>
- <https://github.com/rqlite/rqlite>

I expect that `rqlite` is incompatible because it requires writes through an
HTTP API. I also expect that `litefs` is incompatible because it operates
through FUSE which prevents sqlite-cdc from detecting file changes.

The `litestream` project _should_ be compatible because it uses a standard
file system and supports arbitrary SQLite clients. I also suspect `marmot` is
compatible but redundant because it implements a very similar trigger based
system to this project.

## License

This project is licensed under the Apache 2.0 terms. See `LICENSE` for full
details.

## Prior Art

This project is inspired by <https://github.com/JayJamieson/sqlite-cdc/> and
<https://github.com/maxpert/marmot>.
