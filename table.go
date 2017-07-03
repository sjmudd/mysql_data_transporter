package main

import (
	"fmt"
	"strings"

	"database/sql"
)

// Table holds information on the table structure and has a db handle to connect to it
type Table struct {
	schema string
	table  string
	pkCol  string
	pkType string
	db     *sql.DB
}

// NewTable returns a new Table
func NewTable(schema, table string, db *sql.DB) *Table {
	return &Table{
		schema: schema,
		table:  table,
		db:     db,
	}
}

const nullError = "converting driver.Value type <nil>"

// GetPrimaryKey finds the primary key of the given table. It assumes
// it should be a single column integer type but doesn't fully validate
// this yet.
func (t *Table) GetPrimaryKey() error {

	sql := `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_name = ? AND COLUMN_KEY = 'PRI'`

	rows, err := t.db.Query(sql, t.schema, t.table)
	if err != nil {
		return fmt.Errorf("db.Query failed while trying to get primary key: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&t.pkCol, &t.pkType)
		if err != nil {
			return fmt.Errorf("rows.Scan failed while trying to get primary key: %v", err)
		}
		// Missing: check data_type in
		// Missing: check one row back
		break
	}
	err = rows.Err()
	if err != nil {
		return fmt.Errorf("rows.Err() is set when getting primary key: %v", err)
	}
	return nil
}

// GetChunk returns the chunk given the conditions provided
func (t *Table) GetChunk(firstChunk bool, previous int, size int) *Chunk {
	sql := fmt.Sprintf("SELECT MIN(%s) FROM %s.%s", t.pkCol, t.schema, t.table)
	if !firstChunk {
		sql = sql + fmt.Sprintf(" WHERE %s > %d", t.pkCol, previous)
	}

	var start int
	err := t.db.QueryRow(sql).Scan(&start)
	if err != nil {
		// catch NULLs which will show up as errors (not an error, just no more data)
		if strings.Contains(err.Error(), nullError) {
			fmt.Printf("%s.%s: GetChunk(%v,%d,%d) no data WHERE %s > %d\n", t.schema, t.table, firstChunk, previous, size, t.pkCol, previous)
			return nil
		}

		// show any real errors
		fmt.Printf("rows.Scan failed while trying to get next chunk: %v\n", err)
		return nil
	}
	fmt.Printf("%s.%s: GetChunk(%v,%d,%d) -> (%d,%d)\n", t.schema, t.table, firstChunk, previous, size, start, start+size)
	return NewChunk(t.schema, t.table, start, start+size)
}

// read the pk from the table and generate chunk info
func (t *Table) readPKs(c chan *Chunk, done chan *Table, chunkSize int) {
	err := t.GetPrimaryKey()
	if err != nil {
		fmt.Printf("%s.%s: Failed to get primary key: %v\n", t.schema, t.table, err)
		done <- t
		return
	}

	if t.pkType != "bigint" && t.pkType != "int" {
		fmt.Printf("%s.%s: Wrong primary key type %q\n", t.table, t.schema, t.pkType)
		done <- t
		return
	}

	fmt.Printf("%s.%s: PK `%s` (%s)\n", t.schema, t.table, t.pkCol, t.pkType)

	chunk := t.GetChunk(true, 0, chunkSize-1)
	for chunk != nil {
		// process chunk
		c <- chunk

		// get the next one
		chunk = t.GetChunk(false, chunk.end+1, chunk.end+chunkSize)
	}
	done <- t
}
