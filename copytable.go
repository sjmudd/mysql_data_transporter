package main

import (
	"database/sql"
	"fmt"
)

type TableCopier struct {
	config *Config
	db     *sql.DB
}

func NewTableCopier(db *sql.DB, config *Config) *TableCopier {
	return &TableCopier{
		config: config,
		db:     db,
	}
}

func (tc *TableCopier) Pre() {
	fmt.Printf("TableCopier Pre() should stop replication\n")
}

func (tc *TableCopier) Run() {
	fmt.Printf("TableCopier Run()\n")
	srcChunkChannel := collectChunks(tc.db, tc.config)
	for chunk := range srcChunkChannel {
		tc.Push(chunk)
	}
}

// will use the pk info, select the rows and then push them to the destination server
func (tc *TableCopier) Push(chunk *Chunk) {
	// do nothing
	// fmt.Printf("pushData: %+v\n", chunk)
}

func (tc *TableCopier) Post() {
	fmt.Printf("TableCopier Post() should tell us position of the source\n")
}

// collectChunks will do PK looksup of the tables in config.tables
// and push out the PK chunks
func collectChunks(db *sql.DB, config *Config) chan *Chunk {
	// c holds a queue of chunks
	chunks := make(chan *Chunk, config.srcPKChanSize)
	fmt.Printf("collectChunks: made channel chunks\n")

	// for getting signals back that each table is done
	done := make(chan *Table)
	fmt.Printf("collectChunks: made channel done\n")

	go func() {
		for _, table := range config.tables {
			t := NewTable(config.schema, table, db)
			t.readPKs(chunks, done, config.chunk_size)
		}
	}()

	go func() {
		// Ordering is irrelevant. Get the done signal for each scanned table.
		for i := range config.tables {
			t := <-done // collect the signal we're done
			fmt.Printf("%d: %s.%s finished\n", i, t.schema, t.table)
		}
		close(chunks) // close chunks when we've been told by everyone we are done
	}()

	return chunks
}
