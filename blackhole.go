package main

import (
	"database/sql"
	"fmt"
)

type BlackHoleCopier struct {
	config *Config
	db     *sql.DB
	cache  *Cache
}

func NewBlackHoleCopier(db *sql.DB, config *Config) *BlackHoleCopier {
	return &BlackHoleCopier{
		config: config,
		db:     db,
		cache:  NewCache(config.destTableSuffix),
	}
}

func (bhc *BlackHoleCopier) Pre() {
	fmt.Printf("blackhole: Pre() should rename tables\n")
	for _, table := range bhc.config.tables {
		t := fmt.Sprintf("%s.%s", bhc.config.schema, table)
		new_t := bhc.cache.Get(t)
		fmt.Printf("Pre: RENAME TABLE %s TO %s\n",
			t, new_t)
		fmt.Printf("Pre: CREATE TABLE %s LIKE %s\n",
			new_t, t)
		fmt.Printf("Pre: ALTER TABLE %s ENGINE=blackhole\n",
			new_t)
	}
}

func (bhc *BlackHoleCopier) Run() {
	fmt.Printf("blackhole: Run() should insert into original tables in chunks\n")
	srcChunkChannel := collectChunks(bhc.db, bhc.config)
	for chunk := range srcChunkChannel {
		bhc.Push(chunk)
	}
}

func (bhc *BlackHoleCopier) Post() {
	fmt.Printf("blackhole: Post() rename tables back\n")
	for _, table := range bhc.config.tables {
		fmt.Printf("DROP TABLE IF EXISTS %s.%s\n",
			bhc.config.schema,
			table)
		fmt.Printf("RENAME TABLE %s.%s TO %s.%s\n",
			bhc.config.schema,
			bhc.cache.Get(table),
			bhc.config.schema,
			table)
	}
}

func (bhc *BlackHoleCopier) Push(chunk *Chunk) {
	sql := fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.%s WHERE %s BETWEEN %d AND %d",
		chunk.schema,
		chunk.table,
		"chunk.orig_schema",
		"chunk.orig_table",
		"chunk.pk",
		chunk.start,
		chunk.end)
	fmt.Printf("SQL: %s\n", sql)
}
