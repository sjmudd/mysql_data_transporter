package main

// Config holds configuration data which should be configurable from the command line.
type Config struct {
	srcPKChanSize   int
	dstChanSize     int
	chunk_size      int
	srcURI          string
	dstURI          string
	schema          string
	tables          []string
	destTableSuffix string
	method          string
}

// NewConfig returns a hard-coded set of config values
func NewConfig() *Config {
	return &Config{
		srcPKChanSize:   2,
		dstChanSize:     10,
		chunk_size:      1000,
		srcURI:          "test_user:test_pass@tcp(127.0.0.1:3306)/",
		dstURI:          "test_user:test_pass@tcp(127.0.0.1:3306)/",
		schema:          "testdb",
		tables:          []string{"table1", "table2", "table3"},
		destTableSuffix: "_new",
	}
}
