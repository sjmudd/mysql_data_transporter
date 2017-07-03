package main

// Chunk contains the data to collect from the db.
type Chunk struct {
	schema string
	table  string
	start  int
	end    int
}

// NewChunk returns a new chunk with the specified input values
func NewChunk(schema, table string, start, end int) *Chunk {
	return &Chunk{
		schema: schema,
		table:  table,
		start:  start,
		end:    end,
	}
}
