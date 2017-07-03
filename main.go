package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// get primary key of a table
func getPrimaryKey(db *sql.DB, schema, table string) (string, string) {
	var pk, colType string

	sql := `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_name = ? AND COLUMN_KEY = 'PRI'`

	rows, err := db.Query(sql, schema, table)
	if err != nil {
		log.Fatal("Query failed", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&pk, &colType)
		if err != nil {
			log.Fatal("scan failed", err)
		}
		// Missing: check data_type in
		// Missing: check one row back
		break
	}
	err = rows.Err()
	if err != nil {
		log.Fatal("rows.Err()", err)
	}
	return pk, colType
}

func getMinimumValue(db *sql.DB, schema, table, pk string) int {
	var min int
	sql := fmt.Sprintf("SELECT MIN(%s) FROM %s.%s", pk, schema, table)

	rows, err := db.Query(sql)
	if err != nil {
		log.Fatal("Query failed", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&min)
		if err != nil {
			log.Fatal("scan failed", err)
		}
		break
	}
	err = rows.Err()
	if err != nil {
		log.Fatal("rows.Err()", err)
	}
	return min
}

func main() {
	var method = flag.String("method", "copy", "Method to copy data. Valid values: copy, blackhole")
	var tables = flag.String("tables", "", "List of comma separated tables to copy")

	config := NewConfig()
	flag.Parse()

	fmt.Printf("connecting to mysql\n")
	db, err := sql.Open("mysql", config.srcURI+config.schema)
	if err != nil {
		log.Fatalf("Unable to connect to MysQL: %+v", err)
	}
	defer db.Close()

	// check it works
	err = db.Ping()
	if err != nil {
		log.Fatalf("Unable to ping MysQL (src): %+v", err)
	}

	var a Copier
	// add table configuration
	if *tables != "" {
		config.tables = strings.Split(*tables, ",")
	}
	// define copy method
	if *method == "copy" {
		a = NewTableCopier(db, config)
	} else if *method == "blackhole" {
		a = NewBlackHoleCopier(db, config)
	} else {
		log.Fatal("Invalid method. Should be one of copy/blackhole")
	}

	fmt.Printf("Using method: %s\n", *method)
	a.Pre()
	a.Run()
	a.Post()
}
