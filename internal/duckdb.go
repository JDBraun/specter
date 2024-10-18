package internal

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/marcboeker/go-duckdb"
	"os"
	"strings"
)

func formatComplexField(field interface{}) string {
	if field == nil {
		return "<nil>"
	}
	switch field := field.(type) {
	case map[string]interface{}:
		var formattedParts []string
		for key, value := range field {
			formattedParts = append(formattedParts, fmt.Sprintf("%s: %v", key, value))
		}
		return strings.Join(formattedParts, ", ")
	default:
		return fmt.Sprintf("%v", field)
	}
}

func prettyPrintBlock(cols []string, rows [][]interface{}) {
	fmt.Println("------")
	for _, row := range rows {
		for i, col := range row {
			colVal := formatComplexField(col)
			fmt.Printf("%s: %v\n", cols[i], colVal)
		}
		fmt.Println("------")
	}
}

func ExecuteQueryOnParquet(query, preSignedURL, exportFile string) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %v", err)
	}
	defer db.Close()

	query = strings.Replace(query, "S3_PRESIGNED_URL", fmt.Sprintf("'%s'", preSignedURL), -1)
	rows, err := db.Query(query)

	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	var allRows [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		allRows = append(allRows, values)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over rows: %v", err)
	}

	if exportFile != "" {
		err = exportToCSV(cols, allRows, fmt.Sprintf("%s.csv", exportFile))
		if err != nil {
			return fmt.Errorf("failed to export results: %v", err)
		}
		fmt.Printf("Results exported to %s.csv\n", exportFile)
	} else {
		prettyPrintBlock(cols, allRows)
	}

	return nil
}

func exportToCSV(cols []string, rows [][]interface{}, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	err = writer.Write(cols)
	if err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	for _, row := range rows {
		stringRow := make([]string, len(row))
		for i, val := range row {
			stringRow[i] = formatComplexField(val)
		}
		err = writer.Write(stringRow)
		if err != nil {
			return fmt.Errorf("failed to write CSV row: %v", err)
		}
	}

	return nil
}
