package go_sql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type MySql struct  {
	Db *sql.DB
}

func (mySql *MySql) Query(query string) ([]map[string]interface {}, error) {

	rows, err := mySql.Db.Query(query)
	if err != nil {
		return nil, err
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		myRow := make(map[string]interface{})

		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			myRow[columns[i]] = value
		}
		result = append(result, myRow)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}