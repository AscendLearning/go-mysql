package go_mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"os"
)

/**
Open connection to mysql
 */
func Open() (*sql.DB, error) {
	database := os.Getenv("DB_DATABASE")
	dbUser := os.Getenv("DB_USERNAME")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	conn := dbUser + ":" + dbPassword + "@tcp(" + dbHost + ":3306)/" + database

	return sql.Open(os.Getenv("DB_CONNECTION"), conn)
}

/**
Interface for entities that will use a repository
 */
type Entity interface {
	GenerateInsertSql() string
	ToInsertArgs() []interface{}
}

/**
Base repository struct
 */
type Repository struct {
	LastInsertId int64
	Db *sql.DB
}

/**
Find all query
 */
func (repo *Repository) FindAll(query string) ([]map[string]interface {}, error) {
	return repo.Query(query)
}

/**
Store a single repository into database
 */
func (repo *Repository) Store(entity Entity) error {
	stmtIns, err := repo.Db.Prepare(entity.GenerateInsertSql())
	if err != nil {
		return err
	}
	defer stmtIns.Close()

	res, err := stmtIns.Exec(entity.ToInsertArgs()...)
	if err != nil {
		return err
	}

	repo.LastInsertId, err = res.LastInsertId()
	if err != nil {
		return err
	}

	return nil
}

/**
Make a query against the database and return associative slice
 */
func (repo *Repository) Query(query string) ([]map[string]interface{}, error) {

	rows, err := repo.Db.Query(query)
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