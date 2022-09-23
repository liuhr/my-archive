package db

import (
	"database/sql"
	"github.com/outbrain/golib/log"
	"fmt"
)

const MYSQL_NULL  = "NULL"

func DBQueryAll(db *sql.DB, query string) []map[string]string {
	result := make([]map[string]string, 0, 500)
	rows, err := db.Query(query)
	if nil != err {
		log.Info("db.Query err:%s", err,query)
		return result
	}
	result=queryAllRows(rows)
	return result
}

func queryAllRows(rows *sql.Rows) []map[string]string{
	result := make([]map[string]string, 0, 500)
	defer func(rows *sql.Rows) {
		if rows != nil {
			rows.Close()
		}
	}(rows)

	columnsName, err := rows.Columns()
	if nil != err {
		log.Info("rows.Columns err:", err)
		return result
	}

	values := make([]sql.RawBytes, len(columnsName))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if nil != err {
			log.Info("rows.Next err:", err)
		}

		row_map := make(map[string]string)
		for i, col := range values {
			if col == nil {
				row_map[columnsName[i]] = "NULL"
			} else {
				row_map[columnsName[i]] = string(col)
			}
		}
		result = append(result, row_map)
	}

	err = rows.Err()
	if nil != err {
		log.Info("rows.Err:", err)
	}
	return result
}

func DBQueryCount(db *sql.DB, query string) int64 {
	var count int64
	rows := db.QueryRow(query)
	rows.Scan(&count)
	return count
}

func TxQuerySessionId(tx *sql.Tx) int64 {
	var sessionId int64
	rows := tx.QueryRow("select connection_id()")
	rows.Scan(&sessionId)
	return sessionId
}



func DBQueryOne(db *sql.DB, query string) map[string]string {
	result := make(map[string]string)

	rows, err := db.Query(query)
	if nil != err {
		log.Info("db.Query err:%s", err,query)
		return result
	}

	result=queryOneRow(rows)
	return result
}

func queryOneRow(rows *sql.Rows) map[string]string {
	result := make(map[string]string)
	defer func(rows *sql.Rows) {
		if rows != nil {
			rows.Close()
		}
	}(rows)

	columnsName, err := rows.Columns()
	if nil != err {
		log.Info("rows.Columns err:", err)
		return result
	}

	values := make([]sql.RawBytes, len(columnsName))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	rows.Next()
	err = rows.Scan(scanArgs...)
	if nil != err {
		//log.Info("rows.Next err:", err)
		return result
	}

	for i, col := range values {
		if col == nil {
			result[columnsName[i]] = "NULL"
		} else {
			result[columnsName[i]] = string(col)
		}
	}

	err = rows.Err()
	if nil != err {
		log.Info("rows.Err:", err)
	}
	return result
}

func TxQueryAll(tx *sql.Tx, query string) []map[string]string {
	result := make([]map[string]string, 0, 500)
	rows, err := tx.Query(query)
	if nil != err {
		log.Info("db.Query err:%s", err,query)
		return result
	}
	result=queryAllRows(rows)
	return result
}


func TXQueryOne(tx *sql.Tx, query string) map[string]string {
	result := make(map[string]string)
	rows, err := tx.Query(query)
	if nil != err {
		log.Info("db.Query err:%s", err,query)
		return result
	}
	result=queryOneRow(rows)
	return result
}

func ShowStatusVariable(db *sql.DB, variableName string) (result int64, err error) {
	query := fmt.Sprintf(`show global status like '%s'`, variableName)
	if err := db.QueryRow(query).Scan(&variableName, &result); err != nil {
		return 0, err
	}
	return result, nil
}
