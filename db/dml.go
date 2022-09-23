package db

import (
	"database/sql"
	"github.com/outbrain/golib/log"
)


func ExecInsert(db *sql.DB, sql string)  int64 {
	result,err:=db.Exec(sql)
	if err!=nil {
		log.Errorf("sql出错了,sql:%s,error:%s",sql,err.Error())
		return 0
	}
	insertId,err:=result.LastInsertId()
	if err!=nil {
		log.Errorf("sql出错了,sql:%s,error:%s",sql,err.Error())
	}
	return insertId
}


func ExecUpdate(db *sql.DB, sql string)  int64{

	result,err:=db.Exec(sql)
	if err!=nil {
		log.Errorf("sql出错了,sql:%s,error:%s",sql,err.Error())
	}
	rows,err:=result.RowsAffected()
	if err!=nil {
		log.Errorf("sql出错了,sql:%s,error:%s",sql,err.Error())
	}
	return rows

}


func ExecDelete(db *sql.DB, sql string) int64 {

	return  ExecUpdate(db,sql)

}


