package db

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
	"my-archive/util"
)

func DbConn(dbconn *MysqlConn) (*sql.DB,error) {

	var db *sql.DB
	var err error

	db, err = sql.Open("mysql", dbconn.DbUser+":"+dbconn.DbPass+"@tcp("+dbconn.DbHost+":"+fmt.Sprintf("%d",dbconn.DbPort)+")/"+dbconn.DbName+"?charset="+dbconn.DbCharset)
	if nil != err {
		return nil,fmt.Errorf("sql.Open err:(%s) exit !", err)
	}

	if err := db.Ping(); nil != err {
		return nil,fmt.Errorf("err:(%s) exit,db.Ping conn:%s  !",err,dbconn.DbUser+":"+dbconn.DbPass+"@tcp("+dbconn.DbHost+":"+fmt.Sprintf("%d",dbconn.DbPort)+")/"+dbconn.DbName+"?charset="+dbconn.DbCharset)
	}

	//db.SetMaxIdleConns(3)
	//db.SetConnMaxLifetime(time.Second * 60)

	return  db,nil
}

func NewConfigDbConn(hostIp string,port uint,username string,password string,dbName string,charset string) (*sql.DB,error) {
	dbconn:=MysqlConn{}
	dbconn.DbHost=hostIp
	dbconn.DbPort=port
	if dbName!="" {
		dbconn.DbName=dbName
	} else {
		dbconn.DbName="mysql"
	}
	dbconn.DbUser=username
	dbconn.DbPass=password
	dbconn.DbCharset=charset
	conn, err := DbConn(&dbconn)
	if nil != err {
		return nil,fmt.Errorf("sql.Open err:(%s) exit !", err)
	}
	return conn,nil
}

func NewDefaultDbConn(hostIp string,port uint,dbName string) (*sql.DB,error) {
	dbconn:=MysqlConn{}
	dbconn.DbHost=hostIp
	dbconn.DbPort=port
	if dbName!="" {
		dbconn.DbName=dbName
	} else {
		dbconn.DbName="mysql"
	}
	dbconn.DbUser=default_dbconn_user
	dbconn.DbPass,_=util.Decrypt(default_dbconn_pass)
	dbconn.DbCharset=Default_charset
	conn, err := DbConn(&dbconn)
	if nil != err {
		return nil,fmt.Errorf("sql.Open err:(%s) exit !", err)
	}
	return  conn,nil
}

func CloseConn(db *sql.DB) {
	db.Close()

}

func KillQuerySession(db *sql.DB, sessionId int64) error {
	_,err:=db.Exec(fmt.Sprintf("kill query %d",sessionId))
	return err

}

func KillSession(db *sql.DB, sessionId int64) error {
	_,err:=db.Exec(fmt.Sprintf("kill %d",sessionId))
	return err

}
