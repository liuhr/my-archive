package db

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"sync"
	"my-archive/util"
	"github.com/outbrain/golib/log"
)

const (
	default_dbconn_user="root"
	default_dbconn_pass="16641307019e35b4d6b2bf120f48cfd039f11c2328517cccd4922539bd2b7a03"
	Default_charset="utf8mb4"
	default_dbhost="127.0.0.1"
	default_dbport=3306
	default_dbname="dbplatmeta"
)

type ConnPoolContext struct {
	RecordDb *sql.DB
	dbMutex *sync.Mutex
}

var connPoolContext *ConnPoolContext

func init() {
	connPoolContext = &ConnPoolContext{
		dbMutex: &sync.Mutex{},
	}
	connPoolContext.newConn()
}


func GetRecordDB() *sql.DB {
	if connPoolContext.RecordDb == nil {
		connPoolContext.newConn()
	}
	return connPoolContext.RecordDb
}

func (this *ConnPoolContext) newConn() (*sql.DB,error){
	conn:=MysqlConn{
		DBConn:DBConn{
			DbHost:default_dbhost,
			DbPort:default_dbport,
			DbUser:default_dbconn_user,
			DbPass:default_dbconn_pass,
		},
		DbName:default_dbname,
		DbCharset:Default_charset,
	}
	conn.DBConn.DbPass,_=util.Decrypt(conn.DBConn.DbPass)
	dbConn,err:=DbConn(&conn)
	if err!=nil {
		return nil ,log.Fatalf("can not connect to db:%s",err.Error())
	}
	this.setPoolConn(dbConn)
	return dbConn,nil
}

func (this *ConnPoolContext) setPoolConn(dbConn *sql.DB) {
	this.dbMutex.Lock()
	defer this.dbMutex.Unlock()
	this.RecordDb=dbConn
}

func GetDefaultUserPassword() (string,string) {
	dbpass,_:=util.Decrypt(default_dbconn_pass)
	return default_dbconn_user,dbpass
}
