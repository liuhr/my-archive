package db


type DBConn struct{
	DbHost string
	DbPort uint
	DbUser string
	DbPass string
}

type MysqlConn struct {
	DbCharset string
	DbName string
	DBConn
}

type PgConn struct {
	DbName string
	DBConn
}

type BaseContext struct {
	RecordDb DBConn
	AlarmDb MysqlConn
}

const (
	Master = "Master"
	Slave = "Slave"
)


