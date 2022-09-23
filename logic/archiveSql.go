package logic

import (
	"fmt"
	db_mysql "my-archive/db"
	"strings"
	"my-archive/base"
	"database/sql"
	"time"
	"github.com/outbrain/golib/log"
	"my-archive/util"
)

func GetArchiveConfig(archiveId int64) map[string]string{
	sql:=fmt.Sprintf("select source_db,source_table,archive_type,time_field,where_field,task_server,task_time,max_slave_delay,db_load,batch_delete_rows,max_delete_row,online_keep_days,offline_keep_days,start_time,stop_time,table_parallel,access_require,data_dump_path,is_save_data from archive_table_config where id=%d and is_delete=0 and access_require=1",archiveId)
	return db_mysql.DBQueryOne(db_mysql.GetRecordDB(),sql)
}

func GetNormalMasterInfo(schemaName string) []map[string]string{
	sql:=fmt.Sprintf("select distinct instance,myserverid from mysql_upload_mate where role='master' and  schema_name='%s'",schemaName)
	return db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
}

func GetNormalSlaveInfo(schemaName string,masterServerId string) []map[string]string{
	sql:=fmt.Sprintf("select distinct instance,myserverid from mysql_upload_mate where role='slave' and schema_name='%s' and masterservierid='%s'",schemaName,masterServerId)
	return db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
}

func GetShardingMasterInfo(schemaName string) []map[string]string{
	schemaName=strings.Replace(schemaName,"?","",1)
	sql:=fmt.Sprintf("select distinct instance,myserverid from mysql_upload_mate where role='master' and schema_name regexp '^%s[0-9]{1,4}' ",schemaName)
	return db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
}

func GetShardingSlaveInfo(schemaName string,masterServerId string) []map[string]string{
	schemaName=strings.Replace(schemaName,"?","",1)
	sql:=fmt.Sprintf("select distinct instance,myserverid from mysql_upload_mate where role='slave' and schema_name regexp '^%s[0-9]{1,4}' and masterservierid='%s'",schemaName,masterServerId)
	return db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
}

func CheckSchemaHasSingleMaster(schemaName string) bool {
	sql:=fmt.Sprintf("select count(*) cnt from mysql_upload_mate where role='master' and  schema_name='%s'",schemaName)
	return db_mysql.DBQueryCount(db_mysql.GetRecordDB(),sql)==1
}

func CheckShardingHasSingleMaster(instance string) bool {
	sql:=fmt.Sprintf("select distinct schema_name from mysql_upload_mate where role='master' and  instance='%s'",instance)
	schemas:=db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
	if len(schemas)==0 {
		return false
	}
	for _,schema:=range schemas {
		schemaName:=schema["schema_name"]
		if CheckSchemaHasSingleMaster(schemaName) == false {
			return false
		}
	}
	return true
}

func WriteArchiveTableLog(archiveId int64) uint64{
	sql:=fmt.Sprintf("insert into archive_table_log(archive_id,task_start_time,task_status) values(%d,now(),'%s')",archiveId,base.ARCHIVE_PROCESSING)
	return uint64(db_mysql.ExecInsert(db_mysql.GetRecordDB(),sql))
}

func UpdateArchiveTableLogStatus(archiveCount uint64,archiveByte uint64,archiveStatus string,archiveId uint64) {
	sql:=fmt.Sprintf("update archive_table_log set total_archive_rows=%d,total_archive_byte=%d,task_end_time=now(),task_status='%s' where id=%d ",archiveCount,archiveByte,archiveStatus,archiveId)
	db_mysql.ExecUpdate(db_mysql.GetRecordDB(),sql)
}

func GetTableVersionCrc32(archiveId int64,schemaName string,tableName string) map[string]string {
	sql:=fmt.Sprintf("select id,table_meta_checksum from archive_table_meta_version where archive_id=%d and schema_name='%s' and table_name='%s'",archiveId,schemaName,tableName)
	return db_mysql.DBQueryOne(db_mysql.GetRecordDB(),sql)
}

func WriteTableVersion(dbConn *sql.DB,archiveId int64,schemaName string,tableName string,tableCrc32 string) uint64 {
	sql:=fmt.Sprintf("show create table `%s`.`%s` ",schemaName,tableName)
	createTable:=db_mysql.DBQueryOne(dbConn,sql)
	if len(createTable)!=0 {
		sql="insert into archive_table_meta_version(archive_id,schema_name,table_name,table_meta_checksum,table_meta_text) values(?,?,?,?,?)"
		log.Infof(sql)
		log.Infof("%v",createTable)
		result,_:=db_mysql.GetRecordDB().Exec(sql,archiveId,schemaName,tableName,tableCrc32,createTable["Create Table"])
		lastInsertId,_:=result.LastInsertId()
		return uint64(lastInsertId)
	}
	return 0
}

func WriteArchiveTableDetailLog(archiveTableLodId uint64,schemaName string,tableName string,startTime time.Time,archiveStatus string,batchArchiveRows uint64,tableMetaVersionId uint64,archiveDataPath string,archiveByte,archiveVersion int64,archiveLog string) uint64 {
	kv:=&db_mysql.KV{
		"archive_table_log_id":archiveTableLodId,
		"start_time":util.GetDatetime(startTime),
		"end_time":util.NowDatetime(),
		"archive_log":archiveLog,
		"archive_status":archiveStatus,
		"batch_archive_rows":batchArchiveRows,
		"table_meta_version_id":tableMetaVersionId,
		"archive_data_path":archiveDataPath,
		"archive_byte":archiveByte,
		"schema_name":schemaName,
		"table_name":tableName,
		"archive_version":archiveVersion,
	}
	orm:=db_mysql.ORM{
		DB:db_mysql.GetRecordDB(),
		Table:"archive_table_detail_log",
		KV:kv,
	}
	result,err:=orm.Insert()
	if err!=nil {
		log.Errorf(err.Error())
		return 0
	} else {
		lastInsertId,_:=result.LastInsertId()
		return uint64(lastInsertId)
	}
	//sql:="insert into archive_table_detail_log(archive_table_log_id,start_time,end_time,archive_status,batch_archive_rows,table_meta_version_id) values (%d,now(),'%s','%s',%d,%d)"
	//sql=fmt.Sprintf(sql,archiveTableLodId,util.GetDatetime(endTime),archiveStatus,batchArchiveRows)
}

func GetExpireArchiveData(expireData string ,archiveId int64) []map[string]string{
	sql:=fmt.Sprintf("select t1.id,t1.archive_data_path,t1.schema_name,t1.table_name from archive_table_detail_log t1,archive_table_log t2 where t1.is_delete=0 and t1.start_time<'%s' and t1.archive_table_log_id=t2.id and t2.archive_id=%d ",expireData,archiveId)
	return db_mysql.DBQueryAll(db_mysql.GetRecordDB(),sql)
}

func UpdateExpireArchiveData(archiveDetailLodId uint64,archiveLog string) int64{
	sql:=fmt.Sprintf("update archive_table_detail_log set is_delete=1 ,archive_log=concat(archive_log,'%s'),archive_status='%s',end_time=now() where id=%d ",archiveLog,base.ARCHIVE_DELETE,archiveDetailLodId)
	return db_mysql.ExecUpdate(db_mysql.GetRecordDB(),sql)
}
