package logic

import (
	"github.com/outbrain/golib/log"
	"my-archive/base"
	"fmt"
	"strings"
	"database/sql"
	db_mysql "my-archive/db"
	"my-archive/util"
	"time"
	"sync/atomic"
	"bytes"
)


const (
	DEFALUT_MAX_CHUNK_SIZE=10000*10000*1000
)

func (this *singleArchiveData) doSingleArchiveData() (error){
	this.doDeleteExpireArchiveData()
	log.Infof("开始对表%s:%s在db %s:%d 执行归档",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,this.dbConn.hostIp,this.dbConn.port)
	isSharding:=false
	if this.archiveDataContext.ArchiveType==base.SHARDING_ARCHIVE_TYPE {
		isSharding=true
	}
	archiveTables:=GetArchiveTable(this.dbConn.dbConn,this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,isSharding)
	parallelChan:=make(chan string,this.archiveDataContext.TableParallelArchive)
	for _,archiveTable:=range archiveTables {
		schemaName:=archiveTable["table_schema"]
		tableName:=archiveTable["table_name"]
		this.tableWaitGroup.Add(1)
		parallelChan <- fmt.Sprintf("%s.%s",schemaName,tableName)
		go func(schemaName string,tableName string) {
			singleTableInfo:=newSingleTableInfo(schemaName,tableName)
			defer func() {
				this.archiveDataContext.ArchiveCount=atomic.AddUint64(&this.archiveDataContext.ArchiveCount,singleTableInfo.archiveCount)
				this.archiveDataContext.ArchiveByte=atomic.AddUint64(&this.archiveDataContext.ArchiveByte,singleTableInfo.archiveByte)
				<-parallelChan
				this.tableWaitGroup.Done()
			}()
			this.doSingleTableArchive(singleTableInfo)
		}(schemaName,tableName)
	}
	this.tableWaitGroup.Wait()
	log.Infof("结束对表%s:%s在db %s:%d 的归档",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,this.dbConn.hostIp,this.dbConn.port)
	return nil
}


func (this *singleArchiveData) doSingleTableArchive(singleTableInfo *singleTableInfo) (error){
	primaryKey,err:=this.checkTablePrimaryKey(singleTableInfo.schemaName,singleTableInfo.tableName)
	if err!=nil {
		return err
	}
	if err=this.checkSqlSyntax(singleTableInfo);err!=nil {
		return err
	}
	minPrimaryKey,isError:=this.getArchiveStartKey(singleTableInfo,primaryKey)
	startKey:=util.ConvStrToUint64(minPrimaryKey)
	chunkStep:=this.archiveDataContext.BatchDeleteRows
	if isError && startKey<=0{
		return log.Errorf("对于表 %s.%s 不能获得它的最小主键",singleTableInfo.schemaName,singleTableInfo.tableName)
	}
	loopIndex:=0
	for {
		loopIndex+=1
		archiveLog := bytes.Buffer{}
		var dumpFile string
		starTime:=time.Now()
		archiveLog.WriteString(log.Infof("对于表 %s.%s 第 %d 次执行归档,startKey:%d",singleTableInfo.schemaName,singleTableInfo.tableName,loopIndex,startKey))
		archiveLog.WriteString(this.checkThrottleDbLoad())
		archiveLog.WriteString(this.checkSlaveDelay())
		_,message:=this.checkTableVersion(singleTableInfo)
		archiveLog.WriteString(message)
		if singleTableInfo.tableVersion==0 {
			log.Errorf("不能获得表结构版本信息")
			break
		}
		if !this.checkAccessRequire() {
			log.Infof("表的归档开关已关闭,结束对表的归档")
			break
		}
		if !this.checkTimeWindow() {
			log.Infof("现在时间 %s,表设置了归档时间段 %s 至 %s,结束执行",util.NowTime(),this.archiveDataContext.StartTime,this.archiveDataContext.StopTime)
			break
		}
		endKey:=startKey+chunkStep
		if this.archiveDataContext.IsSaveData {
			archiveLog.WriteString(util.NEWLINE+fmt.Sprintf("从 %s 开始执行表的归档数据保存",util.NowDatetime()))
			dumpFile,_=this.doSingleTableDumpData(singleTableInfo,primaryKey,startKey,endKey,loopIndex)
		}
		archiveLog.WriteString(util.NEWLINE+fmt.Sprintf("从 %s 开始执行表的归档数据删除",util.NowDatetime()))
		deleteRows:=this.doSingleTableDeleteData(singleTableInfo,primaryKey,startKey,endKey)
		if deleteRows<=0 {
			break
		}
		fileSize:=util.GetFileSize(dumpFile)
		singleTableInfo.archiveCount+=deleteRows
		singleTableInfo.archiveByte+=uint64(fileSize)
		WriteArchiveTableDetailLog(this.archiveDataContext.ArchiveLogId,singleTableInfo.schemaName,singleTableInfo.tableName,starTime,base.ARCHIVE_SUCCESS,deleteRows,singleTableInfo.tableVersion,dumpFile,fileSize,singleTableInfo.archiveVersion,archiveLog.String())
		startKey=endKey
		chunkStep=this.changeChunkStep(chunkStep,deleteRows)
	}
	return nil
}

func (this *singleArchiveData) doSingleTableDumpData(tableInfo *singleTableInfo,primaryKey string,startKey uint64,endKey uint64,loopIndex int) (string,error){
	dumpFile:=this.getDumpDataFile(tableInfo,loopIndex)
	cmd:=fmt.Sprintf("%s/mysqldump ",this.archiveDataContext.MysqlBinPath)
	cmd=fmt.Sprintf("%s -t --tz-utc=1 --skip-opt -e --set-gtid-purged=OFF --insert-ignore --default-character-set=utf8mb4 --max-allowed-packet=1073741824 --hex-blob=1 ",cmd)
	cmd=fmt.Sprintf("%s -h%s -P %d ",cmd,this.dbConn.hostIp,this.dbConn.port)
	cmd=fmt.Sprintf("%s -u%s -p%s ",cmd,this.archiveDataContext.Username,this.archiveDataContext.Password)
	cmd=fmt.Sprintf("%s --databases %s --tables %s ",cmd,tableInfo.schemaName,tableInfo.tableName)
	cmd=fmt.Sprintf("%s > %s ",cmd,dumpFile)
	whereCondition,_:=this.getWhereCondition(tableInfo)
	whereCondition=fmt.Sprintf(" %s and %s>=%d and %s<%d ",whereCondition,primaryKey,startKey,primaryKey,endKey)
	cmd=fmt.Sprintf("%s --where \"%s\" ",cmd,whereCondition)
	stdout,stderr,err:=util.ExecBlockShell(cmd)
	if err!=nil {
		return dumpFile,log.Errorf("stdout:%s,stderr:%s,err:%s",stdout,stderr,err.Error())
	}
	return dumpFile,nil
}

func (this *singleArchiveData) doSingleTableDeleteData(tableInfo *singleTableInfo,primaryKey string,startKey uint64,endKey uint64) uint64{
	sql:=fmt.Sprintf("delete from `%s`.`%s` where ",tableInfo.schemaName,tableInfo.tableName)
	whereCondition,_:=this.getWhereCondition(tableInfo)
	whereCondition=fmt.Sprintf(" %s and %s>=%d and %s<%d ",whereCondition,primaryKey,startKey,primaryKey,endKey)
	sql=fmt.Sprintf("%s %s",sql,whereCondition)
	return uint64(db_mysql.ExecDelete(this.dbConn.dbConn,sql))
}



func (this *singleArchiveData) changeChunkStep(chunkStep uint64,rows uint64) uint64{
	changeStep:=chunkStep
	if rows >=this.archiveDataContext.BatchDeleteRows {
		changeStep=changeStep/2
	} else if chunkStep>DEFALUT_MAX_CHUNK_SIZE{
		changeStep=DEFALUT_MAX_CHUNK_SIZE
	} else if changeStep < uint64(this.archiveDataContext.BatchDeleteRows) {
		changeStep=uint64(this.archiveDataContext.BatchDeleteRows)
	} else if rows*2 > this.archiveDataContext.BatchDeleteRows {
		log.Infof("not need change changeStep:%d,queryRows:%d",changeStep,rows)
	} else {
		changeStep=changeStep*2
	}
	log.Debugf("queryRows:%d,changeStep:%d",rows,changeStep)
	return changeStep
}




func (this *singleArchiveData) getWhereCondition(tableInfo *singleTableInfo) (string,bool){
	whereCondition:=" 1=1 "
	isHasWhereCondition:=false
	if this.archiveDataContext.WhereFilter!="" {
		whereCondition = fmt.Sprintf(" %s and %s ",whereCondition,this.archiveDataContext.WhereFilter)
		isHasWhereCondition=true
	}
	if this.archiveDataContext.OnlineKeepDays>0 && this.archiveDataContext.TimeField!="" {
		subDateTime:=util.SubDayDatetime(int(this.archiveDataContext.OnlineKeepDays))
		whereCondition= fmt.Sprintf("%s and %s<='%s 00:00:00' ",whereCondition,this.archiveDataContext.TimeField,util.GetDate(subDateTime))
		isHasWhereCondition=true
	}
	return whereCondition,isHasWhereCondition
}



func  (this *singleArchiveData) getArchiveStartKey(tableInfo *singleTableInfo,primaryKey string) (string,bool){
	whereCondition,isHasCondition:=this.getWhereCondition(tableInfo)
	if !isHasCondition {
		return "",false
	}
	uniqueKeyOrderQuery:="select `"+primaryKey+"` unique_key from `"+ tableInfo.schemaName+"`.`"+tableInfo.tableName + "` where " + whereCondition + " order by " + primaryKey+" limit 1"
	uniqueKeyMinQuery:="select min(`"+primaryKey+"`) unique_key from `"+ tableInfo.schemaName+"`.`"+tableInfo.tableName + "` where " + whereCondition
	return this.queryMinMaxPrimaryKeyForOneColumn(uniqueKeyOrderQuery,uniqueKeyMinQuery,tableInfo.schemaName,tableInfo.schemaName)
}



func (this *singleArchiveData) queryMinMaxPrimaryKeyForOneColumn(uniqueKeyOrderQuery string,uniqueKeyMinMaxQuery string,schemaName string,tableName string) (string,bool) {
	maxPrimaryKeyChan:=make(chan string,2)
	var orderQuerySessionId int64
	var minQuerySessionId int64
	isKillOrderQuerySession:=true
	isKillMinQuerySession:=true
	orderTx,err:=this.dbConn.dbConn.Begin()
	if err!=nil {
		log.Errorf("queryMaxPrimaryKeyForOneColumn for table:%s.%s error:%s",schemaName,tableName,err.Error())
		return "",true
	}
	orderQuerySessionId=db_mysql.TxQuerySessionId(orderTx)
	go func() {
		result:=db_mysql.TXQueryOne(orderTx,uniqueKeyOrderQuery)
		isKillOrderQuerySession=false
		maxPrimaryKeyChan<-result["unique_key"]
	}()
	minTx,err:=this.dbConn.dbConn.Begin()
	if err!=nil {
		log.Errorf("queryMaxPrimaryKeyForOneColumn for table:%s.%s error:%s",schemaName,tableName,err.Error())
		return "",true
	}
	minQuerySessionId=db_mysql.TxQuerySessionId(minTx)
	go func() {
		result:=db_mysql.TXQueryOne(minTx,uniqueKeyMinMaxQuery)
		isKillMinQuerySession=false
		maxPrimaryKeyChan<-result["unique_key"]
	}()
	select {
	case lastMinPrimaryKey:=<-maxPrimaryKeyChan:
		log.Debugf("table:%s.%s,isKillOrderQuerySession:%v,orderQuerySessionId:%v,isKillMinQuerySession:%v,minQuerySessionId:%v",schemaName,tableName,isKillOrderQuerySession,orderQuerySessionId,isKillMinQuerySession,minQuerySessionId)
		if isKillOrderQuerySession {
			log.Infof("because min query table:%s.%s session id:%d is already get min_unique_key kill current session id:%d",schemaName,tableName,minQuerySessionId,orderQuerySessionId)
			err:=db_mysql.KillSession(this.dbConn.dbConn,orderQuerySessionId)
			if err!=nil {
				log.Errorf("kill table:%s.%s session id:%d,err:%s",schemaName,tableName,orderQuerySessionId,err.Error())
			}
		} else {
			err:=orderTx.Commit()
			if err!=nil {
				log.Errorf("table:%s.%s orderTx:%d commit error:%s",schemaName,tableName,orderQuerySessionId,err.Error())
			}
		}
		if isKillMinQuerySession {
			log.Infof("because order query table:%s.%s session id:%d is already get min_unique_key kill current session id:%d",schemaName,tableName,orderQuerySessionId,minQuerySessionId)
			err:=db_mysql.KillSession(this.dbConn.dbConn,minQuerySessionId)
			if err!=nil {
				log.Errorf("kill table:%s.%s session id:%d,err:%s",schemaName,tableName,minQuerySessionId,err.Error())
			}
		} else {
			err:=minTx.Commit()
			if err!=nil {
				log.Errorf("table:%s.%s minTx:%d commit error:%s",schemaName,tableName,minQuerySessionId,err.Error())
			}
		}
		return lastMinPrimaryKey,false
	}
}

func (this *singleArchiveData) checkSqlSyntax(tableInfo *singleTableInfo) error{
	whereCondition,_:=this.getWhereCondition(tableInfo)
	sql:=fmt.Sprintf("explain select * from `%s`.`%s` where %s limit 1",tableInfo.schemaName,tableInfo.tableName,whereCondition)
	result:=db_mysql.DBQueryOne(this.dbConn.dbConn,sql)
	if len(result)==0 {
		return log.Errorf("sql:%s语法错误",sql)
	}
	if result["key"]==db_mysql.MYSQL_NULL {
		return log.Errorf("sql:%s没有能使用到索引",sql)
	} else {
		return nil
	}
}


func (this *singleArchiveData) getDumpDataFile(tableInfo *singleTableInfo,loopIndex int) string{
	dataFile:=fmt.Sprintf("%s/%s/%s",this.archiveDataContext.DataDumpPath,util.NowDate(),tableInfo.schemaName)
	if !util.PathExists(dataFile) {
		util.MakeDir(dataFile)
	}
	dataFile=fmt.Sprintf("%s/%s_%d_%d_%d.sql",dataFile,tableInfo.tableName,tableInfo.archiveVersion,time.Now().Unix(),loopIndex)
	return dataFile
}


func (this *singleArchiveData) checkAccessRequire() bool{
	archiveConfig:=GetArchiveConfig(this.archiveDataContext.ArchiveId)
	if util.ConvStrToUint(archiveConfig["access_require"]) !=1 {
		this.archiveDataContext.SetAccessRequire(false)
	}
	return this.archiveDataContext.AccessRequire
}

func (this *singleArchiveData) checkTimeWindow() bool{

	if this.archiveDataContext.StartTime!="" && this.archiveDataContext.StopTime!=""{
		nowTime:=util.NowTime()
		if !(nowTime > this.archiveDataContext.StartTime && nowTime < this.archiveDataContext.StopTime) {
			return false
		}
	}
	return true
}

func (this *singleArchiveData) checkTablePrimaryKey(schemaName string,tableName string) (string,error){
	primaryKey,isIntPrimaryKey:=IsIntPrimaryKey(this.dbConn.dbConn,schemaName,tableName)
	if !isIntPrimaryKey {
		return "",fmt.Errorf("表%s.%s没有int型主键或唯一键",schemaName,tableName)
	}
	return primaryKey,nil
}

func (this *singleArchiveData) checkTableVersion(tableInfo *singleTableInfo) (bool,string) {
	var isTableVersionUpdate bool
	var message string
	lastTableVersionCrc32:=GetTableVersionCrc32(this.archiveDataContext.ArchiveId,tableInfo.schemaName,tableInfo.tableName)
	newTableCrc32:=GetTableMetaCrc32(this.dbConn.dbConn,tableInfo.schemaName,tableInfo.tableName)
	if lastTableVersionCrc32["table_meta_checksum"]!=fmt.Sprintf("%d",newTableCrc32) {
		tableInfo.tableVersion=WriteTableVersion(this.dbConn.dbConn,this.archiveDataContext.ArchiveId,tableInfo.schemaName,tableInfo.tableName,fmt.Sprintf("%d",newTableCrc32))
		isTableVersionUpdate=true
		message=fmt.Sprintf("表结构发生变化,使用新的表版本:%d",tableInfo.tableVersion)
	} else {
		tableInfo.tableVersion=util.ConvStrToUint64(lastTableVersionCrc32["id"])
		isTableVersionUpdate=false
		message=fmt.Sprintf("表结构没有变化,使用上次的表版本:%d",tableInfo.tableVersion)
	}
	return isTableVersionUpdate,message
}

func newSingleTableInfo(schemaName string,tableName string) *singleTableInfo{
	tableInfo:=&singleTableInfo{
		schemaName:schemaName,
		tableName:tableName,
		archiveVersion:time.Now().Unix(),
	}
	return tableInfo
}


func GetArchiveTable(dbConn *sql.DB,schemaName string,tableName string,isSharding bool) []map[string]string {
	sql:=fmt.Sprintf("select table_schema,table_name,table_collation from information_schema.tables where table_type='BASE TABLE' and table_name='%s' ",tableName)
	if isSharding {
		schemaName=strings.Replace(schemaName,"?","",1)
		sql+=fmt.Sprintf(" and table_schema regexp '^%s[0-9]{1,4}'",schemaName)
	} else {
		sql+=fmt.Sprintf(" and table_schema='%s'",schemaName)
	}
	return db_mysql.DBQueryAll(dbConn,sql)
}


func GetUniqueKey(dbConn *sql.DB,schemaName string,tableName string)  ([]string,error) {
	query :="show index from `%s`.`%s` where key_name='primary'"
	query=fmt.Sprintf(query,schemaName,tableName)
	result:=db_mysql.DBQueryAll(dbConn,query)
	if len(result)==0 {
		log.Infof("table %s.%s has no primary key",schemaName,tableName)
		query :="show index from `%s`.`%s` where non_unique=0"
		query=fmt.Sprintf(query,schemaName,tableName)
		result=db_mysql.DBQueryAll(dbConn,query)
		if len(result)==0 {
			return nil,log.Errorf("表%s.%s没有主键或唯一键",schemaName,tableName)
		}
	}
	return util.CollectAllRowsToArray("column_name",result),nil

}

func IsIntPrimaryKey(dbConn *sql.DB,schemaName string,tableName string)  (string,bool) {
	isIntPrimaryKey:=false
	query :="show index from `%s`.`%s` where key_name='primary'"
	query=fmt.Sprintf(query,schemaName,tableName)
	result:=db_mysql.DBQueryAll(dbConn,query)
	columnName:=""
	if len(result)==1 {
		columnName=result[0]["Column_name"]
	}else if len(result)==0 {
		query :="show index from `%s`.`%s` where non_unique=0"
		query=fmt.Sprintf(query,schemaName,tableName)
		result=db_mysql.DBQueryAll(dbConn,query)
		if len(result)==1 {
			columnName=result[0]["Column_name"]
		}
	}
	if strings.TrimSpace(columnName)!="" {
		query :="select data_type from information_schema.columns where table_schema='%s' and table_name='%s' and column_name='%s' "
		query=fmt.Sprintf(query,schemaName,tableName,columnName)
		primaryKeyResult:=db_mysql.DBQueryOne(dbConn,query)
		if primaryKeyResult["data_type"] =="int" || primaryKeyResult["data_type"] =="bigint" || primaryKeyResult["data_type"] =="smallint" || primaryKeyResult["data_type"] =="tinyint"{
			isIntPrimaryKey=true
		}
	}

	return columnName,isIntPrimaryKey

}



func GetTableColumns(dbConn *sql.DB,schemaName string,tableName string)  (string,error) {
	query :="select column_name from information_schema.columns where table_schema='%s' and table_name='%s'"
	query=fmt.Sprintf(query,schemaName,tableName)
	query = query+" order by ordinal_position"
	result:=db_mysql.DBQueryAll(dbConn,query)
	if len(result)==0 {
		return "",log.Errorf("表%s不存在",schemaName,tableName)
	}
	columnNames:=""
	for _,mp:=range result {
		columnNames=columnNames+fmt.Sprintf("`%s`,",mp["column_name"])
	}
	return strings.TrimRight(columnNames,","),nil
}

func GetTableMetaCrc32(dbConn *sql.DB,schemaName string,tableName string) (uint32) {
	query:=fmt.Sprintf("select crc32(concat_ws('#',table_schema,table_name,column_name,column_type)) crc32 from information_schema.columns where table_schema='%s' and table_name='%s' order by ordinal_position",schemaName,tableName)
	columnInfo:=db_mysql.DBQueryAll(dbConn,query)
	columnCrc32String:=""
	for _,mp:=range columnInfo {
		columnCrc32String=columnCrc32String+fmt.Sprintf("%s",mp["crc32"])
	}
	return util.GetCrc32(columnCrc32String)
}

func (this *singleArchiveData) doDeleteExpireArchiveData() error{
	if this.archiveDataContext.OfflineKeepDays>0 {
		subDateTiem:=util.SubDayDatetime(int(this.archiveDataContext.OfflineKeepDays))
		archiveData:=GetExpireArchiveData(util.GetDatetime(subDateTiem),this.archiveDataContext.ArchiveId)
		totalExpiredData:=len(archiveData)
		for i,archiveDataMap:=range archiveData {
			archiveLog:=log.Infof("开始删除 %s:%s 表的过期数据,总进度:%d%%",archiveDataMap["schema_name"],archiveDataMap["table_name"],int(i*100/totalExpiredData))
			if util.FileExists(archiveDataMap["archive_data_path"]) {
				util.ExecBlockShell(fmt.Sprintf("rm -f %s",archiveDataMap["archive_data_path"]))
			}
			UpdateExpireArchiveData(util.ConvStrToUint64(archiveDataMap["id"]),archiveLog)
		}
	}
	return nil
}



