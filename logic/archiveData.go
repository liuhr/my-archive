package logic

import (
	"my-archive/base"
	"database/sql"
	db_mysql "my-archive/db"
	"github.com/outbrain/golib/log"
	"strings"
	"fmt"
	"my-archive/util"
	"sync"
	"regexp"
)




type ArchiveData struct {
	 archiveDataContext *base.ArchiveDataContext
	 mysqlInstances []*singleArchiveData
	 instanceWaitGroup *sync.WaitGroup
}

type singleArchiveData struct {
	archiveDataContext *base.ArchiveDataContext
	schemaTable []*singleTableInfo
	dbConn *singleDbInfo
	slaveDbs []*singleDbInfo
	tableWaitGroup *sync.WaitGroup
}

type singleDbInfo struct {
	hostIp string
	port uint
	dbConn *sql.DB
}
type singleTableInfo struct {
	schemaName string
	tableName string
	tableVersion uint64
	archiveVersion int64
	archiveCount uint64
	archiveByte uint64
}

func NewArchiveData(archiveDataContext *base.ArchiveDataContext) *ArchiveData{
	return &ArchiveData{
		archiveDataContext:archiveDataContext,
		instanceWaitGroup:&sync.WaitGroup{},
	}
}


func (this *ArchiveData) DoArchiveData() error {
	var err error
	if this.archiveDataContext.ArchiveType==base.SHARDING_ARCHIVE_TYPE{
		err=this.initShardingMysqlInstances()
	} else if this.archiveDataContext.ArchiveType==base.NORMAL_ARCHIVE_TYPE {
		if this.archiveDataContext.DbIp!="" {
			err=this.initCmdMysqlInstances()
		} else {
			err=this.initNormalMysqlInstances()
		}
	}

	if err!=nil {
		return err
	}
	this.changeMysqlBinPath()
	this.archiveDataContext.ArchiveLogId=WriteArchiveTableLog(this.archiveDataContext.ArchiveId)
	this.archiveDataContext.SetArchiveStatus(base.ARCHIVE_PROCESSING)
	for _,archiveInstance:=range this.mysqlInstances {
		this.instanceWaitGroup.Add(1)
		go func(archiveInstance *singleArchiveData) {
			defer this.instanceWaitGroup.Done()
			if err:=archiveInstance.doSingleArchiveData();err!=nil {
				log.Errorf("实例 %s:%d 执行归档有错:%s",archiveInstance.dbConn.hostIp,archiveInstance.dbConn.port,err.Error())
			}
		} (archiveInstance)
	}
	this.instanceWaitGroup.Wait()
	this.archiveDataContext.SetArchiveStatus(base.ARCHIVE_SUCCESS)
	UpdateArchiveTableLogStatus(this.archiveDataContext.ArchiveCount,this.archiveDataContext.ArchiveByte,this.archiveDataContext.ArchiveStatus,this.archiveDataContext.ArchiveLogId)
	return nil
}



func (this *ArchiveData) initShardingMysqlInstances() error{
	masterInfos:=GetShardingMasterInfo(this.archiveDataContext.SourceDbName)
	if len(masterInfos)==0 {
		return log.Errorf("sharding db:%s has not found",this.archiveDataContext.SourceDbName)
	}
	for _,masterInfo:=range masterInfos {
		instance:=masterInfo["instance"]
		myServerId:=masterInfo["myserverid"]
		host,port,err:=GetHostPort(instance)
		if err!=nil {
			log.Error("对于数据库 %s:%s 不能连接到它的master:%s",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,instance)
			continue
		}
		if !CheckShardingHasSingleMaster(instance) {
			log.Error("对于数据库 %s:%s, instance:%s有多个master或没有master",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,instance,instance)
			continue
		}
		dbConn,err:=db_mysql.NewDefaultDbConn(host,port,"")
		if err!=nil {
			log.Errorf(err.Error())
			continue
		}
		this.archiveDataContext.Username,this.archiveDataContext.Password=db_mysql.GetDefaultUserPassword()
		masterDbInfo:= newSingleDbInfo(host,port,dbConn)

		singleArchive:= newSingleArchiveData(masterDbInfo,this.archiveDataContext)
		for _,slaveInfo:=range GetShardingSlaveInfo(this.archiveDataContext.SourceDbName,myServerId)  {
			host,port,err:=GetHostPort(slaveInfo["instance"])
			if err!=nil {
				log.Error("对于数据库 %s:%s 不能连接到它的slave:%s",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,slaveInfo)
				continue
			}
			slaveDb,err:=db_mysql.NewDefaultDbConn(host,port,"")
			if err!=nil {
				log.Errorf(err.Error())
				continue
			}
			slaveDbInfo:=newSingleDbInfo(host,port,slaveDb)
			singleArchive.slaveDbs=append(singleArchive.slaveDbs,slaveDbInfo)
		}
		this.mysqlInstances=append(this.mysqlInstances,singleArchive)
	}
	return nil
}

func (this *ArchiveData) initNormalMysqlInstances() error{
	masterInfo:=GetNormalMasterInfo(this.archiveDataContext.SourceDbName)
	if len(masterInfo)==1 {
		instance:=masterInfo[0]["instance"]
		myServerId:=masterInfo[0]["myserverid"]
		host,port,err:=GetHostPort(instance)
		if err!=nil {
			return log.Error("对于数据库 %s:%s 不能连接到它的master:%s",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,instance)
		}
		dbConn,err:=db_mysql.NewDefaultDbConn(host,port,this.archiveDataContext.SourceDbName)
		if err!=nil {
			return log.Errorf(err.Error())
		}
		this.archiveDataContext.Username,this.archiveDataContext.Password=db_mysql.GetDefaultUserPassword()
		masterDbInfo:= newSingleDbInfo(host,port,dbConn)
		singleArchive:=newSingleArchiveData(masterDbInfo,this.archiveDataContext)
		for _,slaveInfo:=range GetNormalSlaveInfo(this.archiveDataContext.SourceDbName,myServerId)  {
			host,port,err:=GetHostPort(slaveInfo["instance"])
			if err!=nil {
				log.Error("对于数据库 %s:%s 不能连接到它的slave:%s",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,slaveInfo)
				continue
			}
			slaveDb,err:=db_mysql.NewDefaultDbConn(host,port,this.archiveDataContext.SourceDbName)
			if err!=nil {
				log.Errorf(err.Error())
				continue
			}
			slaveDbInfo:=newSingleDbInfo(host,port,slaveDb)
			singleArchive.slaveDbs=append(singleArchive.slaveDbs,slaveDbInfo)
		}
		this.mysqlInstances=append(this.mysqlInstances,singleArchive)
		return nil
	} else {
		return log.Errorf("db:%s has more tow master or no master",this.archiveDataContext.SourceDbName)
	}
}

func (this *ArchiveData) initCmdMysqlInstances() error{
	log.Info("使用指定的连接串%s:%d执行归档",this.archiveDataContext.DbIp,this.archiveDataContext.DbPort)
	sourceDb,err:=db_mysql.NewConfigDbConn(this.archiveDataContext.DbIp,this.archiveDataContext.DbPort,this.archiveDataContext.Username,this.archiveDataContext.Password,this.archiveDataContext.SourceDbName,db_mysql.Default_charset)
	if err!=nil {
		return log.Errorf(err.Error())
	}
	masterDbInfo:= newSingleDbInfo(this.archiveDataContext.DbIp,this.archiveDataContext.DbPort,sourceDb)
	singleArchive:=newSingleArchiveData(masterDbInfo,this.archiveDataContext)
	for _,slaveInfo:=range strings.Split(this.archiveDataContext.CheckSlaves,base.SLAVES_INFO_SPLIT)  {
		host,port,err:=GetHostPort(slaveInfo)
		if err!=nil {
			log.Error("对于数据库 %s:%s 不能连接到它的slave:%s",this.archiveDataContext.SourceDbName,this.archiveDataContext.SourceTable,slaveInfo)
			continue
		}
		slaveDb,err:=db_mysql.NewConfigDbConn(host,port,this.archiveDataContext.Username,this.archiveDataContext.Password,this.archiveDataContext.SourceDbName,db_mysql.Default_charset)
		if err!=nil {
			log.Errorf(err.Error())
			continue
		}
		slaveDbInfo:=newSingleDbInfo(host,port,slaveDb)
		singleArchive.slaveDbs=append(singleArchive.slaveDbs,slaveDbInfo)
	}
	this.mysqlInstances=append(this.mysqlInstances,singleArchive)

	return nil
}

func (this *ArchiveData) changeMysqlBinPath() {
	if this.archiveDataContext.MysqlBinPath==base.PREFIX_MYSQL_BIN_PATH {
		sql:="select version() version"
		result:=db_mysql.DBQueryOne(this.mysqlInstances[0].dbConn.dbConn,sql)
		mysqlVersion:=result["version"]
		reg:=regexp.MustCompile(`\d+\.\d+\.\d+`)
		mysqlVersion=reg.FindString(mysqlVersion)
		mysqlBinPath:=fmt.Sprintf("/opt/tiger/app/percona-%s/bin",mysqlVersion)
		if util.FileExists(fmt.Sprintf("%s/mysqldump",mysqlBinPath)) {
			this.archiveDataContext.MysqlBinPath=mysqlBinPath
		}
	}
}

func GetHostPort(dbInfo string) (string,uint,error){
	infos:=strings.Split(dbInfo,base.HOST_PORT_SPLIT)
	if len(infos)!=2 {
		return "", 0, fmt.Errorf("can not split host port from:%s",dbInfo)
	}
	return infos[0],util.ConvStrToUint(infos[1]),nil
}

func newSingleDbInfo(host string,port uint,dbConn *sql.DB) *singleDbInfo{
	return &singleDbInfo{
		hostIp:host,
		port:port,
		dbConn:dbConn,
	}
}

func newSingleArchiveData(singleDbInfo *singleDbInfo,archiveDataContext *base.ArchiveDataContext) *singleArchiveData{
	return &singleArchiveData{
		dbConn:singleDbInfo,
		archiveDataContext:archiveDataContext,
		tableWaitGroup:&sync.WaitGroup{},
	}
}
