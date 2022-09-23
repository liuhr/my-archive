package main

import (
	"my-archive/base"
	"my-archive/logic"
	"my-archive/util"
	"flag"
	"fmt"
	"github.com/outbrain/golib/log"
	"os"
	"time"
)

var (
	version bool
	verbose bool
)

func main() {
	archiveDataContext:=base.NewArchiveDataContext()
	flag.StringVar(&archiveDataContext.Username, "username", "", "连接数据库的用户名")
	flag.StringVar(&archiveDataContext.Password, "password", "", "连接数据库的密码")
	flag.StringVar(&archiveDataContext.DbIp,"db_ip","","要连接的数据库ip")
	flag.UintVar(&archiveDataContext.DbPort,"db_port",3306,"要连接的数据库端口")
	flag.StringVar(&archiveDataContext.SourceDbName,"source_db_name","","")
	flag.StringVar(&archiveDataContext.SourceTable,"source_table_name","","要归档的表名")
	flag.StringVar(&archiveDataContext.TimeField,"time_field","","进行归档的时间字段")
	flag.UintVar(&archiveDataContext.ArchiveType,"archive_type",base.NORMAL_ARCHIVE_TYPE,"归档表类型,1:普通表,2:Sharing表")
	flag.StringVar(&archiveDataContext.CheckSlaves,"check_slaves","","进行延迟校验的slave,比如127.0.0.1:3306...")
	flag.UintVar(&archiveDataContext.MaxSlaveDelay,"max_slave_delay",base.DEAFULT_SLAVE_DELAY,"在归档期间slave的最大延迟秒,超过此值则暂停归档")
	flag.Int64Var(&archiveDataContext.ArchiveId,"archive_id",0,"归档的任务id,如果此值存在,此从已有配置中读取默认值")
	flag.Uint64Var(&archiveDataContext.BatchDeleteRows,"batch_archive_rows",base.DEFAULT_BATCH_DELETE_ROWS,"每批次归档的行数")
	flag.Uint64Var(&archiveDataContext.MaxDeleteRows,"max_archive_rows",0,"总共要归档的行数,默认不受限制")
	flag.UintVar(&archiveDataContext.OnlineKeepDays,"online_keep_days",0,"线上数据要保存的天数")
	flag.UintVar(&archiveDataContext.OfflineKeepDays,"offline_keep_days",base.DEFAULT_OFFLINE_KEEP_DAYS,"归档数据保存多少天后自动删除")
	flag.StringVar(&archiveDataContext.WhereFilter,"where_filter","","自定义归档的条件")
	flag.StringVar(&archiveDataContext.StartTime,"start_time","","归档可以执行的时间")
	flag.StringVar(&archiveDataContext.StopTime,"end_time","","归档结束的时间")
	flag.StringVar(&archiveDataContext.DbLoad,"db_load","","归档时要检测的db负载,比如 thread_running=50")
	flag.UintVar(&archiveDataContext.TableParallelArchive,"parallel_archive",1,"针对sharing表的并行归档数")
	flag.StringVar(&archiveDataContext.MysqlBinPath,"mysql_bin_path","","执行归档时mysqldump的路径")
	flag.StringVar(&archiveDataContext.DataDumpPath,"data_dump_path","","dump数据要保存的路径")
	flag.BoolVar(&archiveDataContext.IsSaveData,"is_save_data",true,"删除归档数据前是否要保存数据")
	flag.BoolVar(&version,"version",false,"查看系统版本")
	flag.BoolVar(&verbose,"verbose",false,"查看详细日志")
	flag.Parse()
	if version {
		fmt.Println(base.Version)
		os.Exit(-1)
	}
	if verbose {
		log.SetLevel(log.DEBUG)
	} else {
		log.SetLevel(log.INFO)
	}
	if archiveDataContext.ArchiveId>0 {
		archiveConfig:=logic.GetArchiveConfig(archiveDataContext.ArchiveId)
		if len(archiveConfig)==0 {
			log.Fatalf("不能根据archive_id:%d获得它的配置参数",archiveDataContext.ArchiveId)
		}
		if archiveDataContext.SourceTable=="" {
			archiveDataContext.SourceTable=archiveConfig["source_table"]
		}
		if archiveDataContext.SourceDbName=="" {
			archiveDataContext.SourceDbName=archiveConfig["source_db"]
		}
		if archiveDataContext.ArchiveType==base.NORMAL_ARCHIVE_TYPE {
			archiveDataContext.ArchiveType=util.ConvStrToUint(archiveConfig["archive_type"])
		}
		if archiveDataContext.TimeField=="" {
			archiveDataContext.TimeField=archiveConfig["time_field"]
		}
		if archiveDataContext.WhereFilter=="" {
			archiveDataContext.WhereFilter=archiveConfig["where_field"]
		}
		if archiveDataContext.MaxSlaveDelay==base.DEAFULT_SLAVE_DELAY {
			archiveDataContext.MaxSlaveDelay=util.ConvStrToUint(archiveConfig["max_slave_delay"])
		}
		if archiveDataContext.DbLoad=="" {
			archiveDataContext.DbLoad=archiveConfig["db_load"]
		}
		if archiveDataContext.BatchDeleteRows==base.DEFAULT_BATCH_DELETE_ROWS {
			archiveDataContext.BatchDeleteRows=util.ConvStrToUint64(archiveConfig["batch_delete_rows"])
		}
		archiveDataContext.ChangeBatchDeleteRows()
		if archiveDataContext.MaxDeleteRows==0 {
			archiveDataContext.MaxDeleteRows=base.DEFAULT_MAX_DELETE_ROWS
		}
		if archiveDataContext.OnlineKeepDays==0 {
			archiveDataContext.OnlineKeepDays=util.ConvStrToUint(archiveConfig["online_keep_days"])
		}
		if archiveDataContext.OfflineKeepDays==base.DEFAULT_OFFLINE_KEEP_DAYS {
			archiveDataContext.OfflineKeepDays=util.ConvStrToUint(archiveConfig["offline_keep_days"])
		}
		if archiveDataContext.StartTime=="" {
			archiveDataContext.StartTime=archiveConfig["start_time"]
		}
		if archiveDataContext.StopTime=="" {
			archiveDataContext.StopTime=archiveConfig["stop_time"]
		}
		if archiveDataContext.TableParallelArchive==1 {
			archiveDataContext.TableParallelArchive=util.ConvStrToUint(archiveConfig["table_parallel"])
		}
		if archiveDataContext.MysqlBinPath=="" {
			archiveDataContext.MysqlBinPath=base.PREFIX_MYSQL_BIN_PATH
		}
		if archiveDataContext.DataDumpPath=="" {
			if archiveConfig["data_dump_path"]!="" {
				archiveDataContext.DataDumpPath=archiveConfig["data_dump_path"]
			} else {
				archiveDataContext.DataDumpPath=base.PREFIX_DATA_DUMP_PATH
			}
		}
		if !archiveDataContext.IsSaveData {
			if util.ConvStrToInt(archiveConfig["is_save_data"])==1 {
				archiveDataContext.IsSaveData=true
			}
		}
	} else {
		archiveDataContext.ArchiveId=time.Now().Unix()
	}
	archiveDataContext.ArchiveVersion=time.Now().Unix()
	archiveData:=logic.NewArchiveData(archiveDataContext)
	err:=archiveData.DoArchiveData()
	if err!=nil {
		log.Error(err.Error())
	}

}
