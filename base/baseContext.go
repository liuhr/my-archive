package base

import (
	"sync"
	"database/sql"
)

const (
	Version="1.0"
)

const (
	NORMAL_ARCHIVE_TYPE=1
	SHARDING_ARCHIVE_TYPE=2
)

const (
	SCHEMA_TABLE_SPLIT="."
	HOST_PORT_SPLIT=":"
	SLAVES_INFO_SPLIT=","
)

const (
	ARCHIVE_INIT                  = "ARCHIVE_INIT"
	ARCHIVE_PROCESSING                  = "ARCHIVE_PROCESSING"
	ARCHIVE_SUCCESS               = "ARCHIVE_SUCCESS"
	ARCHIVE_DELETE                  = "ARCHIVE_DELETE"
	ARCHIVE_OUT_TIME_WINDOW       = "ARCHIVE_OUT_TIME_WINDOW"
	ARCHIVE_POWER_OFF             = "ARCHIVE_POWER_OFF"
	ARCHIVE_COPY_FAILED           = "ARCHIVE_COPY_FAILED"
	//ARCHIVE_MAX_RUNNING_FAILED    = "ARCHIVE_MAX_RUNNING_FAILED"
	//ARCHIVE_SLAVE_DELAY_FAILED    = "ARCHIVE_SLAVE_DELAY_FAILED"
	//ARCHIVE_TABLE_METADATA_FAILED = "ARCHIVE_TABLE_METADATA_FAILED"
	ARCHIVE_PENDING 			  = "ARCHIVE_PENDING"
	ARCHIVE_SYSTEM_SIGNAL_FAILED  = "ARCHIVE_SYSTEM_SIGNAL_FAILED"

)

const (
	MIN_BATCH_DELETE_ROWS     = 1000
	MAX_BATCH_DELETE_ROWS     = 100000
	DEFAULT_BATCH_DELETE_ROWS = 5000
	DEFAULT_MAX_DELETE_ROWS   = 1000000000
	DEFAULT_OFFLINE_KEEP_DAYS         = 180
	DEAFULT_SLAVE_DELAY		  = 10
	DEFAULT_SESSION_TIMEOUT   = 60000
	PREFIX_MYSQL_BIN_PATH           = "/usr/bin"
	PREFIX_DATA_DUMP_PATH           = "/tmp"
	HIBERNATE_INTERVAL        = 3
	MAX_RUNNING_COUNT         = 50
	CRITICAL_RUNNING_COUNT    = 100
	DEFAULT_SLEEP_SECOND      = 10
)


type ArchiveDataContext struct {
	Username string
	Password string
	DbIp string
	DbPort uint
	SourceDbName string
	SourceTable string
	ArchiveType uint
	CheckSlaves string
	DbLoad string
	MaxSlaveDelay uint
	ArchiveStatus string
	ArchiveId               int64
	ArchiveLogId            uint64
	//TaskServer           string
	TimeField            string
	BatchDeleteRows      uint64
	MaxDeleteRows        uint64
	ArchiveCount         uint64
	ArchiveByte			uint64
	OnlineKeepDays             uint
	OfflineKeepDays 	uint
	//untilDatetime        string
	//KeepData             bool
	AccessRequire        bool
	WhereFilter          string
	StopTime             string
	StartTime            string
	TableParallelArchive uint
	MonitorDb            *sql.DB
	//TargetDb             *archiveHostInfo
	throttleMutex   *sync.Mutex
	PanicAbortError chan error
	MysqlBinPath string
	DataDumpPath string
	ArchiveVersion int64
	IsSaveData bool
}

func NewArchiveDataContext() *ArchiveDataContext {
	return &ArchiveDataContext{
		throttleMutex:&sync.Mutex{},
		PanicAbortError:make(chan error),
		ArchiveStatus:ARCHIVE_INIT,
		AccessRequire:true,
	}
}


func (this *ArchiveDataContext) ChangeBatchDeleteRows() {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	if this.BatchDeleteRows>MAX_BATCH_DELETE_ROWS || this.BatchDeleteRows<MIN_BATCH_DELETE_ROWS {
		this.BatchDeleteRows=DEFAULT_BATCH_DELETE_ROWS
	}
}

func (this *ArchiveDataContext) SetArchiveStatus(archiveStatus string) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.ArchiveStatus=archiveStatus
}

func (this *ArchiveDataContext) SetAccessRequire(accessRequire bool) {
	this.throttleMutex.Lock()
	defer this.throttleMutex.Unlock()
	this.AccessRequire=accessRequire
}