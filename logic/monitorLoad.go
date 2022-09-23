package logic

import (
	"github.com/outbrain/golib/log"
	"time"
	"strings"
	"fmt"
	"strconv"
	db_mysql "my-archive/db"
	"my-archive/base"
	"bytes"
	"my-archive/util"
)


func ParseLoadMap(loadList string) (map[string]int64, error) {
	result := make(map[string]int64)
	if loadList == "" {
		return result, nil
	}

	loadConditions := strings.Split(loadList, ",")
	for _, loadCondition := range loadConditions {
		loadTokens := strings.Split(loadCondition, "=")
		if len(loadTokens) != 2 {
			return result, fmt.Errorf("转换负载均衡参数时出错: %s", loadCondition)
		}
		if strings.Trim(loadTokens[0]," ") == "" {
			return result, fmt.Errorf("转换负载均衡参数时变量名出错: %s", loadCondition)
		}
		if n, err := strconv.ParseInt(strings.Trim(loadTokens[1]," "), 10, 0); err != nil {
			return result, fmt.Errorf("转换负载均衡参数时变量值出错: %s", loadCondition)
		} else {
			result[strings.Trim(loadTokens[0]," " )] = n
		}
	}

	return result, nil
}



func (this *singleArchiveData) checkThrottleDbLoad() string{
	logBuffer := bytes.Buffer{}
	if this.archiveDataContext.DbLoad!="" {
		log.Infof("数据库 %s:%d ,系统定义了负载均衡参数:%s,开始监听系统负载",this.dbConn.hostIp,this.dbConn.port,this.archiveDataContext.DbLoad)
		loadMap,err:=ParseLoadMap(this.archiveDataContext.DbLoad)
		if err!=nil {
			log.Infof(err.Error())
		}
		if len(loadMap) > 0 {
			for {
				maxLoadMet, variableName, value, threshold, err :=  this.maxLoadIsMet(loadMap)
				log.Debugf("数据库 %s:%d,查询变量 %s=%d",this.dbConn.hostIp,this.dbConn.port,variableName,value)
				if err != nil {
					logBuffer.WriteString(log.Errorf("数据库 %s:%d,查询变量 %s 值时出错:%s",this.dbConn.hostIp,this.dbConn.port,variableName,err.Error()).Error())
					time.Sleep(time.Second*base.DEFAULT_SLEEP_SECOND)
				} else if maxLoadMet {
					logBuffer.WriteString(log.Errorf("数据库 %s:%d,DB负载满足条件: %s=%d, >=%d,Sleep:%d秒", this.dbConn.hostIp,this.dbConn.port,variableName, value, threshold,base.HIBERNATE_INTERVAL).Error())
					time.Sleep(time.Second*base.HIBERNATE_INTERVAL)
				} else {
					break
				}
			}
		}
	}
	return logBuffer.String()
}


func (this *singleArchiveData) maxLoadIsMet(maxLoad map[string]int64) (met bool, variableName string, value int64, threshold int64, err error) {
	for variableName, threshold = range maxLoad {
		value, err = db_mysql.ShowStatusVariable(this.dbConn.dbConn,variableName)
		if err != nil {
			return false, variableName, value, threshold, err
		}
		if value >= threshold {
			return true, variableName, value, threshold, nil
		}
	}
	return false, variableName, value, threshold, nil
}


func (this *singleArchiveData) checkSlaveDelay() string{
	logBuffer := bytes.Buffer{}
	if this.archiveDataContext.MaxSlaveDelay>0 && len(this.slaveDbs)>0 {
		for {
			isMeta, err :=  this.showDelayIsMet()
			if isMeta && err != nil {
				logBuffer.WriteString(err.Error())
				time.Sleep(time.Second*base.HIBERNATE_INTERVAL)
			} else {
				break
			}
		}
	}
	return logBuffer.String()
}


func (this *singleArchiveData) showDelayIsMet () (bool,error) {
	query:=" show slave status "
	for _,slaveDb:=range this.slaveDbs {
		sourceResult:=db_mysql.DBQueryOne(slaveDb.dbConn,query)
		if len(sourceResult)>0 {
			delay:=uint(util.ConvStrToFloat(sourceResult["Seconds_Behind_Master"]))
			log.Debugf("当前slave %s:%d 延迟:%d秒",slaveDb.hostIp,slaveDb.port,delay)
			if delay>this.archiveDataContext.MaxSlaveDelay {
				return true,log.Errorf("数据库 :%s:%d 实际延迟时间%d秒大于%d秒最大延迟时间",slaveDb.hostIp,slaveDb.port,delay,this.archiveDataContext.MaxSlaveDelay)
			}
		}
	}
	return false,nil
}
