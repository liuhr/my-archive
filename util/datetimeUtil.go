package util

import (
	"github.com/outbrain/golib/log"
	"time"
	"fmt"
)

func ConvStrToIime(s string) time.Time{
	time, err :=time.ParseInLocation("2006-01-02 15:04:05", s,time.Local)
	if err !=nil {
		log.Errorf("ConvStrToIime error %s:%s",s,err.Error())
	}
	return time
}

func NowTime() string {
	time := time.Now()
	return  time.Format("15:04:05")
}

func NowDate() string {
	time := time.Now()
	return  time.Format("2006-01-02")
}

func NowDatetime() string {
	time := time.Now()
	return  time.Format("2006-01-02 15:04:05")
}

func NowTimestamp() string {
	time := time.Now()
	return  time.Format("2006-01-02 15:04:05.999999999")
}

func GetDate(time time.Time) string{
	return  time.Format("2006-01-02")
}

func GetDatetime(time time.Time) string{
	return  time.Format("2006-01-02 15:04:05")
}

func GetTime(time time.Time) string{
	return  time.Format("15:04:05")
}

func GetTimestamp(time time.Time) string {
	return time.Format("2006-01-02 15:04:05.999999999")
}

func ConvUnixToTimestamp(unix int64) string {
	tm := time.Unix(unix, 0)
	return tm.Format("2006-01-02 15:04:05.999999999")
}


func TimeInterval(startTime time.Time,endTime time.Time) int{
	return int(endTime.Sub(startTime).Seconds())
}

func StringTimeInterval(startTime string,endTime string) int{
	return int(ConvStrToIime(endTime).Sub(ConvStrToIime(startTime)).Seconds())
}


func StringTimeAddSecond(startTime string,addSecond int) string{
	beginTime:=ConvStrToIime(startTime)
	endTime:=beginTime.Add(time.Duration(addSecond)*time.Second)
	return GetDatetime(endTime)
}



func SubDayDatetime(intervalDay int) time.Time {
	nowTime:=time.Now()
	sub,_:=time.ParseDuration(fmt.Sprintf("-%dh",intervalDay*24))
	return  nowTime.Add(sub)
}

func SubSecondDatetime(intervalSecond int) time.Time {
	nowTime:=time.Now()
	endTime:=nowTime.Add(time.Duration(-intervalSecond)*time.Second)
	return endTime
}

func SubSecondDatetimeForString(intervalSecond int) string {
	return GetDatetime(SubSecondDatetime(intervalSecond))
}