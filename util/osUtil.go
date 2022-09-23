package util

import (
	"os"
	"github.com/outbrain/golib/log"
	"time"
	"bufio"
	"os/exec"
	"bytes"
	"io"
)

const (
	TIME_OUT=30
)

func GetHostname() string {
	HostName, err := os.Hostname()
	if err != nil {
		log.Fatal("Get host name error", err.Error())
	}
	return HostName
}


func Timeout() {
	time.AfterFunc(TIME_OUT*time.Second, func() {
		log.Error("执行时间超时")
		os.Exit(1)
	})
}

func FileExists(fileName string) bool {
	if _, err := os.Stat(fileName); err == nil {
		return true
	}
	return false
}


func DeleteFile(fileName string) error{
	return  os.Remove(fileName)
}

func MakeDateDir(parentPath string) string {
	timeNow := time.Now()
	dateTime:=timeNow.Format("2006-01-02")
	dirPath:=parentPath+"/"+dateTime
	pathExists:=PathExists(dirPath)
	if !pathExists {
		err:=os.Mkdir(dirPath,0766)
		if err!=nil {
			log.Errorf("创建目录失败,err:%s",err.Error())
			return parentPath
		}
	}
	return dirPath
}

func MakeDir(parentPath string) error {
	pathExists:=PathExists(parentPath)
	if !pathExists {
		err:=os.MkdirAll(parentPath,0766)
		if err!=nil {
			return log.Errorf("创建目录失败,err:%s",err.Error())
		}
	}
	return nil
}

func PathExists(path string) (bool) {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

func GetFileSize(file string) int64 {
	fileInfo, err := os.Stat(file)
	if err != nil {
		log.Error("文件不存在:%s",file)
		return 0
	}
	fileSize := fileInfo.Size()
	return fileSize
}

func FileLine(path string)(int){
	f,err := os.Open(path)
	if nil != err{
		return 0
	}
	defer f.Close()
	count:=0
	scanner := bufio.NewScanner(f)
	for scanner.Scan(){
		count += 1
	}
	return count
}

//阻塞式的执行外部shell命令的函数,等待执行完毕并返回标准输出
func ExecBlockShell(commandName string) (return_stdout string,sreturn_stderr string, return_err error){
	cmd := exec.Command("/bin/bash", "-c", commandName)
	log.Debugf("shell: %v",cmd.Args)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(),stderr.String(),err
}

func ExecBlockShellForMap(commandName string) (map[string]string,error){
	stdout,_,err:=ExecBlockShell(commandName)
	return CollectStringToMap(stdout),err
}

//非阻塞式的执行外部shell命令的函数,不等待执行完
func ExecNoBlockShell(commandName string) bool {
	cmd := exec.Command("/bin/bash", "-c", commandName)
	log.Debugf("shell: %v",cmd.Args)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Errorf("shell error:%s",err.Error())
		return false
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Errorf("shell error:%s",err.Error())
		return false
	}

	cmd.Start()
	go func() {
		stdoutReader := bufio.NewReader(stdout)
		for {
			line, err2 := stdoutReader.ReadString('\n')
			if err2 != nil || io.EOF == err2 {
				break
			}
			log.Infof("shell output:%s", line)
		}
	}()
	go func() {
		stderrReader := bufio.NewReader(stderr)
		for {
			line, err2 := stderrReader.ReadString('\n')
			if err2 != nil || io.EOF == err2 {
				break
			}
			log.Infof("shell stderr:%s", line)
		}
	}()

	cmd.Wait()
	return true
}

