package util

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/smtp"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
)

const MEM_MAX_SIZE = (1 << 20) * 10

type loginAuth struct {
	username, password string
}

func LoginAuth(username, password string) smtp.Auth {
	return &loginAuth{username, password}
}

func (this *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	return "LOGIN", nil, nil
}

func (this *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	command := string(fromServer)
	command = strings.TrimSpace(command)
	command = strings.TrimSuffix(command, ":")
	command = strings.ToLower(command)
	if more {
		if (command == "username") {
			return []byte(fmt.Sprintf("%s", this.username)), nil
		} else if (command == "password") {
			return []byte(fmt.Sprintf("%s", this.password)), nil
		} else {
			// We've already sent everything.
			return nil, fmt.Errorf("unexpected server challenge: %s", command)
		}
	}
	return nil, nil
}



type Email struct {
	From        string
	To          string
	Subject     string
	Content     string
	ContentPath string
	Type        string
	Attachments string
}

//返回基础的头信息
func (e *Email) headers() (textproto.MIMEHeader, error) {
	res := make(textproto.MIMEHeader)
	if _, ok := res["To"]; !ok && len(e.To) > 0 {
		res.Set("To", e.To)
	}

	if _, ok := res["Subject"]; !ok && e.Subject != "" {
		res.Set("Subject", e.Subject)
	}

	if _, ok := res["From"]; !ok {
		res.Set("From", e.From)
	}
	return res, nil
}

//编码邮件内容
func (e *Email) writer(datawriter io.Writer) error {
	headers, err := e.headers()
	if err != nil {
		return err
	}
	w := multipart.NewWriter(datawriter)

	headers.Set("Content-Type", "multipart/mixed;\r\n boundary="+w.Boundary())
	headerToBytes(datawriter, headers)
	io.WriteString(datawriter, "\r\n")

	fmt.Fprintf(datawriter, "--%s\r\n", w.Boundary())
	header := textproto.MIMEHeader{}
	if e.Content != "" || e.ContentPath != "" {
		subWriter := multipart.NewWriter(datawriter)
		header.Set("Content-Type", fmt.Sprintf("multipart/alternative;\r\n boundary=%s\r\n", subWriter.Boundary()))
		headerToBytes(datawriter, header)
		if e.Content != "" {
			header.Set("Content-Type", fmt.Sprintf("text/%s; charset=UTF-8", e.Type))
			header.Set("Content-Transfer-Encoding", "quoted-printable")
			if _, err := subWriter.CreatePart(header); err != nil {
				return err
			}
			qp := quotedprintable.NewWriter(datawriter)
			if _, err := qp.Write([]byte(e.Content)); err != nil {
				return err
			}
			if err := qp.Close(); err != nil {
				return err
			}
		} else {
			header.Set("Content-Type", fmt.Sprintf("text/%s; charset=UTF-8", e.Type))
			header.Set("Content-Transfer-Encoding", "quoted-printable")
			if _, err := subWriter.CreatePart(header); err != nil {
				return err
			}
			qp := quotedprintable.NewWriter(datawriter)
			File, err := os.Open(e.ContentPath)
			if err != nil {
				return err
			}
			defer File.Close()

			_, err = io.Copy(qp, File)
			if err != nil {
				return err
			}
			if err := qp.Close(); err != nil {
				return err
			}
		}
		if err := subWriter.Close(); err != nil {
			return err
		}
	}
	if e.Attachments != "" {
		list := strings.Split(e.Attachments, ",")
		for _, path := range list {
			err = Attach(w, path)
			if err != nil {
				w.Close()
				return err
			}
		}
	}
	return nil
}

//查看一下发送的内容大小,如果过超过一定大小则,使用磁盘文件做临时
func (e *Email) len() (int64, error) {
	var l int64
	if e.Content != "" {
		l += int64(len(e.Content))
	} else {
		stat, err := os.Lstat(e.ContentPath)
		if err != nil {
			return 0, err
		}
		l += stat.Size()
	}
	if e.Attachments != "" {
		for _, path := range strings.Split(e.Attachments, ",") {
			stat, err := os.Lstat(path)
			if err != nil {
				return 0, err
			}
			l += stat.Size()
		}
	}
	return l, nil

}

//根据头信息创建附件
func headerToBytes(w io.Writer, header textproto.MIMEHeader) {
	for field, vals := range header {
		for _, subval := range vals {
			io.WriteString(w, field)
			io.WriteString(w, ": ")
			switch {
			case field == "Content-Type" || field == "Content-Disposition":
				w.Write([]byte(subval))
			default:
				w.Write([]byte(mime.QEncoding.Encode("UTF-8", subval)))
			}
			io.WriteString(w, "\r\n")
		}
	}
}

//编码成每行固定长度的base64消息
func base64Wrap(w io.Writer, r io.Reader) error {
	//const maxRaw = 57
	//const MaxLineLength = 76
	const maxRaw = 129
	const MaxLineLength = 256

	buffer := make([]byte, MaxLineLength+len("\r\n"))
	copy(buffer[MaxLineLength:], "\r\n")
	var b = make([]byte, maxRaw)
	for {
		n, err := r.Read(b)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n == maxRaw {
			base64.StdEncoding.Encode(buffer, b[:n])
			w.Write(buffer)
			//fmt.Println(string(b[:n])+"####")
		} else {
			out := buffer[:base64.StdEncoding.EncodedLen(n)]
			base64.StdEncoding.Encode(out, b[:n])
			out = append(out, "\r\n"...)
			w.Write(out)
			//fmt.Println(string(b[:n])+"####")
		}
	}
}




//发送消息
func send(msg Email, addr string, auth smtp.Auth, body io.Reader) error {
	to := strings.Split(msg.To, ",")
	if msg.From == "" || len(to) == 0 {
		return errors.New("Must specify at least one From address and one To address")
	}
	client, err := smtp.Dial(addr)
	if err != nil {
		return err
	}
	defer client.Close()
	host := strings.Split(addr, ":")[0]
	if err = client.Hello(host); err != nil {
		return err
	}
	if ok, _ := client.Extension("STARTTLS"); ok {
		config := &tls.Config{ServerName: host}
		if err = client.StartTLS(config); err != nil {
			return err
		}
	}
	if err = client.Auth(auth); err != nil {
		return err
	}
	if err = client.Mail(msg.From); err != nil {
		return err
	}
	for _, addr := range to {
		if err = client.Rcpt(addr); err != nil {
			return err
		}
	}
	w, err := client.Data()
	if err != nil {
		return err
	}
	if value, ok := body.(io.Seeker); ok {
		value.Seek(0, 0)
	}
	_, err = io.Copy(w, body)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	return client.Quit()
}

//添加附件
func Attach(w *multipart.Writer, filename string) (err error) {
	typ := mime.TypeByExtension(filepath.Ext(filename))
	var Header = make(textproto.MIMEHeader)
	if typ != "" {
		Header.Set("Content-Type", typ)
	} else {
		Header.Set("Content-Type", "application/octet-stream")
	}
	basename := filepath.Base(filename)
	Header.Set("Content-Disposition", fmt.Sprintf("attachment;\r\n filename=\"%s\"", basename))
	Header.Set("Content-ID", fmt.Sprintf("<%s>", basename))
	Header.Set("Content-Transfer-Encoding", "base64")
	File, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer File.Close()

	mw, err := w.CreatePart(Header)
	if err != nil {
		return err
	}
	return base64Wrap(mw, File)
}

func SendMail(msg Email) {
	size, err := msg.len()
	if err != nil {
		fmt.Println(err)
		return
	}
	var file io.ReadWriter
	if size >= MEM_MAX_SIZE {
		temp := fmt.Sprintf(".%d.tmp", os.Getpid())
		file, err = os.Create(temp)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer os.Remove(temp)
	} else {
		file = bytes.NewBuffer(make([]byte, 0, size))
	}
	msg.writer(file)
	password:="b065c40178b61b45039a9e14ad949b1b7db3e36cfe275756249cd97dad9314029e298028f1ee1ba4854a1bef5ae71f5f"
	password,err=Decrypt(password)
	auth := LoginAuth("db@ele.me", password)

	err = send(msg, "email.ele.me:25", auth, file)
	if err != nil {
		fmt.Println(err)
	}
	if c, ok := file.(io.Closer); ok {
		c.Close()
	}
}