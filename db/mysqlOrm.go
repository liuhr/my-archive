package db

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type KV map[string]interface{}

type ORM struct {
	DB    *sql.DB
	Table string
	*KV
	Where string
}
func (self *ORM) Insert() (sql.Result, error) {
	split := make([]string, 0)
	columns := make([]string, 0)
	values := make([]interface{}, 0)
	for col, _ := range *self.KV {
		split = append(split, "?")
		columns = append(columns, col)
	}
	//sort map generate a fixed too much type of sql
	sort.Strings(columns)
	for _, v := range columns {
		values = append(values, (*self.KV)[v])
	}
	s := fmt.Sprint("insert into ", self.Table, "(", strings.Join(columns, ","), ")values(",
		strings.Join(split, ","), ")")
	return self.DB.Exec(s, values...)
}
func (self *ORM) Replace() (sql.Result, error) {
	split := make([]string, 0)
	columns := make([]string, 0)
	values := make([]interface{}, 0)
	for col, _ := range *self.KV {
		split = append(split, "?")
		columns = append(columns, col)
	}
	//sort map generate a fixed too much type of sql
	sort.Strings(columns)
	for _, v := range columns {
		values = append(values, (*self.KV)[v])
	}
	s := fmt.Sprint("replace into ", self.Table, "(", strings.Join(columns, ","), ")values(",
		strings.Join(split, ","), ")")
	return self.DB.Exec(s, values...)
}
func (self *ORM) Update() (sql.Result, error) {
	columns := make([]string, 0)
	values := make([]interface{}, 0)
	keys := make([]string, 0)
	for k, _ := range *self.KV {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		columns = append(columns, fmt.Sprintln(k, "=?"))
		values = append(values, (*self.KV)[k])
	}
	s := fmt.Sprint("update ", self.Table, " set ", strings.Join(columns, ","), " where ", self.Where)
	return self.DB.Exec(s, values...)
}
func (self *ORM) Delete() (sql.Result, error) {
	s := fmt.Sprint("delete from ", self.Table, " where ", self.Where)
	return self.DB.Exec(s)
}
