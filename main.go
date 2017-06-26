package main

import (
	"database/sql"
	"fmt"

	"io/ioutil"

	"os"

	"regexp"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
)

//load conf
func loadConf(filename string) *map[string]string {
	bytes, err := ioutil.ReadFile(filename)
	checkErr(err)

	conf := make(map[string]string)

	json.Unmarshal(bytes, &conf)
	return &conf
}

func main() {

	start := time.Now()
	run()

	end := time.Now()
	fmt.Println("total time : ", timeFriendly(end.Sub(start).Seconds()))
}

func run() {

	//os.Exit(1)

	conf := *loadConf("conf.json")

	dsn := conf["dsn"]

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	checkErr(err)
	defer db.Close()

	//query := "select `title` ,`pid` ,`sort` ,`url` ,`hide` ,`group` ,`status` from yii2_menu where id>169"
	query := conf["query"]

	status := sqlBackup(db, query, "")
	fmt.Println(status)

}

//backup mysql to .sql
func sqlBackup(db *sql.DB, query string, fileNameExtra string) bool {

	table := getTableFromSql(query)

	// Execute the query
	rows, err := db.Query(query)

	checkErr(err)
	// Get column names
	columns, err := rows.Columns()
	//fmt.Println(columns)
	checkErr(err)
	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))
	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	ret := make([]map[string]string, 0)

	var columnString string
	for k, v := range columns {
		if k == 0 {
			columnString += "`" + v + "`"
		} else {
			columnString += " ,`" + v + "`"
		}

	}
	sqlStringAll := ""
	sqlString1 := "INSERT INTO " + table + " (" + columnString + ") VALUES "

	if !rows.Next() {
		return false
	}
	for rows.Next() {
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		checkErr(err)
		// Now do something with the data.
		// Here we just print each column as a string.
		var vsql string = "("
		var value string
		vmap := make(map[string]string, len(scanArgs))
		for i, col := range values {
			// Here we can check if the value is nil (NULL value)
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			vmap[columns[i]] = value
			value = strings.Replace(value, "'", "\\'", -1)
			length := len(values) - 1
			if value == "NULL" {
				vsql += "" + value
				if i == length {
					vsql += ""
				} else {
					vsql += ", "
				}

			} else {
				vsql += "'" + value
				if i == length {
					vsql += "'"
				} else {
					vsql += "', "
				}
			}

		}
		ret = append(ret, vmap)
		//fmt.Println("-----------------------------------")
		vsql = sqlString1 + vsql + ");\n"
		sqlStringAll += vsql

	}
	if err = rows.Err(); err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	backupFilePath := table
	if fileNameExtra == "" {
		backupFilePath += ".sql"
	} else {
		backupFilePath += "_" + fileNameExtra + ".sql"
	}

	newFile, err := os.Create(backupFilePath)
	checkErr(err)

	newFile.WriteString(sqlStringAll)

	defer newFile.Close()

	return true
}

//get table name from sql
func getTableFromSql(sql string) string {
	reg := regexp.MustCompile(` [^ ]*`)
	result := reg.FindAllString(sql, -1)
	var table string
	for k, v := range result {

		if strings.Index(v, "from") != -1 {

			table = strings.Trim(result[k+1], " ")
			fmt.Println("table : " + table)
		}
	}

	return table
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// time format fridnely
func timeFriendly(second float64) string {

	if second < 1 {
		return strconv.Itoa(int(second*1000)) + "毫秒"
	} else if second < 60 {
		return strconv.Itoa(int(second)) + "秒" + timeFriendly(second-float64(int(second)))
	} else if second >= 60 && second < 3600 {
		return strconv.Itoa(int(second/60)) + "分" + timeFriendly(second-float64(int(second/60)*60))
	} else if second >= 3600 && second < 3600*24 {
		return strconv.Itoa(int(second/3600)) + "小时" + timeFriendly(second-float64(int(second/3600)*3600))
	} else if second > 3600*24 {
		return strconv.Itoa(int(second/(3600*24))) + "天" + timeFriendly(second-float64(int(second/(3600*24))*(3600*24)))
	}
	return ""
}
