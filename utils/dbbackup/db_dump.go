package dbbackup

import (
	"cli-service/model"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/google/uuid"
)

func dumpDatabase(pathFile *model.PathFile, db model.Database) (model.NameFile, string, error) {
	timesTamp := time.Now().Format("2006-01-02-15-04-05")
	uuid := uuid.New().String()
	nameFileSql := fmt.Sprintf("mysql-%s-%s-%s.sql", timesTamp, db.DatabaseName, uuid)
	pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFileSql)
	mErr := ""

	file, err := os.Create(pathNameFileSql)
	if err != nil {
		mErr = fmt.Sprintf("Error creating file %s, Error: %s\n", pathNameFileSql, err.Error())
		return model.NameFile{NameFileSql: nameFileSql}, mErr, err
	}
	defer file.Close()

	cmd := exec.Command("mysqldump", "-h", db.DBHost, "-P", db.DBPort, "-u", db.DBUsername, "-p"+db.DBPassword, db.DatabaseName)
	cmd.Stdout = file

	err = cmd.Run()
	if err != nil {
		mErr = fmt.Sprintf("Error running mysqldump %s, Error: %s\n", pathNameFileSql, err.Error())
		return model.NameFile{NameFileSql: nameFileSql}, mErr, err
	}

	return model.NameFile{NameFileSql: nameFileSql, NameDatabaseFile: db.DatabaseName}, mErr, nil
}
