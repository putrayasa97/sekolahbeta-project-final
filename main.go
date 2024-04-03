package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sekolahbeta-project-final/model"
	"sync"
	"time"

	"github.com/google/uuid"
)

func main() {

	os.Mkdir("sql", 0777)
	os.Mkdir("zip", 0777)

	listDatabases := loadListDB()

	jmlGoroutine := 10

	var dumpDbChanTemp []<-chan string

	for i := 0; i < jmlGoroutine; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, dumpDB(listDatabases))
	}

	dumpDbChan := appendChan(dumpDbChanTemp...)

	var zipChanTemp []<-chan string

	for i := 0; i < jmlGoroutine; i++ {
		zipChanTemp = append(zipChanTemp, zipDB(dumpDbChan))
	}

	dumpZipChan := appendChan(zipChanTemp...)

	for value := range dumpZipChan {
		fmt.Println(value)
	}
}

func loadListDB() <-chan model.Database {
	dbChan := make(chan model.Database)
	var listDatabases []model.Database
	dataJson, err := os.ReadFile("databases.json")
	if err != nil {
		fmt.Println(err)
	}

	err = json.Unmarshal(dataJson, &listDatabases)
	if err != nil {
		fmt.Println(err)
	}

	go func() {
		for _, db := range listDatabases {
			dbChan <- db
		}
		close(dbChan)
	}()

	return dbChan
}

func dumpDB(listDB <-chan model.Database) <-chan string {
	dbChan := make(chan string)

	go func() {
		for v := range listDB {
			timesTamp := time.Now().Format("2006-01-02-15-04-05")
			uuid := uuid.New().String()
			nameFile := fmt.Sprintf("sql/mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
			file, err := os.Create(nameFile)
			if err != nil {
				panic(err)
			}
			password := fmt.Sprintf("-p%s", v.DBPassword)
			cmd := exec.Command("mysqldump", "-h", v.DBHost, "-P", v.DBPort, "-u", v.DBUsername, password, v.DatabaseName)
			cmd.Stdout = file

			err = cmd.Run()
			if err != nil {
				panic(err)
			}

			dbChan <- fmt.Sprintf("mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
		}

		close(dbChan)
	}()

	return dbChan
}

func zipDB(fileNameCh <-chan string) <-chan string {
	zipChan := make(chan string)
	go func() {
		defer close(zipChan)
		for fileName := range fileNameCh {
			fileNameSql := fmt.Sprintf("sql/%s", fileName)
			fileNameZip := fmt.Sprintf("zip/%s.zip", fileName)
			archive, err := os.Create(fileNameZip)
			if err != nil {
				panic(err)
			}

			zipWriter := zip.NewWriter(archive)

			f, err := os.Open(fileNameSql)
			if err != nil {
				panic(err)
			}

			w, err := zipWriter.Create(fileName)
			if err != nil {
				panic(err)
			}
			if _, err := io.Copy(w, f); err != nil {
				panic(err)
			}

			archive.Close()
			zipWriter.Close()
			f.Close()

			zipChan <- fileNameZip
		}
	}()

	return zipChan
}

func appendChan(chanMany ...<-chan string) <-chan string {
	wg := sync.WaitGroup{}

	mergedChan := make(chan string)

	wg.Add(len(chanMany))
	for _, ch := range chanMany {
		go func(ch <-chan string) {
			for file := range ch {
				mergedChan <- file
			}
			wg.Done()
		}(ch)
	}

	go func() {
		wg.Wait()
		close(mergedChan)
	}()

	return mergedChan
}
