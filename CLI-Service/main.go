package main

import (
	"archive/zip"
	"cli-service/model"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
)

func main() {

	os.Mkdir("sql", 0777)
	os.Mkdir("zip", 0777)

	listDatabases := loadListDB()

	countGorotine := 5

	// Pipeline 1 DumpDatabases
	var dumpDbChanTemp []<-chan string
	for i := 0; i < countGorotine; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, dumpDB(listDatabases))
	}
	dumpDbChan := mergedChan(dumpDbChanTemp...)

	// Pipeline 2 archiveSqlToZip
	var zipChanTemp []<-chan string
	for i := 0; i < countGorotine; i++ {
		zipChanTemp = append(zipChanTemp, zipDB(dumpDbChan))
	}
	archiveZipChan := mergedChan(zipChanTemp...)

	for value := range archiveZipChan {
		fmt.Println(value)
	}
}

func loadListDB() <-chan model.Database {
	dbChan := make(chan model.Database)
	var listDatabases []model.Database
	dataJson, err := os.ReadFile("config/databases.json")
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
		defer close(dbChan)
		for v := range listDB {
			timesTamp := time.Now().Format("2006-01-02-15-04-05")
			uuid := uuid.New().String()
			nameFile := fmt.Sprintf("sql/mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
			file, err := os.Create(nameFile)
			if err != nil {
				fmt.Printf("Error creating file %s, Error: %s\n", nameFile, err)
				return
			}
			defer file.Close()

			cmd := exec.Command("mysqldump", "-h", v.DBHost, "-P", v.DBPort, "-u", v.DBUsername, "-p"+v.DBPassword, v.DatabaseName)
			cmd.Stdout = file

			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error running mysqldump %s, Error: %s\n", nameFile, err)
				os.Remove(nameFile)
				return
			}

			dbChan <- fmt.Sprintf("mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
		}
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
				fmt.Printf("Error creating zip file %s, Error : %s\n", fileNameZip, err)
				continue
			}

			zipWriter := zip.NewWriter(archive)

			f, err := os.Open(fileNameSql)
			if err != nil {
				fmt.Printf("Error opening sql file %s, Error : %s\n", fileName, err)
				continue
			}

			w, err := zipWriter.Create(fileName)
			if err != nil {
				fmt.Printf("Error creating file %s in zip, Error : %s\n", fileName, err)
				continue
			}

			if _, err := io.Copy(w, f); err != nil {
				fmt.Printf("Error copying file %s to zip , Error : %s\n", fileName, err)
				continue
			}

			archive.Close()
			zipWriter.Close()
			f.Close()

			zipChan <- fileNameZip
		}
	}()

	return zipChan
}

func mergedChan(chanMany ...<-chan string) <-chan string {
	wg := sync.WaitGroup{}

	mergedChan := make(chan string)

	go func() {
		wg.Wait()
		close(mergedChan)
	}()

	wg.Add(len(chanMany))
	for _, eachChan := range chanMany {
		go func(eachChan <-chan string) {
			for eachChanData := range eachChan {
				mergedChan <- eachChanData
			}
			wg.Done()
		}(eachChan)
	}

	return mergedChan
}
