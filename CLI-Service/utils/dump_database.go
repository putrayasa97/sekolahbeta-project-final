package utils

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

type PathFile struct {
	PathDBJson  string
	PathFileSql string
	PathFileZip string
}

// Proses DumpDatabase
// TODO 1. Memanggil list databases dari file json
// TODO 2. Proses dump databases
// TODO 3. Proses zip file sql yaang telah di dump
// TODO 4. Proses Upload file ke webservice
// TODO 5. Hapus file temporary sql dan zip ketik selesai proses upload
// Proses ini menggunakan konsep Concurency: Pipeline Pattern

func DumpDatabase() {
	pathFile := PathFile{
		PathDBJson:  "config/databases.json",
		PathFileSql: "temp/sql",
		PathFileZip: "temp/zip",
	}

	os.MkdirAll(pathFile.PathFileSql, 0777)
	os.MkdirAll(pathFile.PathFileZip, 0777)

	listDatabases := getListDB(&pathFile)
	countGorotine := 5

	// Pipeline 1 DumpDatabases
	var dumpDbChanTemp []<-chan string
	for i := 0; i < countGorotine; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, proccessDumpDB(listDatabases, &pathFile))
	}
	dumpDbChan := proccessMergeChan(dumpDbChanTemp...)

	// Pipeline 2 archiveSqlToZip
	var zipChanTemp []<-chan string
	for i := 0; i < countGorotine; i++ {
		zipChanTemp = append(zipChanTemp, proccessZipDB(dumpDbChan, &pathFile))
	}
	archiveZipChan := proccessMergeChan(zipChanTemp...)

	for value := range archiveZipChan {
		fmt.Println(value)
	}
}

// fungsi untuk memanggil list database yg sudah telah terdaftar
func getListDB(pathFile *PathFile) <-chan model.Database {
	listDatabases := []model.Database{}
	dbChan := make(chan model.Database)

	dataJson, err := os.ReadFile(pathFile.PathDBJson)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	err = json.Unmarshal(dataJson, &listDatabases)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	go func() {
		for _, db := range listDatabases {
			dbChan <- db
		}
		close(dbChan)
	}()

	return dbChan
}

// fungsi untuk proses Dump Databases
func proccessDumpDB(listDB <-chan model.Database, pathFile *PathFile) <-chan string {
	dbChan := make(chan string)

	go func() {
		defer close(dbChan)
		for v := range listDB {

			timesTamp := time.Now().Format("2006-01-02-15-04-05")
			uuid := uuid.New().String()
			nameFile := fmt.Sprintf("mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
			pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFile)

			file, err := os.Create(pathNameFileSql)
			if err != nil {
				fmt.Printf("Error creating file %s, Error: %s\n", pathNameFileSql, err)
				return
			}
			defer file.Close()

			cmd := exec.Command("mysqldump", "-h", v.DBHost, "-P", v.DBPort, "-u", v.DBUsername, "-p"+v.DBPassword, v.DatabaseName)
			cmd.Stdout = file

			err = cmd.Run()
			if err != nil {
				fmt.Printf("Error running mysqldump %s, Error: %s\n", pathNameFileSql, err)
				os.Remove(pathNameFileSql)
				return
			}

			dbChan <- nameFile
		}
	}()

	return dbChan
}

// fungsi untuk proses Zip dari database yg di dump sebelumnya
func proccessZipDB(fileNameCh <-chan string, pathFile *PathFile) <-chan string {
	zipChan := make(chan string)
	go func() {
		defer close(zipChan)
		for fileName := range fileNameCh {

			pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, fileName)
			pathNameFileZip := fmt.Sprintf("%s/%s.zip", pathFile.PathFileZip, fileName)

			archive, err := os.Create(pathNameFileZip)
			if err != nil {
				fmt.Printf("Error creating zip file %s, Error : %s\n", pathNameFileZip, err)
				return
			}

			zipWriter := zip.NewWriter(archive)

			f, err := os.Open(pathNameFileSql)
			if err != nil {
				fmt.Printf("Error opening sql file %s, Error : %s\n", fileName, err)
				return
			}

			w, err := zipWriter.Create(fileName)
			if err != nil {
				fmt.Printf("Error creating file %s in zip, Error : %s\n", fileName, err)
				return
			}

			if _, err := io.Copy(w, f); err != nil {
				fmt.Printf("Error copying file %s to zip , Error : %s\n", fileName, err)
				return
			}

			archive.Close()
			zipWriter.Close()
			f.Close()

			zipChan <- pathNameFileZip
		}
	}()

	return zipChan
}

// fungsi untuk proses mengabungkan channel
func proccessMergeChan(chanMany ...<-chan string) <-chan string {
	wg := sync.WaitGroup{}

	mergedChan := make(chan string)

	wg.Add(len(chanMany))
	for _, eachChan := range chanMany {
		go func(eachChan <-chan string) {
			for eachChanData := range eachChan {
				mergedChan <- eachChanData
			}
			wg.Done()
		}(eachChan)
	}

	go func() {
		wg.Wait()
		close(mergedChan)
	}()

	return mergedChan
}

// fungsi untuk proses hapus file temporary sql & zip
// func proccessRemoveFileTemp() {

// }
