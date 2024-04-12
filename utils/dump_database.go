package utils

import (
	"archive/zip"
	"bytes"
	"cli-service/model"
	"cli-service/utils/logger"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
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

type NameFile struct {
	NameFileSql string
	NameFileZip string
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
	var dumpDbChanTemp []<-chan NameFile
	for i := 0; i < countGorotine; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, proccessDumpDB(listDatabases, &pathFile))
	}
	dumpDbChan := proccessMergeChan(dumpDbChanTemp...)

	// Pipeline 2 archiveSqlToZip
	var zipChanTemp []<-chan NameFile
	for i := 0; i < countGorotine; i++ {
		zipChanTemp = append(zipChanTemp, proccessZipDB(dumpDbChan, &pathFile))
	}
	archiveZipChan := proccessMergeChan(zipChanTemp...)

	// Pipeline 3 UploadFileToService
	var uploadChanTemp []<-chan NameFile
	for i := 0; i < countGorotine; i++ {
		uploadChanTemp = append(uploadChanTemp, proccessUploadFile(archiveZipChan, &pathFile))
	}
	uploadFileChan := proccessMergeChan(uploadChanTemp...)

	// Hapus file temporay sql dan zip
	for value := range uploadFileChan {
		pathFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, value.NameFileSql)
		pathFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, value.NameFileZip)
		os.Remove(pathFileSql)
		os.Remove(pathFileZip)

		mErr := fmt.Sprintf("Database: %s Backup Success \n", value.NameFileZip)
		logger.Info(mErr)
	}
}

// fungsi untuk memanggil list database yg sudah telah terdaftar
func getListDB(pathFile *PathFile) <-chan model.Database {
	listDatabases := []model.Database{}
	dbChan := make(chan model.Database)

	dataJson, err := os.ReadFile(pathFile.PathDBJson)
	if err != nil {
		fmt.Println(err)
		logger.Error(err)
		panic(err)
	}

	err = json.Unmarshal(dataJson, &listDatabases)
	if err != nil {
		fmt.Println(err)
		logger.Error(err)
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
func proccessDumpDB(listDB <-chan model.Database, pathFile *PathFile) <-chan NameFile {
	dbChan := make(chan NameFile)

	go func() {
		defer close(dbChan)
		for v := range listDB {

			timesTamp := time.Now().Format("2006-01-02-15-04-05")
			uuid := uuid.New().String()
			nameFileSql := fmt.Sprintf("mysql-%s-%s-%s.sql", timesTamp, v.DatabaseName, uuid)
			pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFileSql)

			file, err := os.Create(pathNameFileSql)
			if err != nil {
				mErr := fmt.Sprintf("Error creating file %s, Error: %s\n", pathNameFileSql, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			cmd := exec.Command("mysqldump", "-h", v.DBHost, "-P", v.DBPort, "-u", v.DBUsername, "-p"+v.DBPassword, v.DatabaseName)
			cmd.Stdout = file

			err = cmd.Run()
			if err != nil {
				mErr := fmt.Sprintf("Error running mysqldump %s, Error: %s\n", pathNameFileSql, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				os.Remove(pathNameFileSql)
				return
			}

			file.Close()

			dbChan <- NameFile{
				NameFileSql: nameFileSql,
				NameFileZip: "",
			}
		}
	}()

	return dbChan
}

// fungsi untuk proses Zip dari database yg di dump sebelumnya
func proccessZipDB(fileNameCh <-chan NameFile, pathFile *PathFile) <-chan NameFile {
	zipChan := make(chan NameFile)
	go func() {
		defer close(zipChan)
		for fileName := range fileNameCh {

			pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, fileName.NameFileSql)
			nameFileZip := fmt.Sprintf("%s.zip", fileName.NameFileSql)
			pathNameFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFileZip)

			archive, err := os.Create(pathNameFileZip)
			if err != nil {
				mErr := fmt.Sprintf("Error creating zip file %s, Error : %s\n", pathNameFileZip, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			zipWriter := zip.NewWriter(archive)

			f, err := os.Open(pathNameFileSql)
			if err != nil {
				mErr := fmt.Sprintf("Error opening sql file %s, Error : %s\n", fileName.NameFileSql, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			w, err := zipWriter.Create(fileName.NameFileSql)
			if err != nil {
				mErr := fmt.Sprintf("Error creating file %s in zip, Error : %s\n", fileName.NameFileSql, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			if _, err := io.Copy(w, f); err != nil {
				mErr := fmt.Sprintf("Error copying file %s to zip , Error : %s\n", fileName.NameFileSql, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			archive.Close()
			zipWriter.Close()
			f.Close()

			zipChan <- NameFile{
				NameFileSql: fileName.NameFileSql,
				NameFileZip: nameFileZip,
			}
		}
	}()

	return zipChan
}

// fungsi untuk proses upload file ke service
func proccessUploadFile(fileNameCh <-chan NameFile, pathFile *PathFile) <-chan NameFile {
	uploadChan := make(chan NameFile)
	go func() {
		defer close(uploadChan)
		for fileName := range fileNameCh {

			serviceURL := os.Getenv("WEB_SERVICE_URL")
			uploadURL := fmt.Sprintf("%s/bckp-database/%s", serviceURL, fileName.NameFileZip)
			pathNameFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, fileName.NameFileZip)

			file, err := os.Open(pathNameFileZip)
			if err != nil {
				mErr := fmt.Sprintf("Error open file %s to zip , Error : %s\n", fileName.NameFileZip, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			var requestBody bytes.Buffer
			writer := multipart.NewWriter(&requestBody)

			fileField, err := writer.CreateFormFile("zip_file", fileName.NameFileZip)
			if err != nil {
				mErr := fmt.Sprintf("Error create form zip_file %s , Error : %s\n", fileName.NameFileZip, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			_, err = io.Copy(fileField, file)
			if err != nil {
				mErr := fmt.Sprintf("Error copying file %s , Error : %s\n", fileName.NameFileZip, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}
			writer.Close()

			req, err := http.NewRequest("POST", uploadURL, &requestBody)
			if err != nil {
				mErr := fmt.Sprintf("Error new request %s , Error : %s\n", uploadURL, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}
			req.Header.Set("Content-Type", writer.FormDataContentType())

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				mErr := fmt.Sprintf("Error send request %s, Error : %s\n", uploadURL, err)
				fmt.Println(mErr)
				logger.Error(mErr)
				return
			}

			resp.Body.Close()
			file.Close()

			uploadChan <- NameFile{
				NameFileSql: fileName.NameFileSql,
				NameFileZip: fileName.NameFileZip,
			}
		}
	}()

	return uploadChan
}

// fungsi untuk proses mengabungkan channel
func proccessMergeChan(chanMany ...<-chan NameFile) <-chan NameFile {
	wg := sync.WaitGroup{}

	mergedChan := make(chan NameFile)

	wg.Add(len(chanMany))
	for _, eachChan := range chanMany {
		go func(eachChan <-chan NameFile) {
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
