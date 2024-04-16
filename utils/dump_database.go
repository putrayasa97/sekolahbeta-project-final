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
	"path/filepath"
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

	logger.Info(fmt.Sprintln("Mulai Proses Backup Databases ..."))

	listDatabases := getListDB(&pathFile)
	// countGorotine := 4

	// Pipeline 1 DumpDatabases
	var dumpDbChanTemp []<-chan NameFile
	for i := 0; i < 3; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, proccessDumpDB(listDatabases, &pathFile))
	}
	dumpDbChan := proccessMergeChan(dumpDbChanTemp...)

	// Pipeline 2 archiveSqlToZip
	var zipChanTemp []<-chan NameFile
	for i := 0; i < 6; i++ {
		zipChanTemp = append(zipChanTemp, proccessZipDB(dumpDbChan, &pathFile))
	}
	archiveZipChan := proccessMergeChan(zipChanTemp...)

	// Pipeline 3 UploadFileToService
	var uploadChanTemp []<-chan NameFile
	for i := 0; i < 3; i++ {
		uploadChanTemp = append(uploadChanTemp, proccessUploadFile(archiveZipChan, &pathFile))
	}
	uploadFileChan := proccessMergeChan(uploadChanTemp...)

	// Hapus file temporay sql dan zip
	for nameFile := range uploadFileChan {
		removeFileTemp(&pathFile, nameFile)

		mErr := fmt.Sprintf("Database: %s Backup Success \n", nameFile.NameFileZip)
		logger.Info(mErr)
	}

	logger.Info(fmt.Sprintln("Proses Backup Database Berakhir !!"))
}

// fungsi untuk memanggil list database yg sudah telah terdaftar
func getListDB(pathFile *PathFile) <-chan model.Database {
	listDatabases := []model.Database{}
	dbChan := make(chan model.Database)

	dataJson, err := os.ReadFile(pathFile.PathDBJson)
	if err != nil {
		logger.Error(fmt.Sprintln(err))
		return nil
	}

	err = json.Unmarshal(dataJson, &listDatabases)
	if err != nil {
		logger.Error(fmt.Sprintln(err))
		return nil
	}

	if len(listDatabases) == 0 {
		mErr := fmt.Sprintln("Tidak ada database yang diproses")
		logger.Error(mErr)
		return nil
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
				logger.Error(mErr)
				return
			}
			defer file.Close()

			cmd := exec.Command("mysqldump", "-h", v.DBHost, "-P", v.DBPort, "-u", v.DBUsername, "-p"+v.DBPassword, v.DatabaseName)
			cmd.Stdout = file

			err = cmd.Run()
			if err != nil {
				mErr := fmt.Sprintf("Error running mysqldump %s, Error: %s\n", pathNameFileSql, err)
				logger.Error(mErr)
				os.Remove(pathNameFileSql)
				return
			}

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
				logger.Error(mErr)
				return
			}
			defer archive.Close()

			zipWriter := zip.NewWriter(archive)

			f, err := os.Open(pathNameFileSql)
			if err != nil {
				mErr := fmt.Sprintf("Error opening sql file %s, Error : %s\n", fileName.NameFileSql, err)
				logger.Error(mErr)
				return
			}
			defer f.Close()

			w, err := zipWriter.Create(fileName.NameFileSql)
			if err != nil {
				mErr := fmt.Sprintf("Error creating file %s in zip, Error : %s\n", fileName.NameFileSql, err)
				logger.Error(mErr)
				return
			}

			if _, err := io.Copy(w, f); err != nil {
				mErr := fmt.Sprintf("Error copying file %s to zip , Error : %s\n", fileName.NameFileSql, err)
				logger.Error(mErr)
				return
			}
			zipWriter.Close()

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
			bearerToken := "Bearer " + os.Getenv("WEB_SERIVCE_STATIC_KEY")
			uploadURL := fmt.Sprintf("%s/bckp-database/%s", serviceURL, fileName.NameFileZip)
			pathNameFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, fileName.NameFileZip)

			uploadChan <- NameFile{
				NameFileSql: fileName.NameFileSql,
				NameFileZip: fileName.NameFileZip,
			}

			file, err := os.Open(pathNameFileZip)
			if err != nil {
				mErr := fmt.Sprintf("Error open file %s to zip , Error : %s\n", fileName.NameFileZip, err)
				logger.Error(mErr)
				return
			}
			defer file.Close()

			requestBody := &bytes.Buffer{}
			writer := multipart.NewWriter(requestBody)

			fileField, err := writer.CreateFormFile("zip_file", filepath.Base(fileName.NameFileZip))
			if err != nil {
				mErr := fmt.Sprintf("Error create form zip_file %s , Error : %s\n", fileName.NameFileZip, err)
				logger.Error(mErr)
				return
			}

			_, err = io.Copy(fileField, file)
			if err != nil {
				mErr := fmt.Sprintf("Error copying file %s , Error : %s\n", fileName.NameFileZip, err)
				logger.Error(mErr)
				return
			}

			err = writer.Close()
			if err != nil {
				mErr := fmt.Sprintf("Error writer close zip %s, Error : %s\n", fileName.NameFileZip, err)
				logger.Error(mErr)
				return
			}

			req, err := http.NewRequest("POST", uploadURL, requestBody)
			if err != nil {
				mErr := fmt.Sprintf("Error new request %s , Error : %s\n", uploadURL, err)
				logger.Error(mErr)
				return
			}

			req.Header.Set("Content-Type", writer.FormDataContentType())
			req.Header.Add("Authorization", bearerToken)
			client := &http.Client{}

			resp, err := client.Do(req)

			if err != nil {
				mErr := fmt.Sprintf("Error send request %s, Error : %s\n", uploadURL, err)
				logger.Error(mErr)
				return
			}

			if resp.StatusCode != http.StatusCreated {
				body, _ := io.ReadAll(resp.Body)
				mErr := fmt.Sprintf("Error send request %s, Error : %s\n", uploadURL, string(body))
				logger.Info(mErr)
				return
			}

			defer resp.Body.Close()
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

func removeFileTemp(pathFile *PathFile, nameFile NameFile) {
	pathFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFile.NameFileSql)
	pathFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFile.NameFileZip)
	os.Remove(pathFileZip)
	os.Remove(pathFileSql)
}
