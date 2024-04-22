package dbbackup

import (
	"cli-service/model"
	"cli-service/utils/logger"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Proses DumpDatabase
// TODO 1. Memanggil list databases dari file json
// TODO 2. Proses dump databases
// TODO 3. Proses zip file sql yaang telah di dump
// TODO 4. Proses Upload file ke webservice
// TODO 5. Hapus file temporary sql dan zip ketik selesai proses upload
// Proses ini menggunakan konsep Concurency: Pipeline Pattern

func BackupRunner() {
	pathFile := model.PathFile{
		PathDBJson:  "databases.json",
		PathFileSql: "temp/sql",
		PathFileZip: "temp/zip",
	}

	os.MkdirAll(pathFile.PathFileSql, 0777)
	os.MkdirAll(pathFile.PathFileZip, 0777)

	logger.Info(fmt.Sprintln("Mulai Proses Backup Databases ..."))

	// get list databases
	listDatabases := getListDB(&pathFile)

	// Pipeline 1 DumpDatabases
	var dumpDbChanTemp []<-chan model.NameFile
	for i := 0; i < 3; i++ {
		dumpDbChanTemp = append(dumpDbChanTemp, processDumpDB(listDatabases, &pathFile))
	}
	dumpDbChan := processMergeChan(dumpDbChanTemp...)

	// Pipeline 2 archiveSqlToZip
	var zipChanTemp []<-chan model.NameFile
	for i := 0; i < 6; i++ {
		zipChanTemp = append(zipChanTemp, processArchiveDB(dumpDbChan, &pathFile))
	}
	archiveZipChan := processMergeChan(zipChanTemp...)

	// Pipeline 3 UploadFileToService
	var uploadChanTemp []<-chan model.NameFile
	for i := 0; i < 3; i++ {
		uploadChanTemp = append(uploadChanTemp, processUploadFile(archiveZipChan, &pathFile))
	}
	uploadFileChan := processMergeChan(uploadChanTemp...)

	// Hapus file temporay sql dan zip
	for nameFile := range uploadFileChan {
		mErr, err := removeFile(&pathFile, nameFile)
		if err != nil {
			logger.Error(mErr)
		}

		mErr = fmt.Sprintf("Database: %s Telah diproses \n", nameFile.NameFileZip)
		logger.Info(mErr)
	}

	logger.Info(fmt.Sprintln("Proses Backup Database Berakhir !!"))
}

// fungsi untuk memanggil list database yg sudah telah terdaftar
func getListDB(pathFile *model.PathFile) <-chan model.Database {
	listDatabases := []model.Database{}
	dbChan := make(chan model.Database)

	dataJson, err := os.ReadFile(pathFile.PathDBJson)
	if err != nil {
		logger.Error(fmt.Sprintln("File databases.json tidak ditemukan !"))
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
func processDumpDB(listDB <-chan model.Database, pathFile *model.PathFile) <-chan model.NameFile {
	dbChan := make(chan model.NameFile)

	go func() {
		defer close(dbChan)
		for db := range listDB {
			nameFile, mErr, err := dumpDatabase(pathFile, db)
			if err != nil {
				logger.Error(mErr)
			}

			dbChan <- nameFile
		}
	}()

	return dbChan
}

// fungsi untuk proses Zip dari database yg di dump sebelumnya
func processArchiveDB(fileNameCh <-chan model.NameFile, pathFile *model.PathFile) <-chan model.NameFile {
	zipChan := make(chan model.NameFile)
	go func() {
		defer close(zipChan)
		for fileName := range fileNameCh {
			nameFile, mErr, err := archiveDatabase(*pathFile, fileName)
			if err != nil {
				logger.Error(mErr)
			}

			zipChan <- nameFile
		}
	}()

	return zipChan
}

// fungsi untuk proses upload file ke service
func processUploadFile(fileNameCh <-chan model.NameFile, pathFile *model.PathFile) <-chan model.NameFile {
	uploadChan := make(chan model.NameFile)
	go func() {
		defer close(uploadChan)
		for fileName := range fileNameCh {
			mErr, err := uploadFile(pathFile, fileName)
			if err != nil {
				logger.Error(mErr)
			}

			uploadChan <- fileName
		}
	}()

	return uploadChan
}

// fungsi untuk proses mengabungkan channel
func processMergeChan(chanMany ...<-chan model.NameFile) <-chan model.NameFile {
	wg := sync.WaitGroup{}

	mergedChan := make(chan model.NameFile)

	wg.Add(len(chanMany))
	for _, eachChan := range chanMany {
		go func(eachChan <-chan model.NameFile) {
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
