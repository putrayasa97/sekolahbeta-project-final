package dbbackup

import (
	"bytes"
	"cli-service/model"
	"cli-service/utils/logger"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

func uploadFile(pathFile *model.PathFile, fileName model.NameFile) (string, error) {
	serviceURL := os.Getenv("WEB_SERVICE_URL")
	bearerToken := "Bearer " + os.Getenv("WEB_SERIVCE_STATIC_KEY")
	uploadURL := fmt.Sprintf("%s/bckp-database/%s", serviceURL, fileName.NameDatabaseFile)
	pathNameFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, fileName.NameFileZip)
	mErr := ""

	file, err := os.Open(pathNameFileZip)
	if err != nil {
		mErr = fmt.Sprintf("Error open file %s to zip , Error : %s\n", fileName.NameFileZip, err.Error())
		return mErr, err
	}

	defer file.Close()

	requestBody := &bytes.Buffer{}
	writer := multipart.NewWriter(requestBody)

	fileField, err := writer.CreateFormFile("zip_file", filepath.Base(fileName.NameFileZip))
	if err != nil {
		mErr = fmt.Sprintf("Error create form zip_file %s , Error : %s\n", fileName.NameFileZip, err.Error())
		return mErr, err
	}

	_, err = io.Copy(fileField, file)
	if err != nil {
		mErr = fmt.Sprintf("Error copying file %s , Error : %s\n", fileName.NameFileZip, err.Error())
		return mErr, err
	}

	err = writer.Close()
	if err != nil {
		mErr = fmt.Sprintf("Error writer close zip %s, Error : %s\n", fileName.NameFileZip, err.Error())
		return mErr, err
	}

	req, err := http.NewRequest("POST", uploadURL, requestBody)
	if err != nil {
		mErr = fmt.Sprintf("Error new request %s , Error : %s\n", uploadURL, err.Error())
		return mErr, err
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Add("Authorization", bearerToken)
	client := &http.Client{}

	resp, err := client.Do(req)

	if err != nil {
		mErr = fmt.Sprintf("Error send request %s, Error : %s\n", uploadURL, err.Error())
		return mErr, err
	}

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		mErr = fmt.Sprintf("Error send request %s, Error : %s\n", uploadURL, string(body))
		logger.Error(mErr)
	}

	defer resp.Body.Close()

	return mErr, nil
}
