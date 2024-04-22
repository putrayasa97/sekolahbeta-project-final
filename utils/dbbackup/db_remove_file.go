package dbbackup

import (
	"cli-service/model"
	"fmt"
	"os"
)

func removeFile(pathFile *model.PathFile, nameFile model.NameFile) (string, error) {
	pathFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFile.NameFileSql)
	pathFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFile.NameFileZip)
	mErr := ""
	err := os.Remove(pathFileZip)
	if err != nil {
		mErr = fmt.Sprintf("Error remove file %s, Error: %s\n", nameFile.NameFileZip, err.Error())
		return mErr, err
	}

	err = os.Remove(pathFileSql)
	if err != nil {
		mErr = fmt.Sprintf("Error remove file %s, Error: %s\n", nameFile.NameFileSql, err.Error())
		return mErr, err
	}

	return mErr, nil
}
