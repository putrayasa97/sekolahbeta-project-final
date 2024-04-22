package dbbackup

import (
	"archive/zip"
	"cli-service/model"
	"fmt"
	"io"
	"os"
)

func archiveDatabase(pathFile model.PathFile, fileName model.NameFile) (model.NameFile, string, error) {
	pathNameFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, fileName.NameFileSql)
	nameFileZip := fmt.Sprintf("%s.zip", fileName.NameFileSql)
	pathNameFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFileZip)
	mErr := ""

	archive, err := os.Create(pathNameFileZip)
	if err != nil {
		mErr = fmt.Sprintf("Error creating zip file %s, Error : %s\n", pathNameFileZip, err.Error())
		return model.NameFile{
			NameFileSql: fileName.NameFileSql,
			NameFileZip: nameFileZip,
		}, mErr, err
	}
	defer archive.Close()

	zipWriter := zip.NewWriter(archive)

	f, err := os.Open(pathNameFileSql)
	if err != nil {
		mErr = fmt.Sprintf("Error opening sql file %s, Error : %s\n", fileName.NameFileSql, err.Error())
		return model.NameFile{
			NameFileSql: fileName.NameFileSql,
			NameFileZip: nameFileZip,
		}, mErr, err
	}
	defer f.Close()

	w, err := zipWriter.Create(fileName.NameFileSql)
	if err != nil {
		mErr = fmt.Sprintf("Error creating file %s in zip, Error : %s\n", fileName.NameFileSql, err.Error())
		return model.NameFile{
			NameFileSql: fileName.NameFileSql,
			NameFileZip: nameFileZip,
		}, mErr, err
	}

	if _, err := io.Copy(w, f); err != nil {
		mErr = fmt.Sprintf("Error copying file %s to zip , Error : %s\n", fileName.NameFileSql, err.Error())
		return model.NameFile{
			NameFileSql: fileName.NameFileSql,
			NameFileZip: nameFileZip,
		}, mErr, err
	}
	zipWriter.Close()

	return model.NameFile{
		NameFileSql:      fileName.NameFileSql,
		NameFileZip:      nameFileZip,
		NameDatabaseFile: fileName.NameDatabaseFile,
	}, mErr, nil
}
