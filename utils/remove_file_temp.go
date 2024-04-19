package utils

import (
	"cli-service/model"
	"fmt"
	"os"
)

func RemoveFileTemp(pathFile *model.PathFile, nameFile model.NameFile) {
	pathFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFile.NameFileSql)
	pathFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFile.NameFileZip)
	os.Remove(pathFileZip)
	os.Remove(pathFileSql)
}
