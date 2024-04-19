package dbbackup

import (
	"cli-service/model"
	"fmt"
	"os"
)

func removeFile(pathFile *model.PathFile, nameFile model.NameFile) {
	pathFileSql := fmt.Sprintf("%s/%s", pathFile.PathFileSql, nameFile.NameFileSql)
	pathFileZip := fmt.Sprintf("%s/%s", pathFile.PathFileZip, nameFile.NameFileZip)
	os.Remove(pathFileZip)
	os.Remove(pathFileSql)
}
