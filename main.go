package main

import (
	"cli-service/utils"

	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

func InitEnv() {
	err := godotenv.Load(".env")
	if err != nil {
		logrus.Warn("Cannot load env file, using system env")
	}
}
func main() {
	InitEnv()
	utils.DumpDatabase()
}