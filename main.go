package main

import (
	"cli-service/utils"
	"cli-service/utils/logger"

	"github.com/joho/godotenv"
)

func InitEnv() {
	err := godotenv.Load(".env")
	if err != nil {
		logger.Warn("Cannot load env file, using system env")
	}
}
func main() {
	InitEnv()
	utils.DumpDatabase()
}
