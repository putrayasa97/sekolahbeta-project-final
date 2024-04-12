package logger

import (
	"fmt"
	"io"
	"os"

	"github.com/sirupsen/logrus"
	easy "github.com/t-tomalak/logrus-easy-formatter"
)

var Log *logrus.Logger

func init() {
	f, err := os.OpenFile("logger.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("Failed to create logfile" + "logger.log")
		panic(err)
	}
	Log = logrus.New()
	Log.Out = io.MultiWriter(f, os.Stdout)
	Log.Level = logrus.DebugLevel
	Log.Formatter = &easy.Formatter{
		TimestampFormat: "2006-01-02 15:04:05",
		LogFormat:       "[%lvl%]: %time% - %msg%",
	}
}

func Trace(args ...interface{}) {
	Log.Trace(args...)
}

func Info(args ...interface{}) {
	Log.Info(args...)
}

func Warn(args ...interface{}) {
	Log.Warn(args...)
}

func Error(args ...interface{}) {
	Log.Error(args...)
}

func Fatal(args ...interface{}) {
	Log.Fatal(args...)
}
