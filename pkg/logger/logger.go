package logger

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"
)

func Init(file string) {
	writers := io.MultiWriter(
		NewConsoleWriter(),
		NewLumberjack(file),
	)

	log.Logger = zerolog.New(writers).With().Timestamp().Caller().Logger()
}

func NewWriter(file string) io.Writer {
	writers := io.MultiWriter(
		NewConsoleWriter(),
		NewLumberjack(file),
	)

	return writers
}

func NewConsoleWriter() io.Writer {
	return zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
}

func NewLumberjack(file string) io.Writer {
	abs, err := filepath.Abs(".")
	if err != nil {
		panic(err)
	}

	path := path.Join(abs, "logs", file)
	return &lumberjack.Logger{
		Filename:   path,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     7,
		Compress:   true,
	}
}
