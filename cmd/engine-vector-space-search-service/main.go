package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/pipeline"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

var (
	_CfgPath = flag.String("conf", "config/pipeline.json", "pipeline config")
)

func main() {
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).With().Caller().Logger()

	var pipelieCfg conf.PipelineConfig
	utils.LoadConfigOrPanic(*_CfgPath, &pipelieCfg)

	container, err := pipeline.NewMOFRPCContainer(&pipelieCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("cannot create pipeline container")
	}
	go container.Run()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

LOOP:
	for {
		select {
		case s := <-sigCh:
			log.Info().Msgf("receive signal %v", s)
			break LOOP
		}
	}

	container.Stop()
}
