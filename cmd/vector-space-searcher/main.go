package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	conf "github.com/amazingchow/photon-dance-vector-space-searcher/internal/config"
	"github.com/amazingchow/photon-dance-vector-space-searcher/internal/utils"
)

var (
	cfgPathFlag = flag.String("conf", "config/pipeline.json", "pipeline config")
	debugFlag   = flag.Bool("debug", false, "debug log level")
)

func main() {
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debugFlag {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	var cfg conf.ServiceConfig
	utils.LoadConfigOrPanic(*cfgPathFlag, &cfg)

	stopGroup := &sync.WaitGroup{}
	defer func() {
		stopGroup.Wait()
	}()
	stopCh := make(chan struct{})

	qss := NewQueryServiceServer(cfg.Pipeline)
	go qss.container.Run()

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
	}()

	go serverGRPCService(ctx, qss, &cfg, stopGroup, stopCh)
	go sereveHTTPService(ctx, qss, &cfg, stopGroup, stopCh)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

LOOP:
	for { // nolint
		select {
		case s := <-sigCh:
			{
				log.Info().Msgf("receive signal %v", s)
				break LOOP
			}
		}
	}
	// send stop signal to grpc service and http service
	close(stopCh)

	qss.container.Stop()
}
