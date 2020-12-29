package main

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"

	pb "github.com/amazingchow/photon-dance-vector-space-searcher/api"
	conf "github.com/amazingchow/photon-dance-vector-space-searcher/internal/config"
)

func serverGRPCService(ctx context.Context, qss *QueryServiceServer, cfg *conf.ServiceConfig, stopGroup *sync.WaitGroup, stopCh chan struct{}) {
	stopGroup.Add(1)
	defer stopGroup.Done()

	l, err := net.Listen("tcp", cfg.GRPCEndpoint)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to start grpc service")
	}

	opts := []grpc.ServerOption{
		grpc.MaxSendMsgSize(64 * 1024 * 1024),
		grpc.MaxRecvMsgSize(64 * 1024 * 1024),
	}
	grpcServer := grpc.NewServer(opts...)

	pb.RegisterQueryServiceServer(grpcServer, qss)
	log.Info().Msgf("grpc service is listening at \x1b[1;31m%s\x1b[0m", cfg.GRPCEndpoint)
	go func() {
		if err := grpcServer.Serve(l); err != nil {
			log.Warn().Err(err)
		}
	}()

GRPC_LOOP:
	for { // nolint
		select {
		case _, ok := <-stopCh:
			{
				if !ok {
					break GRPC_LOOP
				}
			}
		}
	}

	grpcServer.GracefulStop()
	log.Info().Msg("stop grpc service")
}

func sereveHTTPService(ctx context.Context, qss *QueryServiceServer, cfg *conf.ServiceConfig, stopGroup *sync.WaitGroup, stopCh chan struct{}) {
	stopGroup.Add(1)
	defer stopGroup.Done()

	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	if err := pb.RegisterQueryServiceHandlerFromEndpoint(ctx, mux, cfg.GRPCEndpoint, opts); err != nil {
		log.Fatal().Err(err).Msg("failed to register http service")
	}

	http.Handle("/", mux)
	httpServer := http.Server{
		Addr: cfg.HTTPEndpoint,
	}

	log.Info().Msgf("http service is listening at \x1b[1;31m%s\x1b[0m", cfg.HTTPEndpoint)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil {
			log.Warn().Err(err)
		}
	}()

HTTP_LOOP:
	for { // nolint
		select {
		case _, ok := <-stopCh:
			{
				if !ok {
					break HTTP_LOOP
				}
			}
		}
	}

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Warn().Err(err).Msg("failed to stop http service")
	}
	log.Info().Msg("stop http service")
}
