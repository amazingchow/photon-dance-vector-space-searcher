package main

import (
	"context"
	"flag"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/amazingchow/engine-vector-space-search-service/api"
	conf "github.com/amazingchow/engine-vector-space-search-service/internal/config"
	"github.com/amazingchow/engine-vector-space-search-service/internal/pipeline"
	"github.com/amazingchow/engine-vector-space-search-service/internal/utils"
)

var (
	_CfgPath = flag.String("conf", "config/pipeline.json", "pipeline config")
	_Debug   = flag.Bool("debug", false, "debug log level")
)

type QueryServiceServer struct {
	container *pipeline.MOFRPCContainer
}

func NewQueryServiceServer(cfg *conf.PipelineConfig) *QueryServiceServer {
	return &QueryServiceServer{
		container: pipeline.NewMOFRPCContainer(cfg),
	}
}

// Query 查询服务接口.
func (qss *QueryServiceServer) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	if len(req.GetQuery()) == 0 || req.GetTopk() == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "invalid input")
	}

	docs, err := qss.container.Query(ctx, req.GetTopk(), req.GetQuery())
	if err != nil {
		if err == utils.ErrServiceUnavailable {
			return nil, status.Errorf(codes.Unavailable, err.Error())
		} else if err == utils.ErrContextDone {
			return nil, status.Errorf(codes.DeadlineExceeded, err.Error())
		}
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return &pb.QueryResponse{
		Docs: docs,
	}, nil
}

// GetSystemInfo 获取系统信息接口.
func (qss *QueryServiceServer) GetSystemInfo(ctx context.Context, req *pb.GetSystemInfoRequest) (*pb.GetSystemInfoResponse, error) {
	info, err := qss.container.GetSystemInfo()
	if err != nil {
		if err == utils.ErrServiceUnavailable {
			return nil, status.Errorf(codes.Unavailable, err.Error())
		} else if err == utils.ErrContextDone {
			return nil, status.Errorf(codes.DeadlineExceeded, err.Error())
		}
		return nil, status.Errorf(codes.Unknown, err.Error())
	}

	return info, nil
}

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

func main() {
	flag.Parse()

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *_Debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	var cfg conf.ServiceConfig
	utils.LoadConfigOrPanic(*_CfgPath, &cfg)

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
